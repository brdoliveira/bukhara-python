"""Runtime FastAPI do payment-service com Kafka e PostgreSQL reais."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Response, status

from .adapter import PaymentAdapter, TransientDependencyError
from .consumer import MAX_RETRIES, PaymentConsumer, SERVICE_NAME
from .handler import PaymentHandler
from .outbox import InMemoryOutbox, PostgresOutbox
from .persistence import PostgresPaymentRepository


INVENTORY_TOPIC = "inventory.events"
PAYMENTS_TOPIC = "payments.events"
RETRY_TOPICS = ("payment.retry.1", "payment.retry.2", "payment.retry.3")
RETRY_DELAYS = (1, 5, 15)
DLQ_TOPIC = "payment.dlq"


@dataclass
class DependencyProbe:
    """Double pequeno para testes HTTP sem infraestrutura externa."""

    available: bool = True

    def is_available(self) -> bool:
        return self.available


class PaymentRuntime:
    """Consome reservas, grava a decisão e drena a Outbox com aiokafka."""

    def __init__(
        self,
        *,
        database_url: str | None = None,
        bootstrap_servers: str | None = None,
        adapter: PaymentAdapter | None = None,
        consumer_factory: Any | None = None,
        producer_factory: Any | None = None,
    ) -> None:
        self.database_url = database_url or os.environ.get("DATABASE_URL", "")
        self.bootstrap_servers = bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.adapter = adapter or PaymentAdapter()
        self.consumer_factory = consumer_factory
        self.producer_factory = producer_factory
        self.repository: PostgresPaymentRepository | None = None
        self.outbox: PostgresOutbox | None = None
        self.consumer: Any | None = None
        self.producer: Any | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self.started = False

    async def start(self) -> None:
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is required for payment-service")
        if self.consumer_factory is None or self.producer_factory is None:
            try:
                from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
            except ImportError as error:  # Docker/runtime dependency, not a unit-test dependency.
                raise RuntimeError("aiokafka must be installed to run payment-service") from error
            self.consumer_factory = self.consumer_factory or AIOKafkaConsumer
            self.producer_factory = self.producer_factory or AIOKafkaProducer

        self.repository = PostgresPaymentRepository(self.database_url)
        await asyncio.to_thread(self.repository.initialize)
        self.outbox = PostgresOutbox(self.repository.engine)
        self.producer = self.producer_factory(bootstrap_servers=self.bootstrap_servers)
        self.consumer = self.consumer_factory(
            INVENTORY_TOPIC,
            *RETRY_TOPICS,
            bootstrap_servers=self.bootstrap_servers,
            group_id=SERVICE_NAME,
            enable_auto_commit=False,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )
        await self.producer.start()
        await self.consumer.start()
        self.started = True
        self._tasks = [
            asyncio.create_task(self._consume_loop(), name="payment-kafka-consumer"),
            asyncio.create_task(self._publish_outbox_loop(), name="payment-outbox-publisher"),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.consumer is not None:
            await self.consumer.stop()
        if self.producer is not None:
            await self.producer.stop()
        self.started = False

    async def ready(self) -> bool:
        if not self.started or self.repository is None or self.producer is None:
            return False
        try:
            # force_metadata_update performs an operation against the configured broker.
            await self.producer.client.force_metadata_update()
            return await asyncio.to_thread(self.repository.is_available)
        except Exception:
            return False

    async def _consume_loop(self) -> None:
        assert self.consumer is not None
        async for record in self.consumer:
            await self.process(record.value)
            await self.consumer.commit()

    async def _publish_outbox_loop(self) -> None:
        while True:
            await self.publish_pending()
            await asyncio.sleep(0.2)

    async def publish_pending(self) -> int:
        assert self.outbox is not None and self.producer is not None
        pending = await asyncio.to_thread(self.outbox.pending)
        for row in pending:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            await self.producer.send_and_wait(row["topic"], json.dumps(payload).encode("utf-8"), key=payload["order_id"].encode("utf-8"))
            await asyncio.to_thread(self.outbox.mark_published, row["id"])
        return len(pending)

    async def process(self, incoming: Any) -> None:
        """Processa uma mensagem; exceções de infraestrutura não recebem commit."""
        assert self.repository is not None and self.outbox is not None
        event = PaymentConsumer._normalize(incoming)
        errors = PaymentConsumer._validate(event)
        if errors:
            await asyncio.to_thread(self.outbox.enqueue, self._dlq(event, "ValidationError", 0, errors), topic=DLQ_TOPIC)
            return
        assert isinstance(event, dict)
        if await asyncio.to_thread(self.repository.is_terminal, event["event_id"]):
            return

        handler = PaymentHandler(self.adapter, InMemoryOutbox(), self._memory_repository())
        try:
            emitted = handler.handle(event)
        except TransientDependencyError as error:
            await self._retry_or_fallback(event, handler, error)
            return
        except Exception as error:
            await asyncio.to_thread(self.outbox.enqueue, self._dlq(event, type(error).__name__, self._attempt(event)), topic=DLQ_TOPIC)
            return

        outcome = "declined" if any(item["type"] == "payment.failed" for item in emitted) else "approved"
        await asyncio.to_thread(self.repository.persist_outcome, event, outcome, self._topic_events(emitted))

    async def _retry_or_fallback(self, event: dict[str, Any], handler: PaymentHandler, error: TransientDependencyError) -> None:
        assert self.repository is not None and self.outbox is not None
        attempt = self._attempt(event)
        if attempt < MAX_RETRIES:
            retry = deepcopy(event)
            retry["retry_attempt"] = attempt + 1
            await asyncio.to_thread(
                self.outbox.enqueue,
                retry,
                topic=RETRY_TOPICS[attempt],
                available_at=datetime.now(timezone.utc) + timedelta(seconds=RETRY_DELAYS[attempt]),
            )
            return

        emitted = handler.fallback(event)
        dlq = self._dlq(event, type(error).__name__, attempt, [])
        await asyncio.to_thread(
            self.repository.persist_outcome,
            event,
            "unavailable",
            [*self._topic_events(emitted), (DLQ_TOPIC, dlq)],
            terminal=True,
        )

    @staticmethod
    def _memory_repository() -> Any:
        # O handler precisa somente registrar fallback; a decisão durável ocorre acima.
        from .persistence import PaymentRepository

        return PaymentRepository()

    @staticmethod
    def _attempt(event: dict[str, Any]) -> int:
        return int(event.get("retry_attempt", 0))

    @staticmethod
    def _topic_events(events: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
        return [(PAYMENTS_TOPIC if event["type"].startswith("payment.") else INVENTORY_TOPIC, event) for event in events]

    @staticmethod
    def _dlq(event: Any, error_type: str, attempts: int, validation_errors: list[str]) -> dict[str, Any]:
        source = event if isinstance(event, dict) else {"raw": event}
        return {
            "event_id": str(uuid4()),
            "type": "payment.dlq",
            "event_type": "payment.dlq",
            "event_version": 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "producer": SERVICE_NAME,
            "order_id": source.get("order_id", "unknown"),
            "correlation_id": source.get("correlation_id", "unknown"),
            "causation_id": source.get("event_id"),
            "payload": {
                "event": source,
                "error_type": error_type,
                "origin_service": SERVICE_NAME,
                "attempts": attempts,
                "validation_errors": validation_errors,
            },
        }


def create_app(
    kafka: DependencyProbe | None = None,
    postgres: DependencyProbe | None = None,
    *,
    runtime_factory: Any | None = None,
) -> FastAPI:
    """Cria FastAPI; probes injetadas evitam iniciar infraestrutura nos testes."""

    injected_probes = kafka is not None or postgres is not None
    kafka_probe = kafka or DependencyProbe()
    postgres_probe = postgres or DependencyProbe()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if injected_probes:
            yield
            return
        runtime = (runtime_factory or PaymentRuntime)()
        app.state.runtime = runtime
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="payment-service", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/ready")
    async def ready(response: Response) -> dict[str, str]:
        if injected_probes:
            kafka_available = kafka_probe.is_available()
            postgres_available = postgres_probe.is_available()
        else:
            runtime = getattr(app.state, "runtime", None)
            kafka_available = postgres_available = bool(runtime and await runtime.ready())
        if not (kafka_available and postgres_available):
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "ready" if kafka_available and postgres_available else "not_ready",
            "kafka": "available" if kafka_available else "unavailable",
            "postgres": "available" if postgres_available else "unavailable",
        }

    return app


app = create_app()
