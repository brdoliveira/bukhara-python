"""Runtime FastAPI/Kafka do inventory-service.

As classes de mem\u00f3ria permanecem nos demais m\u00f3dulos para que testes unit\u00e1rios
continuem independentes de broker e banco. Este arquivo \u00e9 o caminho usado em
produ\u00e7\u00e3o: AIOKafkaConsumer/AIOKafkaProducer e PostgreSQL dur\u00e1vel.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, AsyncIterator, Protocol

from fastapi import FastAPI, Response

from .adapter import InMemoryBroker, InventoryAdapter
from .consumer import InventoryConsumer
from .handler import InventoryHandler
from .outbox import PostgresOutbox
from .persistence import PostgresInventoryRepository


SERVICE_NAME = "inventory-service"
CONSUMED_TOPICS = ("orders.events", "inventory.events", "inventory.retry.1", "inventory.retry.2", "inventory.retry.3")
RETRY_DELAYS = (1, 5, 15)


class DependencyProbe:
    """Double simples, preservado para testes de health sem infraestrutura."""

    def __init__(self, available: bool = True) -> None:
        self.available = available

    def is_available(self) -> bool:
        return self.available


class AsyncProbe(Protocol):
    async def check(self) -> bool: ...


async def probe_available(probe: Any) -> bool:
    try:
        check = getattr(probe, "check", None)
        if check is not None:
            return bool(await check())
        return bool(probe.is_available())
    except Exception:
        return False


class PostgresReadinessProbe:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    async def check(self) -> bool:
        def query() -> bool:
            import psycopg

            with psycopg.connect(self.database_url, connect_timeout=2) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return cursor.fetchone() == (1,)

        return await asyncio.to_thread(query)


class KafkaReadinessProbe:
    def __init__(self, bootstrap_servers: str, runtime: "KafkaInventoryRuntime | None" = None) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.runtime = runtime

    async def check(self) -> bool:
        if self.runtime and self.runtime.producer is not None:
            return bool(self.runtime.producer.client.bootstrap_connected())
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers, request_timeout_ms=2000)
        try:
            await producer.start()
            return bool(producer.client.bootstrap_connected())
        finally:
            await producer.stop()


class KafkaDispatchBroker(InMemoryBroker):
    """Bridge: o consumidor s\u00edncrono registra inten\u00e7\u00f5es; este adaptador as envia."""

    def __init__(self, producer: Any) -> None:
        super().__init__()
        self.producer = producer

    async def flush(self) -> None:
        retries, dlq = self.retries[:], self.dlq[:]
        self.retries.clear()
        self.dlq.clear()
        for item in retries:
            event = deepcopy(item["event"])
            delay = item["delay_seconds"]
            event["retry_delay_seconds"] = delay
            event["retry_not_before"] = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
            await self._send(f"inventory.retry.{event['retry_attempt']}", event)
        for item in dlq:
            await self._send("inventory.dlq", {
                "event": self._canonical(item["event"]),
                "error_type": item["error_type"],
                "origin_service": item["origin_service"],
                "attempts": item["attempts"],
                "validation_errors": item["validation_errors"],
            })

    async def _send(self, topic: str, event: dict[str, Any]) -> None:
        key_source = event.get("order_id") or event.get("event", {}).get("order_id") or ""
        await self.producer.send_and_wait(topic, json.dumps(self._canonical(event), default=str).encode("utf-8"), key=str(key_source).encode("utf-8"))

    @staticmethod
    def _canonical(event: dict[str, Any]) -> dict[str, Any]:
        value = deepcopy(event)
        if "type" in value and "event_type" not in value:
            value["event_type"] = value["type"]
        return value


class KafkaInventoryRuntime:
    def __init__(self, *, database_url: str, bootstrap_servers: str) -> None:
        self.database_url = database_url
        self.bootstrap_servers = bootstrap_servers
        self.repository = PostgresInventoryRepository(database_url)
        self.producer: Any | None = None
        self.consumer: Any | None = None
        self.broker: KafkaDispatchBroker | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        await asyncio.to_thread(self.repository.initialize)
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            *CONSUMED_TOPICS,
            bootstrap_servers=self.bootstrap_servers,
            group_id=SERVICE_NAME,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self.consumer.start()
        self.broker = KafkaDispatchBroker(self.producer)
        self._running = True
        self._task = asyncio.create_task(self._consume_loop(), name="inventory-kafka-consumer")
        await self.publish_pending_outbox()

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
        await asyncio.to_thread(self.repository.close)

    async def _consume_loop(self) -> None:
        assert self.consumer is not None
        async for message in self.consumer:
            if not self._running:
                break
            await self.process_message(message.value)
            await self.consumer.commit()

    async def process_message(self, value: bytes | str | dict[str, Any]) -> str:
        """M\u00e9todo separado para testes com doubles, sem broker local."""
        try:
            raw = json.loads(value.decode("utf-8") if isinstance(value, bytes) else value) if not isinstance(value, dict) else value
        except (UnicodeDecodeError, json.JSONDecodeError):
            raw = {"raw": repr(value)}
        if isinstance(raw, dict):
            not_before = raw.get("retry_not_before")
            if isinstance(not_before, str):
                try:
                    remaining = (datetime.fromisoformat(not_before.replace("Z", "+00:00")) - datetime.now(timezone.utc)).total_seconds()
                    if remaining > 0:
                        await asyncio.sleep(remaining)
                except ValueError:
                    pass
        assert self.broker is not None
        handler = InventoryHandler(InventoryAdapter(self.repository), PostgresOutbox(self.repository), self.repository)
        worker = InventoryConsumer(handler, self.repository, self.broker, publish_immediately=False, retry_delays=RETRY_DELAYS)
        result = await asyncio.to_thread(worker.consume, raw)
        await self.publish_pending_outbox()
        await self.broker.flush()
        return result.status

    async def publish_pending_outbox(self) -> int:
        assert self.producer is not None
        events = await asyncio.to_thread(self.repository.pending_outbox)
        for event in events:
            canonical = KafkaDispatchBroker._canonical(event)
            await self.producer.send_and_wait(
                "inventory.events",
                json.dumps(canonical, default=str).encode("utf-8"),
                key=event["order_id"].encode("utf-8"),
            )
            await asyncio.to_thread(self.repository.mark_outbox_published, event["event_id"])
        return len(events)


class CompatFastAPI(FastAPI):
    """Mant\u00e9m os pequenos testes ASGI existentes, que omitem campos opcionais."""

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") == "http":
            scope = {"method": "GET", "headers": [], "query_string": b"", **scope}
        await super().__call__(scope, receive, send)


def create_app(
    kafka: Any | None = None,
    postgres: Any | None = None,
    *,
    runtime: KafkaInventoryRuntime | None = None,
) -> FastAPI:
    database_url = os.getenv("DATABASE_URL", "postgresql://inventory:inventory@localhost:5432/inventory")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    managed_runtime = runtime or (None if kafka is not None or postgres is not None else KafkaInventoryRuntime(database_url=database_url, bootstrap_servers=bootstrap_servers))
    kafka_probe = kafka or KafkaReadinessProbe(bootstrap_servers, managed_runtime)
    postgres_probe = postgres or PostgresReadinessProbe(database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if managed_runtime:
            await managed_runtime.start()
            app.state.inventory_runtime = managed_runtime
        try:
            yield
        finally:
            if managed_runtime:
                await managed_runtime.stop()

    app = CompatFastAPI(title=SERVICE_NAME, lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/ready")
    async def ready(response: Response) -> dict[str, str]:
        kafka_ok, postgres_ok = await asyncio.gather(probe_available(kafka_probe), probe_available(postgres_probe))
        result = {"status": "ready" if kafka_ok and postgres_ok else "not_ready", "kafka": "available" if kafka_ok else "unavailable", "postgres": "available" if postgres_ok else "unavailable"}
        if not kafka_ok or not postgres_ok:
            response.status_code = 503
        return result

    return app


app = create_app()
