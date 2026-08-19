"""Consumidor idempotente de pagamentos aprovados."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import asyncio
import json
from typing import Any

from observability.logging import get_logger
from observability.telemetry import Telemetry

from .adapter import InMemoryBroker, TransientDependencyError
from .handler import NotificationHandler
from .persistence import NotificationRepository


MAX_RETRIES = 3
SERVICE_NAME = "notification-service"
SUPPORTED_EVENT_TYPES = {"payment.approved"}
PRODUCTION_RETRY_DELAYS = (1, 5, 15)


@dataclass(frozen=True)
class ConsumerResult:
    status: str


class NotificationConsumer:
    def __init__(
        self,
        handler: NotificationHandler,
        repository: NotificationRepository,
        broker: InMemoryBroker,
        telemetry: Telemetry | None = None,
    ) -> None:
        self.handler = handler
        self.repository = repository
        self.broker = broker
        self.telemetry = telemetry
        self.logger = get_logger("notification-service", service_name=SERVICE_NAME)

    def consume(self, event: dict[str, Any], headers: Any = None) -> ConsumerResult:
        if self.telemetry is None:
            return self._consume(event)
        with self.telemetry.kafka_consume(topic="payments.events", event=event, headers=headers):
            result = self._consume(event)
            event_type = str(event.get("event_type") or event.get("type") or "unknown") if isinstance(event, dict) else "unknown"
            self.telemetry.record_event(event_type=event_type, result=result.status)
            if result.status in {"retried", "dlq"}:
                self.telemetry.record_resilience(operation="retry" if result.status == "retried" else "dlq", event_type=event_type, result=result.status)
            if isinstance(event, dict):
                self.logger.info("notification event processed", order_id=event.get("order_id"), correlation_id=event.get("correlation_id"), result=result.status)
            return result

    def _consume(self, event: dict[str, Any]) -> ConsumerResult:
        errors = self._validate(event)
        event_id = event.get("event_id") if isinstance(event, dict) else None
        if errors:
            self.broker.send_to_dlq(event if isinstance(event, dict) else {"raw": event}, error_type="ValidationError",
                                    origin_service=SERVICE_NAME, attempts=0, validation_errors=errors)
            if isinstance(event_id, str):
                self.broker.acknowledge(event_id)
            return ConsumerResult("invalid")

        assert isinstance(event_id, str)
        if self.repository.was_processed(event_id):
            self.broker.acknowledge(event_id)
            return ConsumerResult("duplicate")
        if self.repository.has_terminal_failure(event_id):
            self.broker.acknowledge(event_id)
            return ConsumerResult("dlq")
        try:
            self.handler.handle(event)
            self.repository.mark_processed(event_id)
            self.broker.acknowledge(event_id)
            return ConsumerResult("processed")
        except TransientDependencyError as error:
            return self._retry_or_dlq(event, error)
        except Exception as error:
            self.broker.send_to_dlq(event, error_type=type(error).__name__, origin_service=SERVICE_NAME,
                                    attempts=self._attempt(event))
            self.broker.acknowledge(event_id)
            return ConsumerResult("dlq")

    def _retry_or_dlq(self, event: dict[str, Any], error: TransientDependencyError) -> ConsumerResult:
        attempt = self._attempt(event)
        if attempt < MAX_RETRIES:
            retry = deepcopy(event)
            retry["retry_attempt"] = attempt + 1
            self.broker.schedule_retry(retry, delay_seconds=2 ** attempt)
            self.broker.acknowledge(event["event_id"])
            return ConsumerResult("retried")
        if self.repository.mark_terminal_failure(event["event_id"]):
            self.handler.fallback(event)
            self.broker.send_to_dlq(event, error_type=type(error).__name__, origin_service=SERVICE_NAME, attempts=attempt)
        self.broker.acknowledge(event["event_id"])
        return ConsumerResult("dlq")

    @staticmethod
    def _attempt(event: dict[str, Any]) -> int:
        return int(event.get("retry_attempt", 0))

    @staticmethod
    def _validate(event: Any) -> list[str]:
        if not isinstance(event, dict):
            return ["event must be an object"]
        errors: list[str] = []
        for key in ("event_id", "type", "order_id", "correlation_id"):
            if not isinstance(event.get(key), str) or not event[key]:
                errors.append(f"{key} must be a non-empty string")
        if event.get("type") not in SUPPORTED_EVENT_TYPES:
            errors.append("type is not supported")
        if not isinstance(event.get("payload"), dict):
            errors.append("payload must be an object")
        try:
            if NotificationConsumer._attempt(event) < 0:
                errors.append("retry_attempt must be non-negative")
        except (TypeError, ValueError):
            errors.append("retry_attempt must be an integer")
        return errors


class KafkaNotificationWorker:
    """Liga o consumidor de domínio ao Kafka real sem contaminar os testes."""

    def __init__(
        self,
        bootstrap_servers: str,
        handler: NotificationHandler,
        repository: NotificationRepository,
        telemetry: Telemetry | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.handler = handler
        self.repository = repository
        self.producer: Any = None
        self.consumer: Any = None
        self.task: asyncio.Task | None = None
        self.ready = False
        self.telemetry = telemetry

    async def start(self) -> None:
        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        except ImportError as error:  # pragma: no cover - protegido no build da imagem
            raise RuntimeError("aiokafka is required for the production worker") from error
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        self.consumer = AIOKafkaConsumer(
            "payments.events",
            "notification.retry.1",
            "notification.retry.2",
            "notification.retry.3",
            bootstrap_servers=self.bootstrap_servers,
            group_id="notification-service-v1",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        await self.producer.start()
        await self.consumer.start()
        self.ready = True
        self.task = asyncio.create_task(self._run(), name="notification-kafka-consumer")

    async def stop(self) -> None:
        self.ready = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

    async def _run(self) -> None:
        async for message in self.consumer:
            event = self.normalize_event(message.value)
            if event.get("type") != "payment.approved":
                await self.consumer.commit()
                continue
            broker = InMemoryBroker()
            result = NotificationConsumer(self.handler, self.repository, broker, self.telemetry).consume(event, headers=message.headers)
            for retry in broker.retries:
                attempt = int(retry["event"]["retry_attempt"])
                await asyncio.sleep(PRODUCTION_RETRY_DELAYS[attempt - 1])
                await self._publish(f"notification.retry.{attempt}", retry["event"])
            for dead_letter in broker.dlq:
                await self._publish("notification.dlq", {
                    **dead_letter,
                    "event": self.to_envelope(dead_letter["event"]),
                })
            if result.status == "processed":
                await self._publish("notifications.events", self.to_envelope({
                    **event,
                    "event_id": f"notification-{event['event_id']}",
                    "type": "notification.sent",
                    "payload": {"kind": "order_completed"},
                    "causation_id": event["event_id"],
                }))
            await self.consumer.commit()

    async def _publish(self, topic: str, event: dict[str, Any]) -> None:
        if self.telemetry is None:
            await self.producer.send_and_wait(topic, event, key=event.get("order_id", "").encode("utf-8"))
            return
        with self.telemetry.kafka_publish(topic=topic, event=event) as headers:
            await self.producer.send_and_wait(
                topic,
                event,
                key=event.get("order_id", "").encode("utf-8"),
                headers=[(key, item.encode("utf-8")) for key, item in headers.items()],
            )

    @staticmethod
    def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(event)
        normalized["type"] = normalized.get("type") or normalized.get("event_type")
        normalized.setdefault("payload", {})
        return normalized

    @staticmethod
    def to_envelope(event: dict[str, Any]) -> dict[str, Any]:
        envelope = dict(event)
        event_type = envelope.pop("type", envelope.get("event_type"))
        envelope["event_type"] = event_type
        envelope.setdefault("event_version", 1)
        envelope.setdefault("producer", SERVICE_NAME)
        return envelope
