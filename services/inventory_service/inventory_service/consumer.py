"""Consumidor resiliente dos eventos que pertencem ao servi\u00e7o de estoque."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .adapter import InMemoryBroker, TransientDependencyError
from .handler import InventoryHandler
from .persistence import InventoryRepository


MAX_RETRIES = 3
SERVICE_NAME = "inventory-service"
SUPPORTED_EVENT_TYPES = {"order.created", "inventory.release.requested"}


@dataclass(frozen=True)
class ConsumerResult:
    status: str


class InventoryConsumer:
    """Aplica Inbox/Outbox e mant\u00e9m a API s\u00edncrona test\u00e1vel por doubles."""

    def __init__(
        self,
        handler: InventoryHandler,
        repository: InventoryRepository,
        broker: InMemoryBroker,
        *,
        publish_immediately: bool = True,
        retry_delays: tuple[int, int, int] = (1, 2, 4),
    ) -> None:
        self.handler = handler
        self.repository = repository
        self.broker = broker
        self.publish_immediately = publish_immediately
        self.retry_delays = retry_delays

    def consume(self, event: dict[str, Any]) -> ConsumerResult:
        event = self.normalize(event)
        errors = self._validate(event)
        event_id = event.get("event_id") if isinstance(event, dict) else None
        if errors:
            self.broker.send_to_dlq(
                event if isinstance(event, dict) else {"raw": event},
                error_type="ValidationError",
                origin_service=SERVICE_NAME,
                attempts=0,
                validation_errors=errors,
            )
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
            # No PostgreSQL, reserva, Inbox e Outbox compartilham a transa\u00e7\u00e3o.
            with self.repository.transaction():
                emitted = self.handler.handle(event)
                self.repository.mark_processed(event_id)
            if self.publish_immediately:
                for produced in emitted:
                    self.broker.publish(produced)
            self.broker.acknowledge(event_id)
            return ConsumerResult("processed")
        except TransientDependencyError as error:
            return self._retry_or_dlq(event, error)
        except Exception as error:
            if self._is_transient_infrastructure_error(error):
                return self._retry_or_dlq(event, error)
            self.broker.send_to_dlq(
                event,
                error_type=type(error).__name__,
                origin_service=SERVICE_NAME,
                attempts=self._attempt(event),
            )
            self.broker.acknowledge(event_id)
            return ConsumerResult("dlq")

    def _retry_or_dlq(self, event: dict[str, Any], error: TransientDependencyError) -> ConsumerResult:
        attempt = self._attempt(event)
        if attempt < MAX_RETRIES:
            retry = deepcopy(event)
            retry["retry_attempt"] = attempt + 1
            self.broker.schedule_retry(retry, delay_seconds=self.retry_delays[attempt])
            self.broker.acknowledge(event["event_id"])
            return ConsumerResult("retried")

        with self.repository.transaction():
            claimed_terminal = self.repository.mark_terminal_failure(event["event_id"])
            if claimed_terminal:
                fallback_events = self.handler.fallback(event)
            else:
                fallback_events = []
        if claimed_terminal:
            if self.publish_immediately:
                for produced in fallback_events:
                    self.broker.publish(produced)
            self.broker.send_to_dlq(
                event,
                error_type=type(error).__name__,
                origin_service=SERVICE_NAME,
                attempts=attempt,
            )
        self.broker.acknowledge(event["event_id"])
        return ConsumerResult("dlq")

    @staticmethod
    def _is_transient_infrastructure_error(error: Exception) -> bool:
        """Evita transformar uma queda tempor\u00e1ria de PostgreSQL em DLQ imediata."""
        if isinstance(error, (ConnectionError, TimeoutError)):
            return True
        return error.__class__.__module__.startswith("psycopg") and error.__class__.__name__ in {"OperationalError", "InterfaceError"}

    @staticmethod
    def normalize(event: Any) -> Any:
        """Aceita o envelope compartilhado Kafka e o formato de testes anterior."""
        if not isinstance(event, dict):
            return event
        normalized = deepcopy(event)
        if "type" not in normalized and isinstance(normalized.get("event_type"), str):
            normalized["type"] = normalized["event_type"]
        if "event_type" not in normalized and isinstance(normalized.get("type"), str):
            normalized["event_type"] = normalized["type"]
        payload = normalized.get("payload")
        if isinstance(payload, dict) and "order_id" not in normalized and isinstance(payload.get("order_id"), str):
            normalized["order_id"] = payload["order_id"]
        return normalized

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
        elif event.get("type") == "order.created" and not isinstance(event["payload"].get("items"), list):
            errors.append("payload.items must be a list")
        try:
            if InventoryConsumer._attempt(event) < 0:
                errors.append("retry_attempt must be non-negative")
        except (TypeError, ValueError):
            errors.append("retry_attempt must be an integer")
        return errors
