"""Consumidor resiliente de `inventory.reserved`."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .adapter import InMemoryBroker, TransientDependencyError
from .handler import PaymentHandler
from .persistence import PaymentRepository


MAX_RETRIES = 3
SERVICE_NAME = "payment-service"
SUPPORTED_EVENT_TYPES = {"inventory.reserved"}


@dataclass(frozen=True)
class ConsumerResult:
    status: str


class PaymentConsumer:
    def __init__(self, handler: PaymentHandler, repository: PaymentRepository, broker: InMemoryBroker) -> None:
        self.handler = handler
        self.repository = repository
        self.broker = broker

    def consume(self, event: dict[str, Any]) -> ConsumerResult:
        event = self._normalize(event)
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
            for emitted in self.handler.handle(event):
                self.broker.publish(emitted)
            self.repository.mark_processed(event_id)
            self.broker.acknowledge(event_id)
            return ConsumerResult("processed")
        except TransientDependencyError as error:
            return self._retry_or_dlq(event, error)
        except Exception as error:
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
            self.broker.schedule_retry(retry, delay_seconds=2**attempt)
            self.broker.acknowledge(event["event_id"])
            return ConsumerResult("retried")

        if self.repository.mark_terminal_failure(event["event_id"]):
            for emitted in self.handler.fallback(event):
                self.broker.publish(emitted)
            self.broker.send_to_dlq(
                event,
                error_type=type(error).__name__,
                origin_service=SERVICE_NAME,
                attempts=attempt,
            )
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
            if PaymentConsumer._attempt(event) < 0:
                errors.append("retry_attempt must be non-negative")
        except (TypeError, ValueError):
            errors.append("retry_attempt must be an integer")
        return errors

    @staticmethod
    def _normalize(event: Any) -> Any:
        """Aceita o contrato Kafka (`event_type`) sem quebrar os testes legados."""
        if not isinstance(event, dict):
            return event
        normalized = deepcopy(event)
        if "type" not in normalized and isinstance(normalized.get("event_type"), str):
            normalized["type"] = normalized["event_type"]
        if "event_type" not in normalized and isinstance(normalized.get("type"), str):
            normalized["event_type"] = normalized["type"]
        return normalized
