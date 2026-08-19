"""Adaptadores determinísticos para mensageria e envio de notificações."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import json
import logging
from typing import Any, Iterable


class TransientDependencyError(RuntimeError):
    """Indica que o provedor de notificação pode ser chamado novamente."""


@dataclass
class InMemoryBroker:
    """Registra acknowledgements, retries e DLQ para o consumidor e seus testes."""

    retries: list[dict[str, Any]] = field(default_factory=list)
    dlq: list[dict[str, Any]] = field(default_factory=list)
    acknowledged: list[str] = field(default_factory=list)

    def schedule_retry(self, event: dict[str, Any], delay_seconds: int) -> None:
        self.retries.append({"event": deepcopy(event), "delay_seconds": delay_seconds})

    def send_to_dlq(
        self, event: dict[str, Any], *, error_type: str, origin_service: str,
        attempts: int, validation_errors: list[str] | None = None,
    ) -> None:
        self.dlq.append({
            "event": deepcopy(event), "error_type": error_type, "origin_service": origin_service,
            "attempts": attempts, "validation_errors": list(validation_errors or []),
        })

    def acknowledge(self, event_id: str) -> None:
        self.acknowledged.append(event_id)


class NotificationAdapter:
    """Porta de envio de notificações e do fallback de falhas terminais."""

    def __init__(self, *, send_failures: Iterable[Exception] = ()) -> None:
        self._send_failures = list(send_failures)
        self.sent_notifications: list[dict[str, str]] = []
        self.fallbacks: list[str] = []

    @property
    def sent(self) -> list[str]:
        return [notification["order_id"] for notification in self.sent_notifications]

    def send_order_completed(self, *, order_id: str, correlation_id: str) -> None:
        if self._send_failures:
            raise self._send_failures.pop(0)
        self.sent_notifications.append({
            "order_id": order_id, "correlation_id": correlation_id, "kind": "order_completed",
        })

    def fallback(self, order_id: str) -> None:
        self.fallbacks.append(order_id)


class LoggingNotificationAdapter(NotificationAdapter):
    """Adaptador de produção do MVP; emite notificações estruturadas no log."""

    def __init__(self) -> None:
        super().__init__()
        self._logger = logging.getLogger("notification-service")

    def send_order_completed(self, *, order_id: str, correlation_id: str) -> None:
        super().send_order_completed(order_id=order_id, correlation_id=correlation_id)
        self._logger.info(json.dumps({
            "event": "notification.sent",
            "kind": "order_completed",
            "order_id": order_id,
            "correlation_id": correlation_id,
        }))
