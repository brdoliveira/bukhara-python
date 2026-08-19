"""Regra de negócio para confirmar ao cliente a conclusão do pedido."""

from __future__ import annotations

from typing import Any

from .adapter import NotificationAdapter
from .persistence import NotificationRepository


class NotificationHandler:
    """Aplica a regra de envio para pagamentos aprovados e seu fallback seguro."""

    def __init__(self, adapter: NotificationAdapter, repository: NotificationRepository) -> None:
        self.adapter = adapter
        self.repository = repository

    def handle(self, event: dict[str, Any]) -> None:
        if event["type"] != "payment.approved":
            raise ValueError(f"unsupported event type: {event['type']}")
        self.adapter.send_order_completed(order_id=event["order_id"], correlation_id=event["correlation_id"])

    def fallback(self, event: dict[str, Any]) -> None:
        """Registra um encaminhamento seguro, sem reemitir a notificação."""
        self.adapter.fallback(event["order_id"])
        self.repository.record_fallback(event["event_id"])
