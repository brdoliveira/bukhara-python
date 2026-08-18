"""Regras de negócio para reserva e compensação de estoque."""

from __future__ import annotations

from typing import Any

from .adapter import InventoryAdapter
from .outbox import InMemoryOutbox
from .persistence import InsufficientStockError, InventoryRepository


class InventoryHandler:
    def __init__(self, adapter: InventoryAdapter, outbox: InMemoryOutbox, repository: InventoryRepository) -> None:
        self.adapter = adapter
        self.outbox = outbox
        self.repository = repository

    def handle(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        event_type = event["type"]
        if event_type == "order.created":
            return self._reserve(event)
        if event_type == "inventory.release.requested":
            return self._release(event)
        raise ValueError(f"unsupported event type: {event_type}")

    def _reserve(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            self.adapter.reserve(event["order_id"], event["payload"]["items"])
        except InsufficientStockError:
            return [
                self.outbox.add(
                    "inventory.rejected",
                    order_id=event["order_id"],
                    correlation_id=event["correlation_id"],
                    payload={"reason": "insufficient_stock"},
                )
            ]
        return [
            self.outbox.add(
                "inventory.reserved",
                order_id=event["order_id"],
                correlation_id=event["correlation_id"],
                payload={"items": event["payload"]["items"]},
            )
        ]

    def _release(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        self.adapter.release(event["order_id"])
        return [
            self.outbox.add(
                "inventory.released",
                order_id=event["order_id"],
                correlation_id=event["correlation_id"],
                payload={},
            )
        ]

    def fallback(self, event: dict[str, Any]) -> None:
        """Compensação segura: liberar é idempotente quando não há reserva."""
        self.adapter.release(event["order_id"])
        self.repository.record_fallback(event["event_id"])
