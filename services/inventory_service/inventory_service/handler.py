"""Regras de neg\u00f3cio para reserva e compensa\u00e7\u00e3o de estoque."""

from __future__ import annotations

from typing import Any

from .adapter import InventoryAdapter
from .outbox import Outbox
from .persistence import InsufficientStockError, InventoryRepository


class InventoryHandler:
    def __init__(self, adapter: InventoryAdapter, outbox: Outbox, repository: InventoryRepository) -> None:
        self.adapter = adapter
        self.outbox = outbox
        self.repository = repository

    def handle(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        if event["type"] == "order.created":
            return self._reserve(event)
        if event["type"] == "inventory.release.requested":
            return self._release(event)
        raise ValueError(f"unsupported event type: {event['type']}")

    def _reserve(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        # O contrato HTTP do order-service chama o identificador de product_id.
        # Internamente o estoque usa sku; aceitar ambos mantém a fronteira entre
        # serviços explícita sem vazar o modelo de persistência para a API.
        items = [
            {**item, "sku": item.get("sku") or item.get("product_id")}
            for item in event["payload"]["items"]
        ]
        try:
            self.adapter.reserve(event["order_id"], items)
        except InsufficientStockError:
            return [self.outbox.add("inventory.rejected", order_id=event["order_id"], correlation_id=event["correlation_id"], payload={"reason": "insufficient_stock"}, causation_id=event["event_id"])]
        return [self.outbox.add("inventory.reserved", order_id=event["order_id"], correlation_id=event["correlation_id"], payload={"items": items}, causation_id=event["event_id"])]

    def _release(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        self.adapter.release(event["order_id"])
        return [self.outbox.add("inventory.released", order_id=event["order_id"], correlation_id=event["correlation_id"], payload={}, causation_id=event["event_id"])]

    def fallback(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """Falha t\u00e9cnica nunca vira sucesso; libera e rejeita o pedido uma \u00fanica vez."""
        self.adapter.release(event["order_id"])
        self.repository.record_fallback(event["event_id"])
        return [self.outbox.add("inventory.rejected", order_id=event["order_id"], correlation_id=event["correlation_id"], payload={"reason": "technical_failure"}, causation_id=event["event_id"])]
