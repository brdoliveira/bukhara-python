"""Contratos de persistência para Inbox, reservas e efeitos terminais.

No processo de produção estes métodos correspondem a operações transacionais
no PostgreSQL. A implementação em memória mantém a mesma semântica para o MVP
e evita que uma entrega duplicada repita efeitos de negócio.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any


class InsufficientStockError(ValueError):
    """Falha de negócio: não deve entrar em retry."""


class InventoryRepository:
    def __init__(self, stock: dict[str, int] | None = None) -> None:
        self._stock = dict(stock or {})
        self._reservations: dict[str, list[dict[str, Any]]] = {}
        self._processed: set[str] = set()
        self._terminal_failures: set[str] = set()
        self._fallbacks: Counter[str] = Counter()

    def available(self, sku: str) -> int:
        return self._stock.get(sku, 0)

    def reserve(self, order_id: str, items: list[dict[str, Any]]) -> None:
        if order_id in self._reservations:
            return
        requested: Counter[str] = Counter()
        for item in items:
            sku = item.get("sku")
            quantity = item.get("quantity")
            if not isinstance(sku, str) or not sku or not isinstance(quantity, int) or quantity <= 0:
                raise ValueError("invalid inventory item")
            requested[sku] += quantity
        if not requested:
            raise ValueError("order must contain items")
        missing = [sku for sku, quantity in requested.items() if self.available(sku) < quantity]
        if missing:
            raise InsufficientStockError("insufficient_stock")
        for sku, quantity in requested.items():
            self._stock[sku] -= quantity
        self._reservations[order_id] = deepcopy(items)

    def release(self, order_id: str) -> bool:
        items = self._reservations.pop(order_id, None)
        if items is None:
            return False
        for item in items:
            self._stock[item["sku"]] = self.available(item["sku"]) + item["quantity"]
        return True

    def was_processed(self, event_id: str) -> bool:
        return event_id in self._processed

    def mark_processed(self, event_id: str) -> None:
        self._processed.add(event_id)

    def mark_terminal_failure(self, event_id: str) -> bool:
        if event_id in self._terminal_failures:
            return False
        self._terminal_failures.add(event_id)
        return True

    def has_terminal_failure(self, event_id: str) -> bool:
        return event_id in self._terminal_failures

    def record_fallback(self, event_id: str) -> None:
        self._fallbacks[event_id] += 1

    def fallback_count(self, event_id: str) -> int:
        return self._fallbacks[event_id]
