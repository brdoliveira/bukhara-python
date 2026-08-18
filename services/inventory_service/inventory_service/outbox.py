"""Outbox durável do serviço de estoque."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import uuid4


class InMemoryOutbox:
    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []

    def add(self, event_type: str, *, order_id: str, correlation_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = {
            "event_id": str(uuid4()),
            "type": event_type,
            "order_id": order_id,
            "correlation_id": correlation_id,
            "payload": deepcopy(payload),
        }
        self._events.append(event)
        return deepcopy(event)

    def pending(self) -> list[dict[str, Any]]:
        return deepcopy(self._events)
