"""Outbox do servi\u00e7o de estoque.

``InMemoryOutbox`` continua sendo deliberadamente simples para os testes
unit\u00e1rios.  ``PostgresOutbox`` usa a mesma API, mas guarda o evento na mesma
transa\u00e7\u00e3o que a reserva e a Inbox.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import uuid4


class Outbox(Protocol):
    def add(
        self,
        event_type: str,
        *,
        order_id: str,
        correlation_id: str,
        payload: dict[str, Any],
        causation_id: str | None = None,
    ) -> dict[str, Any]: ...


def event_for_outbox(
    event_type: str,
    *,
    order_id: str,
    correlation_id: str,
    payload: dict[str, Any],
    causation_id: str | None = None,
) -> dict[str, Any]:
    """Cria o envelope can\u00f4nico e conserva ``type`` para clientes legados."""
    return {
        "event_id": str(uuid4()),
        "type": event_type,
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "producer": "inventory-service",
        "order_id": order_id,
        "correlation_id": correlation_id,
        "causation_id": causation_id,
        "payload": deepcopy(payload),
    }


class InMemoryOutbox:
    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []

    def add(
        self,
        event_type: str,
        *,
        order_id: str,
        correlation_id: str,
        payload: dict[str, Any],
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        event = event_for_outbox(
            event_type,
            order_id=order_id,
            correlation_id=correlation_id,
            payload=payload,
            causation_id=causation_id,
        )
        self._events.append(event)
        return deepcopy(event)

    def pending(self) -> list[dict[str, Any]]:
        return deepcopy(self._events)


class PostgresOutbox:
    """Adaptador fino sobre o reposit\u00f3rio, para manter uma conex\u00e3o transacional."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def add(
        self,
        event_type: str,
        *,
        order_id: str,
        correlation_id: str,
        payload: dict[str, Any],
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        event = event_for_outbox(
            event_type,
            order_id=order_id,
            correlation_id=correlation_id,
            payload=payload,
            causation_id=causation_id,
        )
        self.repository.add_outbox(event, topic="inventory.events")
        return event

    def pending(self) -> list[dict[str, Any]]:
        return self.repository.pending_outbox()

    def mark_published(self, event_id: str) -> None:
        self.repository.mark_outbox_published(event_id)
