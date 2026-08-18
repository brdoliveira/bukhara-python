"""Outbox do payment-service, desacoplada do publicador Kafka."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


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
        event = {
            "event_id": str(uuid4()),
            "type": event_type,
            "event_type": event_type,
            "event_version": 1,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "producer": "payment-service",
            "order_id": order_id,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "payload": deepcopy(payload),
        }
        self._events.append(event)
        return deepcopy(event)

    def pending(self) -> list[dict[str, Any]]:
        return deepcopy(self._events)


class PostgresOutbox:
    """Outbox durável; o publicador confirma somente após o ACK do Kafka."""

    def __init__(self, engine: Any) -> None:
        self.engine = engine

    def enqueue(self, event: dict[str, Any], *, topic: str, available_at: datetime | None = None) -> None:
        from sqlalchemy import text

        import json

        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO payment_outbox (event_id, topic, payload, available_at) "
                    "VALUES (:event_id, :topic, CAST(:payload AS jsonb), :available_at) "
                    "ON CONFLICT (event_id) DO NOTHING"
                ),
                {
                    "event_id": str(uuid4()),
                    "topic": topic,
                    "payload": json.dumps(event),
                    "available_at": available_at or datetime.now(timezone.utc),
                },
            )

    def pending(self, limit: int = 100) -> list[dict[str, Any]]:
        from sqlalchemy import text

        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT id, topic, payload FROM payment_outbox "
                    "WHERE published_at IS NULL AND available_at <= NOW() ORDER BY id LIMIT :limit"
                ),
                {"limit": limit},
            ).mappings()
            return [dict(row) for row in rows]

    def mark_published(self, record_id: int) -> None:
        from sqlalchemy import text

        with self.engine.begin() as connection:
            connection.execute(text("UPDATE payment_outbox SET published_at = NOW() WHERE id = :id"), {"id": record_id})
