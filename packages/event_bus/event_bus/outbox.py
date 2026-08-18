"""Contrato de Outbox para publicação recuperável e idempotente."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Callable, Optional, Protocol

from .envelope import EventEnvelope


@dataclass(frozen=True)
class OutboxRecord:
    event: EventEnvelope
    topic: str
    created_at: datetime
    published_at: Optional[datetime] = None

    @property
    def event_id(self) -> str:
        return self.event.event_id


class OutboxRepository(Protocol):
    def enqueue(self, event: EventEnvelope, topic: str) -> OutboxRecord: ...
    def pending(self) -> list[OutboxRecord]: ...
    def mark_published(self, event_id: str) -> None: ...


@dataclass
class InMemoryOutbox:
    """Referência para testes; adaptadores Postgres preservam esta API."""

    _records: dict[str, OutboxRecord] = field(default_factory=dict)

    def enqueue(self, event: EventEnvelope, topic: str) -> OutboxRecord:
        if not topic.strip():
            raise ValueError("topic must be non-empty")
        if event.event_id in self._records:
            raise ValueError(f"event {event.event_id} is already in the outbox")
        record = OutboxRecord(event=event, topic=topic, created_at=datetime.now(timezone.utc))
        self._records[event.event_id] = record
        return record

    def pending(self) -> list[OutboxRecord]:
        return [record for record in self._records.values() if record.published_at is None]

    def mark_published(self, event_id: str) -> None:
        self._records[event_id] = replace(self._records[event_id], published_at=datetime.now(timezone.utc))

    def publish_pending(self, publish: Callable[[str, EventEnvelope], None]) -> int:
        published = 0
        for record in self.pending():
            publish(record.topic, record.event)
            self.mark_published(record.event_id)
            published += 1
        return published
