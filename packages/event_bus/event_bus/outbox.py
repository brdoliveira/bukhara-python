"""Contrato de Outbox para publicação recuperável e idempotente."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Callable, Optional, Protocol

from .envelope import EventEnvelope


@dataclass(frozen=True)
class OutboxRecord:
    """Persisted event and its publication status in the transactional Outbox."""
    event: EventEnvelope
    topic: str
    created_at: datetime
    published_at: Optional[datetime] = None

    @property
    def event_id(self) -> str:
        """Expose the stable event identifier used to acknowledge publication."""
        return self.event.event_id


class OutboxRepository(Protocol):
    """Persistence contract for durable, at-least-once event publication."""

    def enqueue(self, event: EventEnvelope, topic: str) -> OutboxRecord:
        """Persist an unpublished event in the same transaction as business state."""
        ...

    def pending(self) -> list[OutboxRecord]:
        """Return events still awaiting broker acknowledgement."""
        ...

    def mark_published(self, event_id: str) -> None:
        """Record a successful publication so recovery does not resend it."""
        ...


@dataclass
class InMemoryOutbox:
    """Referência para testes; adaptadores Postgres preservam esta API."""

    _records: dict[str, OutboxRecord] = field(default_factory=dict)

    def enqueue(self, event: EventEnvelope, topic: str) -> OutboxRecord:
        """Persist a unique event after validating that its destination is usable."""
        if not topic.strip():
            raise ValueError("topic must be non-empty")
        if event.event_id in self._records:
            raise ValueError(f"event {event.event_id} is already in the outbox")
        record = OutboxRecord(event=event, topic=topic, created_at=datetime.now(timezone.utc))
        self._records[event.event_id] = record
        return record

    def pending(self) -> list[OutboxRecord]:
        """List records whose publication timestamp has not been set."""
        return [record for record in self._records.values() if record.published_at is None]

    def mark_published(self, event_id: str) -> None:
        """Acknowledge one existing record after a successful broker write."""
        self._records[event_id] = replace(self._records[event_id], published_at=datetime.now(timezone.utc))

    def publish_pending(self, publish: Callable[[str, EventEnvelope], None]) -> int:
        """Publish pending records and acknowledge only successful callbacks.

        A callback exception intentionally leaves the current and subsequent
        records pending, allowing a later recovery loop to retry them.
        """
        published = 0
        for record in self.pending():
            publish(record.topic, record.event)
            self.mark_published(record.event_id)
            published += 1
        return published
