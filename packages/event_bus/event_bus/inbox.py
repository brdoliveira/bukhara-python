"""Contrato de Inbox para impedir efeitos de negócio duplicados."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class InboxRepository(Protocol):
    """Persistence contract that serializes handling of a single event ID."""

    def claim(self, event_id: str) -> bool:
        """Atomically claim an unprocessed event, returning whether it was claimed."""
        ...

    def complete(self, event_id: str) -> None:
        """Mark a claimed event completed after its business effect succeeds."""
        ...

    def release(self, event_id: str) -> None:
        """Release a failed claim so a later delivery can process the event."""
        ...


@dataclass
class InMemoryInbox:
    """Referência para testes; Postgres deve persistir as mesmas transições."""

    _processing: set[str] = field(default_factory=set)
    _completed: set[str] = field(default_factory=set)

    def claim(self, event_id: str) -> bool:
        """Claim an event unless it is in-flight or already completed."""
        if event_id in self._processing or event_id in self._completed:
            return False
        self._processing.add(event_id)
        return True

    def complete(self, event_id: str) -> None:
        """Finalize an event and prevent any later duplicate from running."""
        self._processing.discard(event_id)
        self._completed.add(event_id)

    def release(self, event_id: str) -> None:
        """Remove only the in-flight marker after a recoverable failure."""
        self._processing.discard(event_id)

    def was_processed(self, event_id: str) -> bool:
        """Return whether an event reached the completed terminal state."""
        return event_id in self._completed
