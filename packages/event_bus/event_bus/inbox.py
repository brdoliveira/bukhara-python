"""Contrato de Inbox para impedir efeitos de negócio duplicados."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class InboxRepository(Protocol):
    def claim(self, event_id: str) -> bool: ...
    def complete(self, event_id: str) -> None: ...
    def release(self, event_id: str) -> None: ...


@dataclass
class InMemoryInbox:
    """Referência para testes; Postgres deve persistir as mesmas transições."""

    _processing: set[str] = field(default_factory=set)
    _completed: set[str] = field(default_factory=set)

    def claim(self, event_id: str) -> bool:
        if event_id in self._processing or event_id in self._completed:
            return False
        self._processing.add(event_id)
        return True

    def complete(self, event_id: str) -> None:
        self._processing.discard(event_id)
        self._completed.add(event_id)

    def release(self, event_id: str) -> None:
        self._processing.discard(event_id)

    def was_processed(self, event_id: str) -> bool:
        return event_id in self._completed
