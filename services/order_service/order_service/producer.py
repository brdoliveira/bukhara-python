"""Adaptador do produtor Kafka e implementação em memória para testes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from .persistence import OutboxEvent, OrderStore


class EventProducer(Protocol):
    def publish(self, topic: str, message: dict[str, Any]) -> None: ...
    def is_available(self) -> bool: ...


class InMemoryProducer:
    def __init__(self, available: bool = True) -> None:
        self.available = available
        self.messages: list[tuple[str, dict[str, Any]]] = []

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        if not self.available:
            raise ConnectionError("Kafka indisponível")
        self.messages.append((topic, message))

    def is_available(self) -> bool:
        return self.available


class OutboxPublisher:
    def __init__(self, store: OrderStore, producer: EventProducer) -> None:
        self.store = store
        self.producer = producer

    def publish_pending(self) -> int:
        """Tenta cada evento pendente; falhas ficam para uma próxima execução."""
        published = 0
        for event in self.store.pending_events():
            self.producer.publish(event.topic, event.message())
            self.store.mark_published(event.event_id)
            published += 1
        return published
