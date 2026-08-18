"""Produtores Kafka de produção e doubles em memória para testes."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
from typing import Any, Awaitable, Callable, Optional, Protocol, Union

try:
    from aiokafka import AIOKafkaProducer
except ImportError:  # pragma: no cover - a dependência é declarada no projeto.
    AIOKafkaProducer = None  # type: ignore[assignment,misc]

from .persistence import OrderStore


class EventProducer(Protocol):
    def publish(self, topic: str, message: dict[str, Any]) -> Union[None, Awaitable[None]]: ...
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

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


class KafkaProducer:
    """Adaptador do AIOKafkaProducer que preserva disponibilidade observável."""

    def __init__(self, bootstrap_servers: Optional[str] = None, producer_factory: Optional[Callable[..., Any]] = None) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._producer_factory = producer_factory
        self._producer: Any = None
        self._available = False
        self._lock = asyncio.Lock()

    @classmethod
    def from_environment(cls) -> "KafkaProducer":
        return cls(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))

    async def start(self) -> None:
        if self._available:
            return
        async with self._lock:
            if self._available:
                return
            factory = self._producer_factory or AIOKafkaProducer
            if factory is None:
                self._available = False
                return
            producer = factory(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
                key_serializer=lambda key: str(key).encode("utf-8"),
                request_timeout_ms=3_000,
            )
            try:
                await producer.start()
            except Exception:
                try:
                    await producer.stop()
                except Exception:
                    pass
                self._available = False
                return
            self._producer = producer
            self._available = True

    async def stop(self) -> None:
        producer, self._producer = self._producer, None
        self._available = False
        if producer is not None:
            await producer.stop()

    async def publish(self, topic: str, message: dict[str, Any]) -> None:
        if not self._available:
            await self.start()
        if self._producer is None or not self._available:
            raise ConnectionError("Kafka indisponível")
        try:
            await self._producer.send_and_wait(topic, message, key=message.get("order_id"))
        except Exception as exc:
            self._available = False
            raise ConnectionError("Falha ao publicar no Kafka") from exc

    def is_available(self) -> bool:
        return self._available


class OutboxPublisher:
    def __init__(self, store: OrderStore, producer: EventProducer) -> None:
        self.store = store
        self.producer = producer

    def publish_pending(self) -> int:
        """Mantém o contrato síncrono dos doubles usados nos testes existentes."""
        published = 0
        for event in self.store.pending_events():
            result = self.producer.publish(event.topic, event.message())
            if inspect.isawaitable(result):
                close = getattr(result, "close", None)
                if close is not None:
                    close()
                raise RuntimeError("KafkaProducer exige publish_pending_async")
            self.store.mark_published(event.event_id)
            published += 1
        return published

    async def publish_pending_async(self) -> int:
        """Publica e só confirma na Outbox após a confirmação do Kafka."""
        published = 0
        for event in self.store.pending_events():
            result = self.producer.publish(event.topic, event.message())
            if inspect.isawaitable(result):
                await result
            self.store.mark_published(event.event_id)
            published += 1
        return published
