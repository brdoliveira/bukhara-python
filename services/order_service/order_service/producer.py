"""Produtores Kafka de produção e doubles em memória para testes."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
from typing import Any, Awaitable, Callable, Optional, Protocol, Union

from observability.telemetry import Telemetry

try:
    from aiokafka import AIOKafkaProducer
except ImportError:  # pragma: no cover - a dependência é declarada no projeto.
    AIOKafkaProducer = None  # type: ignore[assignment,misc]

from .persistence import OrderStore


class EventProducer(Protocol):
    """Define o contrato de publicação e de prontidão para eventos de pedidos."""

    def publish(self, topic: str, message: dict[str, Any]) -> Union[None, Awaitable[None]]: ...
    def is_available(self) -> bool: ...


class InMemoryProducer:
    """Produtor determinístico em memória usado em testes e desenvolvimento local."""

    def __init__(self, available: bool = True) -> None:
        self.available = available
        self.messages: list[tuple[str, dict[str, Any]]] = []

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        """Armazena uma mensagem ou informa indisponibilidade simulada."""
        if not self.available:
            raise ConnectionError("Kafka indisponível")
        self.messages.append((topic, message))

    def is_available(self) -> bool:
        """Informa a disponibilidade configurada para o double de produtor."""
        return self.available

    async def start(self) -> None:
        """Implementa o ciclo de vida assíncrono sem abrir conexões."""
        return None

    async def stop(self) -> None:
        """Implementa o encerramento assíncrono sem recursos a liberar."""
        return None


class KafkaProducer:
    """Adaptador do AIOKafkaProducer que preserva disponibilidade observável."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        producer_factory: Optional[Callable[..., Any]] = None,
        telemetry: Optional[Telemetry] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._producer_factory = producer_factory
        self._producer: Any = None
        self._available = False
        self._lock = asyncio.Lock()
        self.telemetry = telemetry

    @classmethod
    def from_environment(cls, *, telemetry: Optional[Telemetry] = None) -> "KafkaProducer":
        """Cria o adaptador Kafka com o endereço configurado para o runtime."""
        return cls(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), telemetry=telemetry)

    async def start(self) -> None:
        """Inicia a conexão Kafka uma vez e expõe falhas como indisponibilidade."""
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
        """Encerra a conexão Kafka atual e torna o adaptador não pronto."""
        producer, self._producer = self._producer, None
        self._available = False
        if producer is not None:
            await producer.stop()

    async def publish(self, topic: str, message: dict[str, Any]) -> None:
        """Publica uma mensagem, incluindo telemetria, ou sinaliza falha de conexão."""
        if not self._available:
            await self.start()
        if self._producer is None or not self._available:
            raise ConnectionError("Kafka indisponível")
        try:
            if self.telemetry is None:
                await self._producer.send_and_wait(topic, message, key=message.get("order_id"))
                return
            with self.telemetry.kafka_publish(topic=topic, event=message) as headers:
                encoded_headers = [(key, value.encode("utf-8")) for key, value in headers.items()]
                await self._producer.send_and_wait(topic, message, key=message.get("order_id"), headers=encoded_headers)
            self.telemetry.record_event(event_type=str(message.get("event_type") or message.get("type") or "unknown"), result="published")
            self.telemetry.record_resilience(operation="outbox", event_type=str(message.get("event_type") or message.get("type") or "unknown"), result="drained")
        except Exception as exc:
            self._available = False
            raise ConnectionError("Falha ao publicar no Kafka") from exc

    def is_available(self) -> bool:
        """Informa se a conexão Kafka foi iniciada e permanece utilizável."""
        return self._available


class OutboxPublisher:
    """Drena eventos pendentes e os confirma somente após publicar no produtor."""

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
