"""Testes herméticos do runtime Kafka/PostgreSQL do serviço de estoque."""

from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from services.inventory_service.inventory_service.adapter import InventoryAdapter, TransientDependencyError
from services.inventory_service.inventory_service.main import KafkaDispatchBroker, KafkaInventoryRuntime
from services.inventory_service.inventory_service.persistence import InventoryRepository


def order_created(*, event_id: str = "evt-runtime", retry_attempt: int = 0) -> dict[str, Any]:
    """Cria um evento válido sem depender de broker externo."""
    return {
        "event_id": event_id,
        "type": "order.created",
        "order_id": "order-runtime",
        "correlation_id": "corr-runtime",
        "payload": {"items": [{"sku": "tea", "quantity": 1}]},
        "retry_attempt": retry_attempt,
    }


class _Repository(InventoryRepository):
    """Double durável que expõe os efeitos do runtime sem PostgreSQL."""

    def __init__(self, *_: object) -> None:
        super().__init__({"tea": 3})
        self.initialized = False
        self.closed = 0
        self.seeded: dict[str, int] = {}
        self.outbox_rows: list[dict[str, Any]] = []
        self.published: list[str] = []

    def initialize(self) -> None:
        self.initialized = True

    def seed_stock(self, stock: dict[str, int]) -> None:
        self.seeded = dict(stock)

    def close(self) -> None:
        self.closed += 1

    def add_outbox(self, event: dict[str, Any], *, topic: str) -> None:
        self.outbox_rows.append({**event, "topic": topic})

    def pending_outbox(self) -> list[dict[str, Any]]:
        return [event for event in self.outbox_rows if event["event_id"] not in self.published]

    def mark_outbox_published(self, event_id: str) -> None:
        self.published.append(event_id)


class _Producer:
    """Produtor assíncrono que registra chamadas e atende o probe Kafka."""

    def __init__(self, **_: object) -> None:
        self.started = False
        self.stopped = 0
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.client = self

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped += 1

    async def force_metadata_update(self) -> None:
        return None

    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes, **_: object) -> None:
        self.messages.append((topic, key, value))


class _FailingProducer(_Producer):
    """Double que simula uma falha antes da confirmação do produtor."""

    async def send_and_wait(self, *_: object, **__: object) -> None:
        raise ConnectionError("Kafka indisponível")


class _Consumer:
    """Consumidor infinito cancelável, suficiente para validar o lifecycle."""

    def __init__(self, *_: object, **__: object) -> None:
        self.started = False
        self.stopped = 0

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped += 1

    def __aiter__(self) -> "_Consumer":
        return self

    async def __anext__(self) -> object:
        await asyncio.sleep(3600)
        raise StopAsyncIteration

    async def commit(self) -> None:
        return None


def _install_aiokafka(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instala doubles de aiokafka somente para a importação tardia do runtime."""
    monkeypatch.setitem(sys.modules, "aiokafka", SimpleNamespace(AIOKafkaConsumer=_Consumer, AIOKafkaProducer=_Producer))


def test_runtime_inicia_publica_outbox_e_para_com_doubles__spec_AC_028(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec:AC-028 O lifecycle não requer Kafka/PostgreSQL reais e libera todos os recursos."""
    _install_aiokafka(monkeypatch)
    monkeypatch.setattr("services.inventory_service.inventory_service.main.PostgresInventoryRepository", _Repository)
    runtime = KafkaInventoryRuntime(
        database_url="postgresql://inventory",
        bootstrap_servers="kafka:9092",
        initial_stock={"coffee": 2},
    )

    async def scenario() -> None:
        await runtime.start()
        assert runtime.repository.initialized is True
        assert runtime.repository.seeded == {"coffee": 2}
        assert runtime.producer is not None and runtime.producer.started is True
        assert runtime.consumer is not None and runtime.consumer.started is True
        await runtime.stop()
        await runtime.stop()

    asyncio.run(scenario())
    assert runtime.repository.closed == 2
    assert runtime.producer is not None and runtime.producer.stopped == 2
    assert runtime.consumer is not None and runtime.consumer.stopped == 2


def test_runtime_processa_duplicata_e_confirma_outbox_so_apos_envio__spec_AC_028() -> None:
    """@spec:AC-028 Inbox deduplica e Outbox só fica publicada após confirmação Kafka."""
    runtime = KafkaInventoryRuntime(database_url="postgresql://inventory", bootstrap_servers="kafka:9092")
    runtime.repository = _Repository()
    runtime.producer = _Producer()
    runtime.broker = KafkaDispatchBroker(runtime.producer)

    async def scenario() -> tuple[str, str]:
        first = await runtime.process_message(json.dumps(order_created()).encode())
        second = await runtime.process_message(order_created())
        return first, second

    assert asyncio.run(scenario()) == ("processed", "duplicate")
    assert runtime.repository.available("tea") == 2
    assert len(runtime.producer.messages) == 1
    assert runtime.repository.published == [runtime.repository.outbox_rows[0]["event_id"]]

    runtime.repository.outbox_rows.append({**order_created(event_id="pending"), "topic": "inventory.events"})
    runtime.producer = _FailingProducer()
    with pytest.raises(ConnectionError):
        asyncio.run(runtime.publish_pending_outbox())
    assert "pending" not in runtime.repository.published


def test_runtime_envia_retry_e_dlq_com_fallback_unico__spec_AC_028(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec:AC-028 Falhas transitórias reintentam e a terminal gera um único fallback e DLQ."""
    runtime = KafkaInventoryRuntime(database_url="postgresql://inventory", bootstrap_servers="kafka:9092")
    runtime.repository = _Repository()
    runtime.producer = _Producer()
    runtime.broker = KafkaDispatchBroker(runtime.producer)

    class _FailingAdapter(InventoryAdapter):
        def __init__(self, repository: InventoryRepository) -> None:
            super().__init__(repository, reserve_failures=[TransientDependencyError("temporary")])

    monkeypatch.setattr("services.inventory_service.inventory_service.main.InventoryAdapter", _FailingAdapter)
    assert asyncio.run(runtime.process_message(order_created())) == "retried"
    retry = json.loads(runtime.producer.messages[-1][2])
    assert retry["retry_attempt"] == 1

    assert asyncio.run(runtime.process_message(order_created(retry_attempt=3))) == "dlq"
    assert asyncio.run(runtime.process_message(order_created(retry_attempt=3))) == "dlq"
    dlq_messages = [json.loads(value) for topic, _, value in runtime.producer.messages if topic == "inventory.dlq"]
    assert len(dlq_messages) == 1
    assert runtime.repository.fallback_count("evt-runtime") == 1
