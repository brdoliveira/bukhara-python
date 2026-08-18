"""Integração dos caminhos de retry, DLQ e deduplicação."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from services.inventory_service.inventory_service.adapter import InMemoryBroker, InventoryAdapter, TransientDependencyError
from services.inventory_service.inventory_service.consumer import InventoryConsumer
from services.inventory_service.inventory_service.handler import InventoryHandler
from services.inventory_service.inventory_service.outbox import InMemoryOutbox
from services.inventory_service.inventory_service.persistence import InventoryRepository


def _event(event_id: str = "evt-1") -> dict:
    return {"event_id": event_id, "type": "order.created", "order_id": "order-1", "correlation_id": "corr-1", "payload": {"items": [{"sku": "tea", "quantity": 1}]}}


def _consumer(failures: tuple[Exception, ...] = ()) -> tuple[InventoryConsumer, InventoryRepository, InMemoryBroker]:
    repository = InventoryRepository({"tea": 3})
    broker = InMemoryBroker()
    handler = InventoryHandler(InventoryAdapter(repository, reserve_failures=failures), InMemoryOutbox(), repository)
    return InventoryConsumer(handler, repository, broker), repository, broker


def test_transient_failure_uses_three_exponential_retries_spec_ac_008():
    """@spec:AC-008 Falha transitória usa retry exponencial limitado."""
    consumer, _, broker = _consumer((TransientDependencyError(),) * 3)
    message = _event()

    for expected_attempt in range(1, 4):
        assert consumer.consume(message).status == "retried"
        message = broker.retries[-1]["event"]
        assert (message["event_id"], message["correlation_id"], message["retry_attempt"]) == ("evt-1", "corr-1", expected_attempt)

    assert [entry["delay_seconds"] for entry in broker.retries] == [1, 2, 4]


def test_exhausted_retry_uses_one_fallback_and_dlq_spec_ac_009():
    """@spec:AC-009 Retry esgotado usa fallback e DLQ."""
    consumer, repository, broker = _consumer((TransientDependencyError(),) * 4)
    message = _event()

    for _ in range(3):
        consumer.consume(message)
        message = broker.retries[-1]["event"]
    assert consumer.consume(message).status == "dlq"
    assert consumer.consume(message).status == "dlq"

    assert repository.fallback_count("evt-1") == 1
    assert [(entry["error_type"], entry["origin_service"], entry["attempts"]) for entry in broker.dlq] == [("TransientDependencyError", "inventory-service", 3)]


def test_duplicate_event_does_not_repeat_business_effect_spec_ac_010():
    """@spec:AC-010 Evento duplicado não repete efeito de negócio."""
    consumer, repository, broker = _consumer()
    message = _event()

    assert consumer.consume(message).status == "processed"
    assert consumer.consume(message).status == "duplicate"

    assert repository.available("tea") == 2
    assert [entry["type"] for entry in broker.published] == ["inventory.reserved"]


def test_invalid_event_isolated_and_consumer_stays_available_spec_ac_011():
    """@spec:AC-011 Evento inválido é isolado sem derrubar o consumidor."""
    consumer, _, broker = _consumer()

    assert consumer.consume({"event_id": "broken", "type": "order.created"}).status == "invalid"
    assert broker.dlq[0]["validation_errors"]
    assert consumer.consume(_event("valid-event")).status == "processed"

