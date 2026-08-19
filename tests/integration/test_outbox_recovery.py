"""Integração da recuperação confiável de eventos pendentes na Outbox."""

from __future__ import annotations

from pathlib import Path
import sys
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "order_service"))

from order_service.outbox import recover_pending_events
from order_service.persistence import OrderStore, StoredOrder
from order_service.producer import InMemoryProducer, OutboxPublisher


def test_pending_outbox_is_recovered_after_kafka_unavailability_spec_ac_013():
    """@spec:AC-013 Outbox pendente é recuperada após indisponibilidade."""
    store = OrderStore("sqlite+pysqlite:///:memory:")
    order = StoredOrder(
        order_id=str(uuid4()),
        correlation_id=str(uuid4()),
        payload={"items": [{"product_id": "tea", "quantity": 1, "price": "10.00"}]},
        event_id=str(uuid4()),
    )
    store.create_order_with_outbox(order, "outbox-recovery")

    try:
        OutboxPublisher(store, InMemoryProducer(available=False)).publish_pending()
    except ConnectionError:
        pass
    assert [event.event_id for event in store.pending_events()] == [order.event_id]

    recovered = InMemoryProducer()
    assert recover_pending_events(OutboxPublisher(store, recovered)) == 1
    assert len(recovered.messages) == 1
    topic, event = recovered.messages[0]
    assert topic == "orders.events"
    assert event["event_id"] == order.event_id
    assert event["event_type"] == "order.created"
    assert event["event_version"] == 1
    assert event["producer"] == "order-service"
    assert event["order_id"] == order.order_id
    assert event["correlation_id"] == order.correlation_id
    assert event["payload"]["items"] == [{"product_id": "tea", "quantity": 1, "price": "10.00"}]
    assert store.pending_events() == []
