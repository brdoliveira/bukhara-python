"""Provas da publicação confiável pela Outbox."""

from pathlib import Path
import sys
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parents[1]))

from order_service.outbox import recover_pending_events
from order_service.persistence import OrderStore, StoredOrder
from order_service.producer import InMemoryProducer, OutboxPublisher


def test_pending_outbox_is_recovered_after_kafka_returns_spec_ac_013():
    """@spec:AC-013 Outbox pendente é recuperada após indisponibilidade."""
    store = OrderStore("sqlite+pysqlite:///:memory:")
    order = StoredOrder(str(uuid4()), str(uuid4()), {"items": [{"product_id": "book", "quantity": 1, "price": "10.00"}]}, str(uuid4()))
    store.create_order_with_outbox(order, "recover-key")
    offline = InMemoryProducer(available=False)
    publisher = OutboxPublisher(store, offline)

    try:
        publisher.publish_pending()
    except ConnectionError:
        pass
    assert [event.event_id for event in store.pending_events()] == [order.event_id]

    online = InMemoryProducer(available=True)
    assert recover_pending_events(OutboxPublisher(store, online)) == 1
    assert online.messages[0][1]["event_id"] == order.event_id
    assert online.messages[0][1]["correlation_id"] == order.correlation_id
    assert store.pending_events() == []
