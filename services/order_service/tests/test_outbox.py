"""Provas da publicação confiável pela Outbox."""

import asyncio
from pathlib import Path
import sys
from typing import Optional
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).parents[1]))

from order_service.outbox import recover_pending_events
from order_service.persistence import OrderStore, StoredOrder
from order_service.producer import InMemoryProducer, KafkaProducer, OutboxPublisher


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
    assert online.messages[0][0] == "orders.events"
    assert online.messages[0][1]["event_id"] == order.event_id
    assert online.messages[0][1]["correlation_id"] == order.correlation_id
    assert online.messages[0][1]["event_type"] == online.messages[0][1]["type"] == "order.created"
    assert store.pending_events() == []


def test_async_kafka_producer_publishes_outbox_to_orders_events_without_broker():
    class KafkaDouble:
        def __init__(self, **_: object) -> None:
            self.started = False
            self.stopped = False
            self.sent: list[tuple[str, dict[str, object], Optional[str]]] = []

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

        async def send_and_wait(self, topic: str, message: dict[str, object], key: Optional[str] = None) -> None:
            self.sent.append((topic, message, key))

    async def scenario() -> None:
        store = OrderStore("sqlite+pysqlite:///:memory:")
        order = StoredOrder(str(uuid4()), str(uuid4()), {"items": []}, str(uuid4()))
        store.create_order_with_outbox(order, "kafka-double")
        doubles: list[KafkaDouble] = []

        def factory(**kwargs: object) -> KafkaDouble:
            double = KafkaDouble(**kwargs)
            doubles.append(double)
            return double

        producer = KafkaProducer("kafka:9092", producer_factory=factory)
        await producer.start()
        assert producer.is_available()
        assert await OutboxPublisher(store, producer).publish_pending_async() == 1
        assert doubles[0].sent[0][0] == "orders.events"
        assert doubles[0].sent[0][1]["event_type"] == "order.created"
        assert doubles[0].sent[0][1]["type"] == "order.created"
        await producer.stop()
        assert doubles[0].stopped

    asyncio.run(scenario())
