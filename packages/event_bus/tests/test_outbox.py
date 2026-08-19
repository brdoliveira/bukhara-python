from __future__ import annotations

import pytest

from event_bus.envelope import EventEnvelope
from event_bus.outbox import InMemoryOutbox


def make_event() -> EventEnvelope:
    return EventEnvelope.new(
        event_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        event_type="order.created",
        producer="order-service",
        correlation_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        payload={"order_id": "order-1"},
    )


def test_outbox_pendente_e_recuperada_apos_indisponibilidade__spec_AC_013() -> None:
    """@spec:AC-013 Outbox pendente é recuperada após indisponibilidade."""
    outbox = InMemoryOutbox()
    event = make_event()
    outbox.enqueue(event, "orders.events")

    with pytest.raises(ConnectionError):
        outbox.publish_pending(lambda *_: (_ for _ in ()).throw(ConnectionError("Kafka offline")))
    assert [record.event_id for record in outbox.pending()] == [event.event_id]

    published: list[tuple[str, EventEnvelope]] = []
    assert outbox.publish_pending(lambda topic, message: published.append((topic, message))) == 1
    assert published == [("orders.events", event)]
    assert outbox.pending() == []
