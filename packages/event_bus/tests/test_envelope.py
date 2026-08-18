from __future__ import annotations

from datetime import datetime, timezone

import pytest

from event_bus.envelope import EnvelopeValidationError, EventEnvelope

EVENT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
CORRELATION_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def test_envelope_versionado_serializa_e_preserva_causacao() -> None:
    event = EventEnvelope.new(
        event_id=EVENT_ID,
        event_type="order.created",
        event_version=1,
        producer="order-service",
        correlation_id=CORRELATION_ID,
        causation_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        payload={"order_id": "order-1"},
        occurred_at=datetime(2026, 8, 18, tzinfo=timezone.utc),
    )

    assert EventEnvelope.from_dict(event.to_dict()) == event


def test_evento_invalido_e_isolado_sem_impedir_proximo_evento__spec_AC_011() -> None:
    """@spec:AC-011 Evento inválido é isolado sem derrubar o consumidor."""
    invalid = {
        "event_id": "not-a-uuid",
        "event_type": "",
        "event_version": 0,
        "occurred_at": "invalid-date",
        "producer": "",
        "correlation_id": "not-a-uuid",
        "payload": [],
    }
    dlq: list[tuple[str, list[str]]] = []

    with pytest.raises(EnvelopeValidationError) as captured:
        EventEnvelope.from_dict(invalid)
    dlq.append(("EnvelopeValidationError", captured.value.errors))

    next_event = EventEnvelope.new(
        event_id=EVENT_ID,
        event_type="order.created",
        producer="order-service",
        correlation_id=CORRELATION_ID,
        payload={"order_id": "order-1"},
    )
    assert dlq[0][0] == "EnvelopeValidationError"
    assert dlq[0][1]
    assert next_event.event_id == EVENT_ID
