import pytest

from services.payment_service.payment_service.adapter import PaymentAdapter, PaymentDeclinedError
from services.payment_service.payment_service.handler import PaymentHandler
from services.payment_service.payment_service.outbox import InMemoryOutbox
from services.payment_service.payment_service.persistence import PaymentRepository


def reserved_event(event_id="evt-reserved", **overrides):
    event = {
        "event_id": event_id,
        "type": "inventory.reserved",
        "order_id": "order-1",
        "correlation_id": "corr-1",
        "payload": {"items": [{"sku": "tea", "quantity": 1}]},
    }
    event.update(overrides)
    return event


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-006"], ids=str)
def test_ac_006_pagamento_aprovado_publica_evento_com_identificadores__spec_AC_006(_spec_tag):
    adapter = PaymentAdapter()
    handler = PaymentHandler(adapter, InMemoryOutbox(), PaymentRepository())

    emitted = handler.handle(reserved_event())

    assert [(event["type"], event["order_id"], event["correlation_id"]) for event in emitted] == [
        ("payment.approved", "order-1", "corr-1")
    ]
    assert adapter.charges == [{"order_id": "order-1", "payload": {"items": [{"sku": "tea", "quantity": 1}]}}]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-007"], ids=str)
def test_ac_007_recusa_definitiva_publica_falha_e_solicita_liberacao__spec_AC_007(_spec_tag):
    handler = PaymentHandler(
        PaymentAdapter(charge_failures=[PaymentDeclinedError("declined")]),
        InMemoryOutbox(),
        PaymentRepository(),
    )

    emitted = handler.handle(reserved_event())

    assert [(event["type"], event["payload"]["reason"]) for event in emitted] == [
        ("payment.failed", "payment_declined"),
        ("inventory.release.requested", "payment_declined"),
    ]

