import pytest

from services.notification_service.notification_service.adapter import NotificationAdapter
from services.notification_service.notification_service.handler import NotificationHandler
from services.notification_service.notification_service.persistence import NotificationRepository


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-006"], ids=str)
def test_ac_006_pagamento_aprovado_processa_notificacao_de_pedido_concluido__spec_AC_006(_spec_tag):
    adapter = NotificationAdapter()
    handler = NotificationHandler(adapter, NotificationRepository())

    handler.handle({
        "event_id": "evt-payment", "type": "payment.approved", "order_id": "order-1",
        "correlation_id": "corr-1", "payload": {"amount": 42},
    })

    assert adapter.sent_notifications == [{
        "order_id": "order-1", "correlation_id": "corr-1", "kind": "order_completed",
    }]
