import pytest

from services.notification_service.notification_service.adapter import InMemoryBroker, NotificationAdapter, TransientDependencyError
from services.notification_service.notification_service.consumer import NotificationConsumer
from services.notification_service.notification_service.handler import NotificationHandler
from services.notification_service.notification_service.persistence import NotificationRepository


def event(event_id="evt-1", **overrides):
    message = {
        "event_id": event_id,
        "type": "payment.approved",
        "order_id": "order-1",
        "correlation_id": "corr-1",
        "payload": {"amount": 42},
    }
    message.update(overrides)
    return message


def consumer(failures=()):
    repository = NotificationRepository()
    adapter = NotificationAdapter(send_failures=list(failures))
    broker = InMemoryBroker()
    handler = NotificationHandler(adapter, repository)
    return NotificationConsumer(handler, repository, broker), repository, adapter, broker


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-008"], ids=str)
def test_ac_008_falha_transitoria_usa_tres_retries_exponenciais_e_preserva_metadados__spec_AC_008(_spec_tag):
    service, _, _, broker = consumer([TransientDependencyError()] * 3)
    message = event()

    for attempt in range(3):
        assert service.consume(message).status == "retried"
        message = broker.retries[-1]["event"]
        assert message["event_id"] == "evt-1"
        assert message["correlation_id"] == "corr-1"
        assert message["retry_attempt"] == attempt + 1

    assert [entry["delay_seconds"] for entry in broker.retries] == [1, 2, 4]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-009"], ids=str)
def test_ac_009_retry_esgotado_executa_fallback_uma_vez_e_envia_dlq__spec_AC_009(_spec_tag):
    service, repository, adapter, broker = consumer([TransientDependencyError()] * 4)
    message = event()

    for _ in range(3):
        service.consume(message)
        message = broker.retries[-1]["event"]
    assert service.consume(message).status == "dlq"
    assert service.consume(message).status == "dlq"

    assert repository.fallback_count("evt-1") == 1
    assert adapter.fallbacks == ["order-1"]
    assert broker.dlq == [{
        "event": message,
        "error_type": "TransientDependencyError",
        "origin_service": "notification-service",
        "attempts": 3,
        "validation_errors": [],
    }]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-010"], ids=str)
def test_ac_010_evento_duplicado_e_confirmado_sem_repetir_notificacao__spec_AC_010(_spec_tag):
    service, repository, adapter, broker = consumer()

    assert service.consume(event()).status == "processed"
    assert service.consume(event()).status == "duplicate"

    assert repository.was_processed("evt-1")
    assert adapter.sent == ["order-1"]
    assert broker.acknowledged == ["evt-1", "evt-1"]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-011"], ids=str)
def test_ac_011_evento_invalido_e_isolado_na_dlq_sem_deter_consumidor__spec_AC_011(_spec_tag):
    service, _, adapter, broker = consumer()

    assert service.consume({"event_id": "broken", "type": "payment.approved"}).status == "invalid"
    assert service.consume(event("evt-valid")).status == "processed"

    assert broker.dlq[0]["validation_errors"]
    assert adapter.sent == ["order-1"]
