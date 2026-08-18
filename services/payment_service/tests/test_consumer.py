import pytest

from services.payment_service.payment_service.adapter import InMemoryBroker, PaymentAdapter, TransientDependencyError
from services.payment_service.payment_service.consumer import PaymentConsumer
from services.payment_service.payment_service.handler import PaymentHandler
from services.payment_service.payment_service.outbox import InMemoryOutbox
from services.payment_service.payment_service.persistence import PaymentRepository


def event(event_id="evt-1", **overrides):
    message = {
        "event_id": event_id,
        "type": "inventory.reserved",
        "order_id": "order-1",
        "correlation_id": "corr-1",
        "payload": {"items": [{"sku": "tea", "quantity": 1}]},
    }
    message.update(overrides)
    return message


def consumer(failures=()):
    repository = PaymentRepository()
    adapter = PaymentAdapter(charge_failures=list(failures))
    broker = InMemoryBroker()
    handler = PaymentHandler(adapter, InMemoryOutbox(), repository)
    return PaymentConsumer(handler, repository, broker), repository, adapter, broker


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

    assert [retry["delay_seconds"] for retry in broker.retries] == [1, 2, 4]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-009"], ids=str)
def test_ac_009_retry_esgotado_executa_fallback_uma_vez_publica_compensacao_e_envia_dlq__spec_AC_009(_spec_tag):
    service, repository, _, broker = consumer([TransientDependencyError()] * 4)
    message = event()

    for _ in range(3):
        assert service.consume(message).status == "retried"
        message = broker.retries[-1]["event"]
    assert service.consume(message).status == "dlq"
    assert service.consume(message).status == "dlq"

    assert repository.fallback_count("evt-1") == 1
    assert [published["type"] for published in broker.published] == ["payment.failed", "inventory.release.requested"]
    assert broker.dlq[0]["error_type"] == "TransientDependencyError"
    assert broker.dlq[0]["origin_service"] == "payment-service"
    assert broker.dlq[0]["attempts"] == 3


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-010"], ids=str)
def test_ac_010_evento_duplicado_confirma_sem_repetir_cobranca_ou_publicacao__spec_AC_010(_spec_tag):
    service, repository, adapter, broker = consumer()

    assert service.consume(event()).status == "processed"
    assert service.consume(event()).status == "duplicate"

    assert repository.was_processed("evt-1")
    assert len(adapter.charges) == 1
    assert [published["type"] for published in broker.published] == ["payment.approved"]
    assert broker.acknowledged == ["evt-1", "evt-1"]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-011"], ids=str)
def test_ac_011_evento_invalido_e_isolado_na_dlq_sem_deter_consumidor__spec_AC_011(_spec_tag):
    service, _, adapter, broker = consumer()

    assert service.consume({"event_id": "broken", "type": "inventory.reserved"}).status == "invalid"
    assert service.consume(event("evt-valid")).status == "processed"

    assert broker.dlq[0]["validation_errors"]
    assert len(adapter.charges) == 1

