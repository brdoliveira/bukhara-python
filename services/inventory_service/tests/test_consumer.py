import pytest

from services.inventory_service.inventory_service.adapter import InMemoryBroker, InventoryAdapter, TransientDependencyError
from services.inventory_service.inventory_service.consumer import InventoryConsumer
from services.inventory_service.inventory_service.handler import InventoryHandler
from services.inventory_service.inventory_service.outbox import InMemoryOutbox
from services.inventory_service.inventory_service.persistence import InventoryRepository


def event(event_id="evt-1", **overrides):
    message = {
        "event_id": event_id,
        "type": "order.created",
        "order_id": "order-1",
        "correlation_id": "corr-1",
        "payload": {"items": [{"sku": "tea", "quantity": 1}]},
    }
    message.update(overrides)
    return message


def consumer(stock=3, failures=()):
    repository = InventoryRepository({"tea": stock})
    adapter = InventoryAdapter(repository, reserve_failures=list(failures))
    outbox = InMemoryOutbox()
    broker = InMemoryBroker()
    handler = InventoryHandler(adapter, outbox, repository)
    return InventoryConsumer(handler, repository, broker), repository, broker


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-008"], ids=str)
def test_ac_008_falha_transitoria_gera_tres_retries_exponenciais_com_metadados_preservados__spec_AC_008(_spec_tag):
    service, _, broker = consumer(failures=[TransientDependencyError()] * 3)
    message = event()

    for attempt in range(3):
        result = service.consume(message)
        assert result.status == "retried"
        message = broker.retries[-1]["event"]
        assert message["event_id"] == "evt-1"
        assert message["correlation_id"] == "corr-1"
        assert message["retry_attempt"] == attempt + 1

    assert [retry["delay_seconds"] for retry in broker.retries] == [1, 2, 4]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-009"], ids=str)
def test_ac_009_retry_esgotado_executa_fallback_uma_vez_e_envia_dlq__spec_AC_009(_spec_tag):
    service, repository, broker = consumer(failures=[TransientDependencyError()] * 4)
    message = event()

    for _ in range(3):
        service.consume(message)
        message = broker.retries[-1]["event"]
    result = service.consume(message)
    repeated = service.consume(message)

    assert result.status == "dlq"
    assert repeated.status == "dlq"
    assert repository.fallback_count("evt-1") == 1
    assert len(broker.dlq) == 1
    assert broker.dlq[0]["error_type"] == "TransientDependencyError"
    assert broker.dlq[0]["origin_service"] == "inventory-service"
    assert broker.dlq[0]["attempts"] == 3


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-010"], ids=str)
def test_ac_010_evento_duplicado_confirma_sem_repetir_reserva_ou_publicacao__spec_AC_010(_spec_tag):
    service, repository, broker = consumer()
    message = event()

    first = service.consume(message)
    second = service.consume(message)

    assert first.status == "processed"
    assert second.status == "duplicate"
    assert repository.available("tea") == 2
    assert [published["type"] for published in broker.published] == ["inventory.reserved"]
    assert broker.acknowledged == ["evt-1", "evt-1"]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-011"], ids=str)
def test_ac_011_evento_invalido_vai_para_dlq_e_consumidor_segue_processando__spec_AC_011(_spec_tag):
    service, _, broker = consumer()

    invalid = service.consume({"event_id": "broken", "type": "order.created"})
    valid = service.consume(event("evt-valid"))

    assert invalid.status == "invalid"
    assert broker.dlq[0]["validation_errors"]
    assert valid.status == "processed"
    assert broker.published[-1]["event_id"] != "evt-valid"
