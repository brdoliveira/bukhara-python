import pytest

from services.inventory_service.inventory_service.adapter import InventoryAdapter
from services.inventory_service.inventory_service.handler import InventoryHandler
from services.inventory_service.inventory_service.outbox import InMemoryOutbox
from services.inventory_service.inventory_service.persistence import InventoryRepository


def order_created(event_id="evt-order", order_id="order-1", correlation_id="corr-1", items=None):
    return {
        "event_id": event_id,
        "type": "order.created",
        "order_id": order_id,
        "correlation_id": correlation_id,
        "payload": {"items": items or [{"sku": "tea", "quantity": 2}]},
    }


def release_requested(event_id="evt-release", order_id="order-1", correlation_id="corr-1"):
    return {
        "event_id": event_id,
        "type": "inventory.release.requested",
        "order_id": order_id,
        "correlation_id": correlation_id,
        "payload": {},
    }


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-004"], ids=str)
def test_ac_004_estoque_disponivel_publica_reserva_com_identificadores__spec_AC_004(_spec_tag):
    repository = InventoryRepository({"tea": 3})
    outbox = InMemoryOutbox()
    handler = InventoryHandler(InventoryAdapter(repository), outbox, repository)

    events = handler.handle(order_created())

    assert [(event["type"], event["order_id"], event["correlation_id"]) for event in events] == [
        ("inventory.reserved", "order-1", "corr-1")
    ]
    assert repository.available("tea") == 1
    assert len(outbox.pending()) == 1


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-005"], ids=str)
def test_ac_005_estoque_indisponivel_rejeita_sem_solicitar_pagamento__spec_AC_005(_spec_tag):
    repository = InventoryRepository({"tea": 1})
    outbox = InMemoryOutbox()
    handler = InventoryHandler(InventoryAdapter(repository), outbox, repository)

    events = handler.handle(order_created(items=[{"sku": "tea", "quantity": 2}]))

    assert events[0]["type"] == "inventory.rejected"
    assert events[0]["payload"]["reason"] == "insufficient_stock"
    assert not [event for event in outbox.pending() if event["type"].startswith("payment.")]
    assert repository.available("tea") == 1


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-007"], ids=str)
def test_ac_007_pedido_de_liberacao_compensa_a_reserva_e_publica_liberacao__spec_AC_007(_spec_tag):
    repository = InventoryRepository({"tea": 3})
    outbox = InMemoryOutbox()
    handler = InventoryHandler(InventoryAdapter(repository), outbox, repository)
    handler.handle(order_created())

    events = handler.handle(release_requested())

    assert events[0]["type"] == "inventory.released"
    assert events[0]["order_id"] == "order-1"
    assert repository.available("tea") == 3
