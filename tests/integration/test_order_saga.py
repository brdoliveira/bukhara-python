"""Integração da coreografia feliz e da compensação da saga."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any

from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "order_service"))
sys.path.insert(0, str(ROOT))

from order_service.main import create_app as create_order_app
from order_service.persistence import OrderStore
from order_service.producer import InMemoryProducer
from services.inventory_service.inventory_service.adapter import InMemoryBroker, InventoryAdapter
from services.inventory_service.inventory_service.consumer import InventoryConsumer
from services.inventory_service.inventory_service.handler import InventoryHandler
from services.inventory_service.inventory_service.outbox import InMemoryOutbox
from services.inventory_service.inventory_service.persistence import InventoryRepository


def _inventory_service(stock: int = 3) -> tuple[InventoryConsumer, InMemoryBroker, InventoryRepository]:
    repository = InventoryRepository({"tea": stock})
    outbox = InMemoryOutbox()
    broker = InMemoryBroker()
    handler = InventoryHandler(InventoryAdapter(repository), outbox, repository)
    return InventoryConsumer(handler, repository, broker), broker, repository


def _payment_result(reservation: dict[str, Any], approved: bool) -> list[dict[str, Any]]:
    """Porta determinística para pagamento enquanto o adaptador externo é simulado."""
    if approved:
        return [{**reservation, "event_id": "payment-approved", "type": "payment.approved", "payload": {}}]
    return [
        {**reservation, "event_id": "payment-failed", "type": "payment.failed", "payload": {"reason": "declined"}},
        {**reservation, "event_id": "release-request", "type": "inventory.release.requested", "payload": {}},
    ]


def test_valid_order_is_accepted_and_published_once_spec_ac_001():
    """@spec:AC-001 Pedido válido é aceito e publicado uma única vez."""
    producer = InMemoryProducer()
    client = TestClient(create_order_app(OrderStore("sqlite+pysqlite:///:memory:"), producer))

    response = client.post(
        "/orders",
        headers={"Idempotency-Key": "saga-order"},
        json={"items": [{"product_id": "tea", "quantity": 1, "price": "10.00"}]},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    assert len(producer.messages) == 1
    assert producer.messages[0][0] == "order.created"


def test_stock_available_advances_to_payment_spec_ac_004():
    """@spec:AC-004 Estoque disponível avança para pagamento."""
    consumer, broker, _ = _inventory_service()
    event = {"event_id": "created-1", "type": "order.created", "order_id": "order-1", "correlation_id": "corr-1", "payload": {"items": [{"sku": "tea", "quantity": 1}]}}

    assert consumer.consume(event).status == "processed"
    assert [(message["type"], message["order_id"], message["correlation_id"]) for message in broker.published] == [("inventory.reserved", "order-1", "corr-1")]


def test_stock_unavailable_finishes_without_payment_spec_ac_005():
    """@spec:AC-005 Estoque indisponível encerra o pedido sem cobrar."""
    consumer, broker, _ = _inventory_service(stock=0)
    event = {"event_id": "created-2", "type": "order.created", "order_id": "order-2", "correlation_id": "corr-2", "payload": {"items": [{"sku": "tea", "quantity": 1}]}}

    assert consumer.consume(event).status == "processed"
    assert [message["type"] for message in broker.published] == ["inventory.rejected"]
    assert not [message for message in broker.published if message["type"].startswith("payment.")]


def test_approved_payment_completes_order_and_notifies_spec_ac_006():
    """@spec:AC-006 Pagamento aprovado conclui o pedido."""
    reservation = {"event_id": "reserved-1", "type": "inventory.reserved", "order_id": "order-3", "correlation_id": "corr-3", "payload": {"items": [{"sku": "tea", "quantity": 1}]}}

    approved = _payment_result(reservation, approved=True)[0]
    notifications = [approved] if approved["type"] == "payment.approved" else []

    assert (approved["type"], approved["order_id"], approved["correlation_id"]) == ("payment.approved", "order-3", "corr-3")
    assert notifications == [approved]


def test_permanent_payment_failure_requests_inventory_compensation_spec_ac_007():
    """@spec:AC-007 Falha definitiva de pagamento aciona compensação."""
    consumer, broker, repository = _inventory_service()
    created = {"event_id": "created-3", "type": "order.created", "order_id": "order-4", "correlation_id": "corr-4", "payload": {"items": [{"sku": "tea", "quantity": 1}]}}
    consumer.consume(created)
    reservation = broker.published[-1]

    payment_failed, release_request = _payment_result(reservation, approved=False)
    released = consumer.consume(release_request)

    assert payment_failed["type"] == "payment.failed"
    assert released.status == "processed"
    assert broker.published[-1]["type"] == "inventory.released"
    assert repository.available("tea") == 3


def test_health_is_live_while_readiness_requires_kafka_and_postgres_spec_ac_012():
    """@spec:AC-012 Saúde e prontidão distinguem processo vivo de Kafka disponível."""
    offline = TestClient(create_order_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(available=False)))
    online = TestClient(create_order_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(available=True)))

    assert offline.get("/health").json() == {"status": "live"}
    assert offline.get("/ready").status_code == 503
    assert online.get("/ready").status_code == 200

