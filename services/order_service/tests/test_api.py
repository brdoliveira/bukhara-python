"""Provas da entrada HTTP de pedidos."""

from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parents[1]))

from order_service.main import create_app
from order_service.persistence import OrderStore
from order_service.producer import InMemoryProducer


def client_for(producer: InMemoryProducer) -> TestClient:
    return TestClient(create_app(OrderStore("sqlite+pysqlite:///:memory:"), producer))


def test_post_valid_order_is_accepted_and_published_once_spec_ac_001():
    """@spec:AC-001 Pedido válido é aceito e publicado uma única vez."""
    producer = InMemoryProducer()
    response = client_for(producer).post("/orders", headers={"Idempotency-Key": "new-order"}, json={"items": [{"product_id": "book", "quantity": 2, "price": "12.50"}]})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["order_id"] and body["correlation_id"]
    assert len(producer.messages) == 1
    topic, event = producer.messages[0]
    assert topic == "order.created"
    assert event["order_id"] == body["order_id"]
    assert event["correlation_id"] == body["correlation_id"]
    assert event["items"] == [{"product_id": "book", "quantity": 2, "price": "12.50"}]


def test_resubmitting_same_key_does_not_duplicate_order_or_event_spec_ac_002():
    """@spec:AC-002 Reenvio idempotente não duplica o pedido."""
    producer = InMemoryProducer()
    client = client_for(producer)
    payload = {"items": [{"product_id": "book", "quantity": 1, "price": "10.00"}]}

    first = client.post("/orders", headers={"Idempotency-Key": "stable-key"}, json=payload)
    retry = client.post("/orders", headers={"Idempotency-Key": "stable-key"}, json=payload)

    assert first.status_code == retry.status_code == 202
    assert retry.json()["order_id"] == first.json()["order_id"]
    assert len(producer.messages) == 1


def test_invalid_order_is_rejected_before_messaging_spec_ac_003():
    """@spec:AC-003 Pedido inválido é rejeitado antes da mensageria."""
    producer = InMemoryProducer()
    response = client_for(producer).post("/orders", headers={"Idempotency-Key": "bad-order"}, json={"items": [{"product_id": "", "quantity": 0, "price": "0"}]})

    assert response.status_code == 422
    fields = {error["loc"][-1] for error in response.json()["detail"]}
    assert {"product_id", "quantity", "price"}.issubset(fields)
    assert producer.messages == []
