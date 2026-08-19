"""Provas de recuperação de falhas de publicação do serviço de pedidos."""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))

from order_service.main import _publish_outbox_forever, create_app
from order_service.outbox import recover_pending_events
from order_service.persistence import OrderStore, StoredOrder
from order_service.producer import InMemoryProducer, OutboxPublisher


def test_publication_failure_keeps_order_for_recovery_and_updates_readiness__spec_AC_027() -> None:
    """@spec:AC-027 Uma falha preserva o pedido e a Outbox até a recuperação."""
    store = OrderStore("sqlite+pysqlite:///:memory:")
    producer = InMemoryProducer(available=False)
    app = create_app(store, producer)

    with TestClient(app) as client:
        response = client.post(
            "/orders",
            headers={"Idempotency-Key": "outbox-recovery"},
            json={"items": [{"product_id": "book", "quantity": 1, "price": "10.00"}]},
        )

        assert response.status_code == 202
        order_id = response.json()["order_id"]
        [pending] = store.pending_events()
        assert pending.order_id == order_id
        assert client.get("/ready").status_code == 503

        producer.available = True
        assert recover_pending_events(app.state.outbox_publisher) == 1
        assert store.pending_events() == []
        assert producer.messages[0][1]["order_id"] == order_id
        assert client.get("/ready").status_code == 200


def test_recovery_loop_retries_pending_event_after_transient_error__spec_AC_027(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec:AC-027 O loop tenta novamente eventos pendentes após erro transitório."""
    store = OrderStore("sqlite+pysqlite:///:memory:")
    order = StoredOrder(str(uuid4()), str(uuid4()), {"items": []}, str(uuid4()))
    store.create_order_with_outbox(order, "retry-in-loop")
    producer = _FailOnceProducer()
    pauses = 0

    async def stop_after_retry(_: float) -> None:
        nonlocal pauses
        pauses += 1
        if pauses == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr("order_service.main.asyncio.sleep", stop_after_retry)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_publish_outbox_forever(OutboxPublisher(store, producer)))

    assert producer.attempts == 2
    assert producer.messages[0]["event_id"] == order.event_id
    assert store.pending_events() == []


class _FailOnceProducer:
    """Double que representa uma indisponibilidade transitória do produtor."""

    def __init__(self) -> None:
        self.attempts = 0
        self.messages: list[dict[str, object]] = []

    async def publish(self, _: str, message: dict[str, object]) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise ConnectionError("Kafka indisponível")
        self.messages.append(message)

    def is_available(self) -> bool:
        """Atende ao protocolo usado pelo publicador durante o teste."""
        return True
