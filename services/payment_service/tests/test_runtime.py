"""Provas herméticas do ciclo de vida e da persistência do runtime de pagamento."""

from __future__ import annotations

import asyncio
import json

import pytest

from services.payment_service.payment_service.adapter import PaymentAdapter, TransientDependencyError
from services.payment_service.payment_service.main import PAYMENTS_TOPIC, PaymentRuntime


def reserved_event(*, retry_attempt: int = 0) -> dict[str, object]:
    return {
        "event_id": "evt-runtime",
        "type": "inventory.reserved",
        "order_id": "order-runtime",
        "correlation_id": "corr-runtime",
        "payload": {"items": [{"sku": "tea", "quantity": 1}]},
        "retry_attempt": retry_attempt,
    }


class _Repository:
    def __init__(self, *_: object) -> None:
        self.engine = object()
        self.initialized = False
        self.outcomes: list[tuple[dict[str, object], str, list[tuple[str, dict[str, object]]], bool]] = []
        self.terminal = False

    def initialize(self) -> None:
        self.initialized = True

    def is_available(self) -> bool:
        return True

    def is_terminal(self, _: str) -> bool:
        return self.terminal

    def persist_outcome(
        self, event: dict[str, object], outcome: str, events: list[tuple[str, dict[str, object]]], *, terminal: bool = False
    ) -> bool:
        self.outcomes.append((event, outcome, events, terminal))
        return True


class _Outbox:
    def __init__(self, _: object | None = None) -> None:
        self.rows: list[dict[str, object]] = []
        self.enqueued: list[tuple[dict[str, object], str]] = []
        self.published: list[object] = []

    def pending(self) -> list[dict[str, object]]:
        return list(self.rows)

    def enqueue(self, event: dict[str, object], *, topic: str, **_: object) -> None:
        self.enqueued.append((event, topic))

    def mark_published(self, row_id: object) -> None:
        self.published.append(row_id)


class _Producer:
    def __init__(self, **_: object) -> None:
        self.started = False
        self.stopped = False
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.client = self

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def force_metadata_update(self) -> None:
        return None

    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes, **_: object) -> None:
        self.messages.append((topic, key, value))


class _Consumer:
    def __init__(self, *_: object, **__: object) -> None:
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self) -> "_Consumer":
        return self

    async def __anext__(self) -> object:
        await asyncio.sleep(3600)
        raise StopAsyncIteration

    async def commit(self) -> None:
        return None


def test_runtime_inicia_e_para_workers_com_doubles__spec_AC_029(monkeypatch: pytest.MonkeyPatch) -> None:
    """@spec:AC-029 O runtime inicia dependências e encerra os workers com segurança."""
    monkeypatch.setattr("services.payment_service.payment_service.main.PostgresPaymentRepository", _Repository)
    monkeypatch.setattr("services.payment_service.payment_service.main.PostgresOutbox", _Outbox)
    runtime = PaymentRuntime(
        database_url="postgresql://payment",
        consumer_factory=_Consumer,
        producer_factory=_Producer,
    )

    async def scenario() -> None:
        await runtime.start()
        assert runtime.started is True
        assert runtime.repository is not None and runtime.repository.initialized is True
        assert await runtime.ready() is True
        await runtime.stop()

    asyncio.run(scenario())
    assert runtime.started is False
    assert runtime._tasks == []
    assert runtime.consumer is not None and runtime.consumer.stopped is True
    assert runtime.producer is not None and runtime.producer.stopped is True


def test_runtime_persiste_aprovacao_retry_e_falha_terminal__spec_AC_029() -> None:
    """@spec:AC-029 Aprovação, retry e fallback persistem os eventos corretos."""
    repository = _Repository()
    outbox = _Outbox()
    runtime = PaymentRuntime(adapter=PaymentAdapter())
    runtime.repository = repository
    runtime.outbox = outbox

    asyncio.run(runtime.process(reserved_event()))
    event, outcome, events, terminal = repository.outcomes.pop()
    assert event["event_id"] == "evt-runtime"
    assert outcome == "approved"
    assert terminal is False
    assert [(topic, item["type"]) for topic, item in events] == [(PAYMENTS_TOPIC, "payment.approved")]

    runtime.adapter = PaymentAdapter(charge_failures=[TransientDependencyError("temporary")])
    asyncio.run(runtime.process(reserved_event()))
    assert outbox.enqueued[0][1] == "payment.retry.1"
    assert outbox.enqueued[0][0]["retry_attempt"] == 1

    runtime.adapter = PaymentAdapter(charge_failures=[TransientDependencyError("terminal")])
    asyncio.run(runtime.process(reserved_event(retry_attempt=3)))
    _, outcome, events, terminal = repository.outcomes.pop()
    assert outcome == "unavailable"
    assert terminal is True
    assert [item["type"] for _, item in events] == ["payment.failed", "inventory.release.requested", "payment.dlq"]


def test_runtime_nao_republica_registro_ja_terminal__spec_AC_029() -> None:
    """@spec:AC-029 Um evento terminal duplicado não recobra nem cria nova Outbox."""
    repository = _Repository()
    repository.terminal = True
    runtime = PaymentRuntime(adapter=PaymentAdapter())
    runtime.repository = repository
    runtime.outbox = _Outbox()

    asyncio.run(runtime.process(reserved_event()))

    assert runtime.adapter.charges == []
    assert repository.outcomes == []


def test_publicacao_confirma_apenas_apos_ack_do_broker__spec_AC_029() -> None:
    """@spec:AC-029 Eventos pendentes só são marcados após publicação bem-sucedida."""
    runtime = PaymentRuntime()
    runtime.outbox = _Outbox()
    runtime.outbox.rows = [{"id": "outbox-1", "topic": PAYMENTS_TOPIC, "payload": json.dumps({"order_id": "order-runtime"})}]
    runtime.producer = _Producer()

    assert asyncio.run(runtime.publish_pending()) == 1
    assert runtime.outbox.published == ["outbox-1"]
    assert runtime.producer.messages[0][0] == PAYMENTS_TOPIC
