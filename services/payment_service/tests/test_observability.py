"""Provas de telemetria na drenagem da Outbox de pagamentos."""

from __future__ import annotations

import asyncio

from observability.telemetry import TelemetrySettings, configure_telemetry
from services.payment_service.payment_service.main import PaymentRuntime


def test_outbox_de_pagamento_propaga_trace_ao_publicar__spec_AC_015() -> None:
    """@spec:AC-015 Contexto do trace atravessa os eventos Kafka."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="payment-service", export_enabled=False))
    runtime = PaymentRuntime(telemetry=telemetry)
    runtime.outbox = _Outbox()
    runtime.producer = _Producer()

    async def publish() -> None:
        with telemetry.tracer.start_as_current_span("payment process"):
            assert await runtime.publish_pending() == 1

    asyncio.run(publish())
    headers = dict(runtime.producer.calls[0]["headers"])
    assert headers["traceparent"]
    assert headers["order_id"] == b"order-1"
    assert runtime.outbox.published == ["row-1"]


def test_drenagem_da_outbox_registra_metrica_de_resiliencia__spec_AC_017() -> None:
    """@spec:AC-017 Métricas de negócio e resiliência são exportadas."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="payment-service", export_enabled=False))
    resilience = _Instrument()
    telemetry._resilience = resilience
    runtime = PaymentRuntime(telemetry=telemetry)
    runtime.outbox = _Outbox()
    runtime.producer = _Producer()

    asyncio.run(runtime.publish_pending())

    assert resilience.calls == [(1, {"operation": "outbox", "event.type": "payment.approved", "result": "drained"})]


class _Outbox:
    def __init__(self) -> None:
        self.published: list[str] = []

    def pending(self) -> list[dict[str, object]]:
        return [{"id": "row-1", "topic": "payments.events", "payload": {"event_type": "payment.approved", "order_id": "order-1", "correlation_id": "corr-1"}}]

    def mark_published(self, row_id: str) -> None:
        self.published.append(row_id)


class _Producer:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def send_and_wait(self, topic: str, value: bytes, *, key: bytes, headers: list[tuple[str, bytes]]) -> None:
        self.calls.append({"topic": topic, "value": value, "key": key, "headers": headers})


class _Instrument:
    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, object]]] = []

    def add(self, value: int, *, attributes: dict[str, object]) -> None:
        self.calls.append((value, attributes))
