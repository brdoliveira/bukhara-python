"""Provas de rastreio e métricas no runtime de estoque."""

from __future__ import annotations

import asyncio

from observability.telemetry import TelemetrySettings, configure_telemetry
from services.inventory_service.inventory_service.main import KafkaDispatchBroker


def test_publicacao_de_retry_mantem_contexto_kafka__spec_AC_015() -> None:
    """@spec:AC-015 Contexto do trace atravessa os eventos Kafka."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="inventory-service", export_enabled=False))
    producer = _Producer()
    broker = KafkaDispatchBroker(producer, telemetry)
    event = {"event_id": "evt-1", "type": "order.created", "order_id": "order-1", "correlation_id": "corr-1", "payload": {}}

    async def publish() -> None:
        with telemetry.tracer.start_as_current_span("inventory consume"):
            await broker._send("inventory.retry.1", event)

    asyncio.run(publish())
    headers = dict(producer.calls[0]["headers"])
    assert headers["traceparent"]
    assert headers["order_id"] == b"order-1"
    assert headers["correlation_id"] == b"corr-1"


def test_metricas_de_resiliencia_nao_usam_ids_como_labels__spec_AC_017() -> None:
    """@spec:AC-017 Métricas de negócio e resiliência são exportadas."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="inventory-service", export_enabled=False))
    resilience = _Instrument()
    telemetry._resilience = resilience

    telemetry.record_resilience(operation="retry", event_type="order.created", result="retried")
    telemetry.record_resilience(operation="dlq", event_type="order.created", result="dlq")

    assert resilience.calls == [
        (1, {"operation": "retry", "event.type": "order.created", "result": "retried"}),
        (1, {"operation": "dlq", "event.type": "order.created", "result": "dlq"}),
    ]


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
