"""Provas de observabilidade na borda HTTP e de publicação do pedido."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import sys

from fastapi.testclient import TestClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(0, str(Path(__file__).parents[1]))

from observability.telemetry import TelemetrySettings, configure_telemetry
from order_service.main import create_app
from order_service.persistence import OrderStore
from order_service.producer import InMemoryProducer, KafkaProducer


def test_http_do_pedido_exporta_trace_metricas_e_correlacao__spec_AC_014() -> None:
    """@spec:AC-014 Requisições HTTP geram telemetria identificada por serviço."""
    exporter = InMemorySpanExporter()
    telemetry = configure_telemetry(TelemetrySettings(service_name="order-service", export_enabled=True), span_exporter=exporter)
    requests = _Instrument()
    durations = _Instrument()
    telemetry._http_requests = requests
    telemetry._http_duration = durations
    app = create_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(), telemetry=telemetry)

    with TestClient(app) as client:
        response = client.post("/orders", headers={"Idempotency-Key": "obs-order"}, json={"items": [{"product_id": "tea", "quantity": 1, "price": "5.00"}]})

    telemetry.tracer_provider.force_flush()
    body = response.json()
    assert response.status_code == 202
    assert response.headers["X-Correlation-ID"] == body["correlation_id"]
    http_spans = [span for span in exporter.get_finished_spans() if span.name == "HTTP POST"]
    [span] = http_spans
    assert span.resource.attributes["service.name"] == "order-service"
    assert span.attributes["url.path"] == "/orders"
    assert span.attributes["http.response.status_code"] == 202
    assert requests.calls[0][1]["http.response.status_code"] == 202
    assert durations.calls[0][0] >= 0


def test_produtor_kafka_propaga_trace_e_identificadores_do_pedido__spec_AC_015() -> None:
    """@spec:AC-015 Contexto do trace atravessa os eventos Kafka."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="order-service", export_enabled=False))
    client = _KafkaClient()
    producer = KafkaProducer(producer_factory=lambda **_: client, telemetry=telemetry)
    event = {"event_id": "evt-1", "event_type": "order.created", "order_id": "order-1", "correlation_id": "corr-1"}

    async def publish() -> int:
        await producer.start()
        with telemetry.tracer.start_as_current_span("create order") as root:
            trace_id = root.get_span_context().trace_id
            await producer.publish("orders.events", event)
        return trace_id

    trace_id = asyncio.run(publish())
    headers = dict(client.calls[0]["headers"])
    assert headers["order_id"] == b"order-1"
    assert headers["correlation_id"] == b"corr-1"
    assert headers["traceparent"].decode().split("-")[1] == f"{trace_id:032x}"


def test_indisponibilidade_do_exportador_nao_impede_aceitar_pedido__spec_AC_018() -> None:
    """@spec:AC-018 Indisponibilidade da observabilidade não interrompe a saga."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="order-service", export_enabled=False))
    telemetry._http_requests = _BrokenInstrument()
    telemetry._http_duration = _BrokenInstrument()
    app = create_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(), telemetry=telemetry)

    with TestClient(app) as client:
        response = client.post("/orders", headers={"Idempotency-Key": "collector-down"}, json={"items": [{"product_id": "tea", "quantity": 1, "price": "5.00"}]})

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"


class _KafkaClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send_and_wait(self, topic: str, message: dict[str, str], *, key: str, headers: list[tuple[str, bytes]]) -> None:
        self.calls.append({"topic": topic, "message": message, "key": key, "headers": headers})


class _Instrument:
    def __init__(self) -> None:
        self.calls: list[tuple[float | int, dict[str, object]]] = []

    def add(self, value: int, *, attributes: dict[str, object]) -> None:
        self.calls.append((value, attributes))

    def record(self, value: float, *, attributes: dict[str, object]) -> None:
        self.calls.append((value, attributes))


class _BrokenInstrument:
    def add(self, *_: object, **__: object) -> None:
        raise ConnectionError("collector unavailable")

    def record(self, *_: object, **__: object) -> None:
        raise ConnectionError("collector unavailable")
