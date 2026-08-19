from __future__ import annotations

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from observability.telemetry import (
    HIGH_CARDINALITY_ATTRIBUTES,
    TelemetrySettings,
    configure_telemetry,
    extract_kafka_context,
    inject_kafka_context,
)


def test_http_gera_trace_e_metricas_identificados_pelo_servico__spec_AC_014() -> None:
    """@spec:AC-014 Requisições HTTP geram telemetria identificada por serviço."""
    exporter = InMemorySpanExporter()
    telemetry = configure_telemetry(TelemetrySettings(service_name="order-service", export_enabled=True), span_exporter=exporter)
    requests = _RecordingInstrument()
    durations = _RecordingInstrument()
    telemetry._http_requests = requests
    telemetry._http_duration = durations

    with telemetry.http_request(method="POST", route="/orders", correlation_id="corr-1"):
        telemetry.record_http_response(method="POST", route="/orders", status_code=202, duration_seconds=0.02)
    telemetry.tracer_provider.force_flush()

    [recorded] = exporter.get_finished_spans()
    assert recorded.resource.attributes["service.name"] == "order-service"
    assert recorded.attributes["http.request.method"] == "POST"
    assert recorded.attributes["url.path"] == "/orders"
    assert recorded.attributes["http.response.status_code"] == 202
    assert requests.calls == [(1, {"http.request.method": "POST", "url.path": "/orders", "http.response.status_code": 202})]
    assert durations.calls == [(0.02, {"http.request.method": "POST", "url.path": "/orders", "http.response.status_code": 202})]


def test_evento_kafka_preserva_trace_e_atributos_pesquisaveis__spec_AC_015() -> None:
    """@spec:AC-015 Contexto do trace atravessa os eventos Kafka."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="order-service", export_enabled=False))
    headers: dict[str, str] = {}
    with telemetry.tracer.start_as_current_span("publish") as publish_span:
        inject_kafka_context(headers, order_id="order-1", correlation_id="corr-1")
        published_trace_id = publish_span.get_span_context().trace_id

    context, attributes = extract_kafka_context(headers)
    with telemetry.tracer.start_as_current_span("consume", context=context, attributes=attributes) as consumer_span:
        assert consumer_span.get_span_context().trace_id == published_trace_id
        assert consumer_span.attributes == {"order_id": "order-1", "correlation_id": "corr-1"}
    assert "traceparent" in headers


def test_metricas_de_negocio_e_resiliencia_usam_somente_labels_de_baixa_cardinalidade__spec_AC_017() -> None:
    """@spec:AC-017 Métricas de negócio e resiliência são exportadas."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="inventory-service", export_enabled=False))
    events = _RecordingInstrument()
    resilience = _RecordingInstrument()
    telemetry._events = events
    telemetry._resilience = resilience

    telemetry.record_event(event_type="order.created", result="success")
    telemetry.record_resilience(operation="retry", event_type="order.created", result="scheduled")
    telemetry.record_resilience(operation="dlq", event_type="order.created", result="sent")

    assert HIGH_CARDINALITY_ATTRIBUTES == {"order_id", "event_id", "correlation_id"}
    assert events.calls == [(1, {"event.type": "order.created", "result": "success"})]
    assert resilience.calls == [
        (1, {"operation": "retry", "event.type": "order.created", "result": "scheduled"}),
        (1, {"operation": "dlq", "event.type": "order.created", "result": "sent"}),
    ]


def test_falha_do_exportador_nao_interrompe_operacao_de_negocio__spec_AC_018() -> None:
    """@spec:AC-018 Indisponibilidade da observabilidade não interrompe a saga."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="payment-service", export_enabled=False))

    class BrokenCounter:
        def add(self, *_: object, **__: object) -> None:
            raise ConnectionError("collector unavailable")

    telemetry._events = BrokenCounter()
    effect: list[str] = []
    telemetry.record_event(event_type="payment.approved", result="success")
    effect.append("payment-approved")

    assert effect == ["payment-approved"]


class _RecordingInstrument:
    def __init__(self) -> None:
        self.calls: list[tuple[float | int, dict[str, object]]] = []

    def add(self, value: int, *, attributes: dict[str, object]) -> None:
        self.calls.append((value, attributes))

    def record(self, value: float, *, attributes: dict[str, object]) -> None:
        self.calls.append((value, attributes))
