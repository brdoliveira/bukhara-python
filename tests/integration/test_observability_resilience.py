from __future__ import annotations

import logging

import pytest

import observability.logging as logging_module
import observability.telemetry as telemetry_module
from observability.logging import configure_logging, get_logger
from observability.telemetry import TelemetrySettings, configure_telemetry, decode_kafka_headers


def test_logging_otlp_configura_handler_uma_vez_e_degrada_sem_exportador(monkeypatch) -> None:
    """@spec:AC-026 Logging opcional pode falhar sem interromper a aplicação."""
    import opentelemetry._logs as logs_api
    import opentelemetry.exporter.otlp.proto.http._log_exporter as exporter_module
    import opentelemetry.sdk._logs as sdk_logs
    import opentelemetry.sdk._logs.export as sdk_export

    created: dict[str, object] = {}

    class Provider:
        def __init__(self, *, resource: object) -> None:
            created["resource"] = resource

        def add_log_record_processor(self, processor: object) -> None:
            created["processor"] = processor

    class Exporter:
        def __init__(self, *, endpoint: str) -> None:
            created["endpoint"] = endpoint

    class Processor:
        def __init__(self, exporter: object) -> None:
            created["exporter"] = exporter

    class Handler(logging.Handler):
        def __init__(self, *, level: int, logger_provider: object) -> None:
            super().__init__(level)
            created["handler_provider"] = logger_provider

        def emit(self, record: logging.LogRecord) -> None:
            return None

    monkeypatch.setattr(logs_api, "set_logger_provider", lambda provider: created.setdefault("provider", provider))
    monkeypatch.setattr(exporter_module, "OTLPLogExporter", Exporter)
    monkeypatch.setattr(sdk_logs, "LoggerProvider", Provider)
    monkeypatch.setattr(sdk_logs, "LoggingHandler", Handler)
    monkeypatch.setattr(sdk_export, "BatchLogRecordProcessor", Processor)

    root = logging.getLogger()
    configure_logging(service_name="orders", otlp_endpoint="http://collector/")
    configure_logging(service_name="orders", otlp_endpoint="http://collector/")
    assert created["endpoint"] == "http://collector/v1/logs"
    assert sum(isinstance(handler, Handler) for handler in root.handlers) == 1
    for handler in list(root.handlers):
        if isinstance(handler, Handler):
            root.removeHandler(handler)

    monkeypatch.setattr(exporter_module, "OTLPLogExporter", lambda **_: (_ for _ in ()).throw(RuntimeError("offline")))
    configure_logging(service_name="orders", otlp_endpoint="http://offline")
    configure_logging(service_name="orders", export_enabled=False)

    logger = get_logger("observability-error-test", service_name="orders")
    logger.error("collector unavailable")


def test_instrumentacao_degradada_preserva_fluxo_e_rejeita_labels_de_alta_cardinalidade(monkeypatch) -> None:
    """@spec:AC-026 Falhas locais de trace e métrica nunca alcançam a saga."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="orders", export_enabled=False))

    class BrokenTracer:
        def start_as_current_span(self, *_: object, **__: object) -> object:
            raise RuntimeError("tracer unavailable")

    class BrokenInstrument:
        def add(self, *_: object, **__: object) -> None:
            raise RuntimeError("meter unavailable")

        def record(self, *_: object, **__: object) -> None:
            raise RuntimeError("meter unavailable")

    telemetry.tracer = BrokenTracer()
    with telemetry.http_request(method="GET", route="/ready") as span:
        span.set_attribute("safe", True)
        span.set_status("ok")
    with telemetry.kafka_publish(topic="orders.events", event={"order_id": "order-1"}) as headers:
        assert headers == {}

    monkeypatch.setattr(
        telemetry_module,
        "decode_kafka_headers",
        lambda *_: (_ for _ in ()).throw(ValueError("invalid headers")),
    )
    with telemetry.kafka_consume(topic="orders.events", event={}, headers={}) as span:
        span.set_attribute("safe", True)

    monkeypatch.setattr(
        telemetry_module.trace,
        "get_current_span",
        lambda: (_ for _ in ()).throw(RuntimeError("no span")),
    )
    telemetry._http_requests = BrokenInstrument()
    telemetry._http_duration = BrokenInstrument()
    telemetry.record_http_response(method="GET", route="/ready", status_code=503, duration_seconds=0.1)
    telemetry._safe_record(BrokenInstrument(), 0.1, {})
    with pytest.raises(ValueError, match="high-cardinality"):
        telemetry._safe_add(BrokenInstrument(), 1, {"order_id": "order-1"})


def test_propagacao_configuracao_e_shutdown_toleram_dependencias_parciais(monkeypatch) -> None:
    """@spec:AC-026 Configuração parcial e encerramento preservam disponibilidade."""
    assert decode_kafka_headers([("traceparent", b"value"), ("ignored", None)]) == {"traceparent": "value"}
    assert telemetry_module._trace_endpoint("http://collector/") == "http://collector/v1/traces"
    assert telemetry_module._metric_endpoint("http://collector/") == "http://collector/v1/metrics"

    telemetry = configure_telemetry(TelemetrySettings(service_name="orders", export_enabled=False))
    monkeypatch.setattr(
        telemetry_module,
        "inject_kafka_context",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("propagator unavailable")),
    )
    with telemetry.kafka_publish(topic="orders.events", event={"event_type": "order.created"}) as headers:
        assert headers == {}

    class Provider:
        def __init__(self, *, broken: bool) -> None:
            self.broken = broken
            self.calls = 0

        def shutdown(self) -> None:
            self.calls += 1
            if self.broken:
                raise RuntimeError("shutdown failed")

    tracer_provider = Provider(broken=True)
    meter_provider = Provider(broken=False)
    telemetry.tracer_provider = tracer_provider
    telemetry.meter_provider = meter_provider
    telemetry.shutdown()
    assert tracer_provider.calls == meter_provider.calls == 1

    monkeypatch.setattr(
        telemetry_module,
        "BatchSpanProcessor",
        lambda *_: (_ for _ in ()).throw(RuntimeError("invalid exporter")),
    )
    degraded = configure_telemetry(
        TelemetrySettings(service_name="orders", export_enabled=True),
        span_exporter=object(),
    )
    assert degraded.service_name == "orders"
