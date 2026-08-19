"""Instrumentos OpenTelemetry de baixa cardinalidade para a saga.

As funções deste módulo nunca deixam uma falha de telemetria alcançar o fluxo
de negócio. Exportadores OTLP em lote fazem I/O em segundo plano; o código que
processa HTTP ou Kafka só registra dados no SDK local.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Iterator, Mapping, MutableMapping, Optional
from uuid import uuid4

from opentelemetry import propagate, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter


HIGH_CARDINALITY_ATTRIBUTES = frozenset({"order_id", "event_id", "correlation_id"})
"""Identificadores que não podem ser labels de métricas."""


@dataclass(frozen=True)
class TelemetrySettings:
    """Configuração da telemetria de um microsserviço."""

    service_name: str
    otlp_endpoint: Optional[str] = None
    export_enabled: bool = True

    @property
    def endpoint(self) -> str:
        return self.otlp_endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")


class Telemetry:
    """Fachada para traces e métricas da aplicação, tolerante a falhas."""

    def __init__(self, *, service_name: str, tracer_provider: TracerProvider, meter_provider: MeterProvider) -> None:
        self.service_name = service_name
        self.tracer_provider = tracer_provider
        self.meter_provider = meter_provider
        self.tracer = tracer_provider.get_tracer("order-saga.telemetry")
        meter = meter_provider.get_meter("order-saga.telemetry")
        self._http_duration = meter.create_histogram("http.server.request.duration", unit="s")
        self._http_requests = meter.create_counter("http.server.requests")
        self._events = meter.create_counter("saga.events.processed")
        self._resilience = meter.create_counter("saga.resilience.operations")

    @contextmanager
    def http_request(
        self,
        *,
        method: str,
        route: str,
        correlation_id: Optional[str] = None,
    ) -> Iterator[Any]:
        """Cria o span HTTP e registra status/duração sem interromper a rota."""
        attributes: dict[str, str] = {"http.request.method": method, "url.path": route}
        if correlation_id:
            attributes["correlation_id"] = correlation_id
        try:
            span_context = self.tracer.start_as_current_span("HTTP " + method, attributes=attributes)
        except Exception:
            # Instrumentação é estritamente auxiliar; a rota não pode falhar por ela.
            yield _NoopSpan()
            return
        with span_context as span:
            yield span

    def record_http_response(self, *, method: str, route: str, status_code: int, duration_seconds: float) -> None:
        try:
            trace.get_current_span().set_attribute("http.response.status_code", status_code)
        except Exception:
            pass
        attributes = {
            "http.request.method": method,
            "url.path": route,
            "http.response.status_code": status_code,
        }
        self._safe_add(self._http_requests, 1, attributes)
        self._safe_record(self._http_duration, duration_seconds, attributes)

    def record_event(self, *, event_type: str, result: str) -> None:
        """Conta um evento de negócio, com labels seguros para séries temporais."""
        self._safe_add(self._events, 1, {"event.type": event_type, "result": result})

    def record_resilience(self, *, operation: str, event_type: str, result: str) -> None:
        """Conta retry, fallback, DLQ e Outbox sem IDs de alta cardinalidade."""
        self._safe_add(
            self._resilience,
            1,
            {"operation": operation, "event.type": event_type, "result": result},
        )

    @contextmanager
    def kafka_publish(self, *, topic: str, event: Mapping[str, Any]) -> Iterator[MutableMapping[str, str]]:
        """Cria um span produtor e devolve headers W3C para o evento Kafka."""
        attributes = _event_attributes(event)
        attributes.update({"messaging.system": "kafka", "messaging.operation": "publish", "messaging.destination.name": topic})
        try:
            span_context = self.tracer.start_as_current_span("kafka publish " + topic, attributes=attributes)
        except Exception:
            # A publicação de negócio não deve depender da criação de um span.
            yield {}
            return
        with span_context:
            headers: dict[str, str] = {}
            try:
                inject_kafka_context(headers, order_id=attributes.get("order_id"), correlation_id=attributes.get("correlation_id"))
            except Exception:
                # Falha no propagador não é motivo para cancelar o envio Kafka.
                headers = {}
            yield headers

    @contextmanager
    def kafka_consume(
        self, *, topic: str, event: Mapping[str, Any], headers: Mapping[str, Any] | None = None
    ) -> Iterator[Any]:
        """Restaura o trace pai de Kafka e associa identificadores ao span consumidor."""
        try:
            carrier = decode_kafka_headers(headers or {})
            context, propagated = extract_kafka_context(carrier)
        except Exception:
            context, propagated = None, {}
        attributes = _event_attributes(event)
        attributes.update(propagated)
        attributes.update({"messaging.system": "kafka", "messaging.operation": "process", "messaging.destination.name": topic})
        try:
            span_context = self.tracer.start_as_current_span("kafka process " + topic, context=context, attributes=attributes)
        except Exception:
            yield _NoopSpan()
            return
        with span_context as span:
            yield span

    @staticmethod
    def _safe_add(counter: Any, value: int, attributes: Mapping[str, Any]) -> None:
        if HIGH_CARDINALITY_ATTRIBUTES.intersection(attributes):
            raise ValueError("metric attributes cannot include high-cardinality identifiers")
        try:
            counter.add(value, attributes=attributes)
        except Exception:
            # O SDK/exportador indisponível jamais interrompe a saga.
            return

    @staticmethod
    def _safe_record(histogram: Any, value: float, attributes: Mapping[str, Any]) -> None:
        try:
            histogram.record(value, attributes=attributes)
        except Exception:
            return

    def shutdown(self) -> None:
        """Drena a telemetria em encerramentos normais, sem propagar falhas."""
        for provider in (self.tracer_provider, self.meter_provider):
            try:
                provider.shutdown()
            except Exception:
                pass


class _NoopSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        return None

    def set_status(self, status: Any) -> None:
        return None


def configure_telemetry(
    settings: TelemetrySettings,
    *,
    span_exporter: Optional[SpanExporter] = None,
    metric_reader: Optional[Any] = None,
) -> Telemetry:
    """Monta provedores locais; OTLP é exportado por processadores assíncronos."""
    resource = Resource.create({SERVICE_NAME: settings.service_name})
    tracer_provider = TracerProvider(resource=resource)
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader] if metric_reader else [])

    if settings.export_enabled:
        try:
            if span_exporter is None:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

                span_exporter = OTLPSpanExporter(endpoint=_trace_endpoint(settings.endpoint))
            tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))

            if metric_reader is None:
                from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

                meter_provider = MeterProvider(
                    resource=resource,
                    metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=_metric_endpoint(settings.endpoint)))],
                )
        except Exception:
            # Uma configuração OTLP inválida degrada para provedores locais vazios.
            pass

    return Telemetry(service_name=settings.service_name, tracer_provider=tracer_provider, meter_provider=meter_provider)


def inject_kafka_context(
    headers: MutableMapping[str, str],
    *,
    order_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> MutableMapping[str, str]:
    """Propaga o contexto W3C e os identificadores pesquisáveis no evento Kafka."""
    propagate.inject(headers)
    if order_id:
        headers["order_id"] = order_id
    if correlation_id:
        headers["correlation_id"] = correlation_id
    return headers


def extract_kafka_context(headers: Mapping[str, str]) -> tuple[Any, dict[str, str]]:
    """Extrai contexto pai e atributos de negócio para o span consumidor."""
    context = propagate.extract(headers)
    attributes = {key: headers[key] for key in ("order_id", "correlation_id") if headers.get(key)}
    return context, attributes


def decode_kafka_headers(headers: Mapping[str, Any] | Iterable[tuple[str, Any]]) -> dict[str, str]:
    """Normaliza headers do aiokafka (bytes) para o carrier aceito pelo propagador."""
    items = headers.items() if isinstance(headers, Mapping) else headers
    decoded: dict[str, str] = {}
    for key, value in items:
        if value is None:
            continue
        decoded[str(key)] = value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return decoded


def instrument_fastapi(app: Any, telemetry: Telemetry) -> None:
    """Adiciona telemetria HTTP e o cabeçalho de correlação a uma aplicação FastAPI."""
    if getattr(app.state, "_observability_http", False):
        return

    @app.middleware("http")
    async def observe_http(request: Any, call_next: Any) -> Any:
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid4())
        request.state.correlation_id = correlation_id
        started_at = perf_counter()
        status_code = 500
        with telemetry.http_request(method=request.method, route=request.url.path, correlation_id=correlation_id):
            try:
                response = await call_next(request)
                status_code = response.status_code
                response.headers["X-Correlation-ID"] = correlation_id
                return response
            finally:
                telemetry.record_http_response(
                    method=request.method,
                    route=request.url.path,
                    status_code=status_code,
                    duration_seconds=perf_counter() - started_at,
                )

    app.state._observability_http = True


def _event_attributes(event: Mapping[str, Any]) -> dict[str, str]:
    return {
        key: str(event[key])
        for key in ("order_id", "correlation_id", "event_id", "type", "event_type")
        if event.get(key) is not None
    }


def _trace_endpoint(base: str) -> str:
    return base.rstrip("/") + "/v1/traces"


def _metric_endpoint(base: str) -> str:
    return base.rstrip("/") + "/v1/metrics"
