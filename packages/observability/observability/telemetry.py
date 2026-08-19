"""Instrumentos OpenTelemetry de baixa cardinalidade para a saga.

As funções deste módulo nunca deixam uma falha de telemetria alcançar o fluxo
de negócio. Exportadores OTLP em lote fazem I/O em segundo plano; o código que
processa HTTP ou Kafka só registra dados no SDK local.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, MutableMapping, Optional

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


def _trace_endpoint(base: str) -> str:
    return base.rstrip("/") + "/v1/traces"


def _metric_endpoint(base: str) -> str:
    return base.rstrip("/") + "/v1/metrics"
