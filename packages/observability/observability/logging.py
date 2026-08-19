"""Logs estruturados que preservam contexto de trace e de negócio."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from opentelemetry import trace


class StructuredLogger:
    """Adaptador de ``logging`` que inclui correlação em cada mensagem."""

    def __init__(self, logger: logging.Logger, *, service_name: str) -> None:
        self._logger = logger
        self.service_name = service_name

    def info(self, message: str, *, order_id: Optional[str] = None, correlation_id: Optional[str] = None, **attributes: Any) -> None:
        """Emit an informational log with optional business correlation fields."""
        self.log(logging.INFO, message, order_id=order_id, correlation_id=correlation_id, **attributes)

    def error(self, message: str, *, order_id: Optional[str] = None, correlation_id: Optional[str] = None, **attributes: Any) -> None:
        """Emit an error log with optional business correlation fields."""
        self.log(logging.ERROR, message, order_id=order_id, correlation_id=correlation_id, **attributes)

    def log(self, level: int, message: str, *, order_id: Optional[str] = None, correlation_id: Optional[str] = None, **attributes: Any) -> None:
        """Attach active trace context and delegate to the standard logger."""
        context = trace.get_current_span().get_span_context()
        extra: dict[str, Any] = {
            "service.name": self.service_name,
            "trace_id": format(context.trace_id, "032x") if context.is_valid else None,
            "span_id": format(context.span_id, "016x") if context.is_valid else None,
            **attributes,
        }
        if order_id is not None:
            extra["order_id"] = order_id
        if correlation_id is not None:
            extra["correlation_id"] = correlation_id
        self._logger.log(level, message, extra=extra)


def get_logger(name: str, *, service_name: str) -> StructuredLogger:
    """Return a structured adapter for a named Python logger."""
    return StructuredLogger(logging.getLogger(name), service_name=service_name)


def configure_logging(*, service_name: str, otlp_endpoint: Optional[str] = None, export_enabled: bool = True) -> None:
    """Conecta logs do ``logging`` ao exportador OTLP em lote quando habilitado."""
    resolved_endpoint = otlp_endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not export_enabled or not resolved_endpoint:
        return
    try:
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource

        provider = LoggerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
        endpoint = resolved_endpoint.rstrip("/") + "/v1/logs"
        provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint)))
        set_logger_provider(provider)
        root = logging.getLogger()
        if not any(isinstance(handler, LoggingHandler) for handler in root.handlers):
            root.addHandler(LoggingHandler(level=logging.NOTSET, logger_provider=provider))
    except Exception:
        # Logging de observabilidade não é uma dependência do processamento de negócio.
        return
