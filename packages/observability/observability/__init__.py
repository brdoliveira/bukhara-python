"""Contratos compartilhados de telemetria da saga de pedidos."""

from .logging import StructuredLogger, configure_logging, get_logger
from .telemetry import (
    HIGH_CARDINALITY_ATTRIBUTES,
    Telemetry,
    TelemetrySettings,
    configure_telemetry,
    extract_kafka_context,
    inject_kafka_context,
)

__all__ = [
    "HIGH_CARDINALITY_ATTRIBUTES",
    "StructuredLogger",
    "Telemetry",
    "TelemetrySettings",
    "configure_logging",
    "configure_telemetry",
    "extract_kafka_context",
    "get_logger",
    "inject_kafka_context",
]
