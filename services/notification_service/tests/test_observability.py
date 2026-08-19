"""Provas de logs, traces e métricas do consumidor de notificações."""

from __future__ import annotations

import logging

from observability.telemetry import TelemetrySettings, configure_telemetry
from services.notification_service.notification_service.adapter import InMemoryBroker, NotificationAdapter
from services.notification_service.notification_service.consumer import NotificationConsumer
from services.notification_service.notification_service.handler import NotificationHandler
from services.notification_service.notification_service.persistence import NotificationRepository


def test_consumidor_registra_trace_logs_e_metricas_correlacionados__spec_AC_016_AC_017() -> None:
    """@spec:AC-016 Logs estruturados permitem correlação com traces e pedidos. @spec:AC-017 Métricas de negócio e resiliência são exportadas."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="notification-service", export_enabled=False))
    events = _Instrument()
    telemetry._events = events
    raw_logger = logging.getLogger("notification-service")
    capture = _Capture()
    raw_logger.addHandler(capture)
    raw_logger.setLevel(logging.INFO)
    raw_logger.propagate = False
    event = {"event_id": "evt-1", "type": "payment.approved", "order_id": "order-1", "correlation_id": "corr-1", "payload": {"amount": 42}}
    consumer = NotificationConsumer(NotificationHandler(NotificationAdapter(), NotificationRepository()), NotificationRepository(), InMemoryBroker(), telemetry)

    result = consumer.consume(event)

    raw_logger.removeHandler(capture)
    assert result.status == "processed"
    assert events.calls == [(1, {"event.type": "payment.approved", "result": "processed"})]
    [record] = capture.records
    assert record.__dict__["service.name"] == "notification-service"
    assert record.order_id == "order-1"
    assert record.correlation_id == "corr-1"
    assert record.trace_id and record.span_id


class _Instrument:
    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, object]]] = []

    def add(self, value: int, *, attributes: dict[str, object]) -> None:
        self.calls.append((value, attributes))


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
