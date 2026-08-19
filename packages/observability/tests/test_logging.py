from __future__ import annotations

import logging

from observability.logging import get_logger
from observability.telemetry import TelemetrySettings, configure_telemetry


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_log_estruturado_correlaciona_servico_trace_e_pedido__spec_AC_016() -> None:
    """@spec:AC-016 Logs estruturados permitem correlação com traces e pedidos."""
    telemetry = configure_telemetry(TelemetrySettings(service_name="notification-service", export_enabled=False))
    raw_logger = logging.getLogger("observability-test")
    handler = _Capture()
    raw_logger.addHandler(handler)
    raw_logger.setLevel(logging.INFO)
    raw_logger.propagate = False

    with telemetry.tracer.start_as_current_span("notify"):
        get_logger("observability-test", service_name="notification-service").info(
            "notification sent", order_id="order-1", correlation_id="corr-1", event_type="payment.approved"
        )

    raw_logger.removeHandler(handler)
    [record] = handler.records
    assert record.levelname == "INFO"
    assert record.__dict__["service.name"] == "notification-service"
    assert record.trace_id and record.span_id
    assert record.order_id == "order-1"
    assert record.correlation_id == "corr-1"
