"""Provas do provisionamento local da observabilidade."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"
DASHBOARD = ROOT / "observability" / "grafana" / "order-saga.json"
PROVISIONING = ROOT / "observability" / "grafana" / "dashboards.yaml"


def _service_block(compose: str, name: str) -> str:
    match = re.search(rf"^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [\w-]+:\n|\Z)", compose, re.MULTILINE | re.DOTALL)
    assert match, f"serviço {name} não foi encontrado no Compose"
    return match.group(0)


def test_business_and_resilience_metrics_are_exported_without_high_cardinality_labels_spec_ac_017() -> None:
    """@spec:AC-017 Métricas de negócio e resiliência são exportadas."""
    compose = COMPOSE.read_text(encoding="utf-8")
    dashboard = DASHBOARD.read_text(encoding="utf-8")

    for service in ("order-service", "inventory-service", "payment-service", "notification-service"):
        block = _service_block(compose, service)
        assert f"OTEL_SERVICE_NAME: {service}" in block
        assert "OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-lgtm:4318" in block
        assert "OTEL_EXPORTER_OTLP_PROTOCOL: http/protobuf" in block

    for metric in ("saga_event_processing_total", "saga_retry_total", "saga_fallback_total", "saga_dlq_total", "saga_outbox_drain_total"):
        assert metric in dashboard
    assert "service_name" in dashboard and "event_type" in dashboard and "result" in dashboard
    assert not any(label in dashboard for label in ("order_id", "event_id", "correlation_id"))


def test_lgtm_stack_is_available_for_otlp_http_spec_ac_019() -> None:
    """@spec:AC-019 Stack LGTM sobe pronta para receber OTLP."""
    compose = COMPOSE.read_text(encoding="utf-8")
    lgtm = _service_block(compose, "otel-lgtm")

    assert "image: grafana/otel-lgtm:0.30.0" in lgtm
    assert '"${GRAFANA_PORT:-3000}:3000"' in lgtm
    assert '"${OTEL_GRPC_PORT:-4317}:4317"' in lgtm
    assert '"${OTEL_HTTP_PORT:-4318}:4318"' in lgtm
    assert "dashboards.yaml:/otel-lgtm/grafana/conf/provisioning/dashboards/order-saga.yaml:ro" in lgtm
    assert "order-saga.json:/otel-lgtm/grafana/conf/provisioning/dashboards/custom/order-saga.json:ro" in lgtm


def test_order_saga_dashboard_is_provisioned_with_loki_and_tempo_drilldowns_spec_ac_020() -> None:
    """@spec:AC-020 Dashboard da saga é provisionado automaticamente."""
    provisioning = PROVISIONING.read_text(encoding="utf-8")
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))

    assert "path: /otel-lgtm/grafana/conf/provisioning/dashboards/custom" in provisioning
    assert dashboard["uid"] == "order-saga"
    titles = {panel["title"] for panel in dashboard["panels"]}
    assert {"Taxa de processamento por serviço", "Erros por serviço", "Duração p95 por serviço", "Eventos por tipo e resultado", "Retries por serviço", "DLQ por serviço"} <= titles
    links = json.dumps(dashboard["links"])
    assert "loki" in links
    assert "tempo" in links
