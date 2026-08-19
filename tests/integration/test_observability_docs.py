from pathlib import Path


ROOT = Path(__file__).parents[2]
RUNBOOK = ROOT / "docs" / "observability.md"
README = ROOT / "README.md"


def test_ac_020_runbook_describes_provisioned_dashboard_and_drilldowns():
    """@spec:AC-020 Dashboard da saga é provisionado automaticamente."""
    text = RUNBOOK.read_text(encoding="utf-8").lower()
    assert "provisionado automaticamente" in text
    assert "grafana" in text and "loki" in text and "tempo" in text
    for signal in ("taxa", "erros", "duração", "eventos", "retries", "dlq"):
        assert signal in text


def test_ac_021_runbook_documents_startup_access_investigation_and_limits():
    """@spec:AC-021 Execução e investigação estão documentadas."""
    text = RUNBOOK.read_text(encoding="utf-8").lower()
    assert "docker compose up --build" in text
    assert "http://localhost:3000" in text
    assert "admin" in text
    assert "correlation_id" in text
    assert "retenção" in text and "autenticação" in text
    assert "dimensionamento" in text and "produção" in text
    assert "docs/observability.md" in README.read_text(encoding="utf-8").lower()
