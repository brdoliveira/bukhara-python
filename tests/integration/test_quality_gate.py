"""Prova de que a meta de cobertura está ativa na configuração do projeto."""

from pathlib import Path


ROOT = Path(__file__).parents[2]


def test_gate_de_cobertura_minima_esta_ativo__spec_AC_031() -> None:
    """@spec:AC-031 O pytest falha abaixo da meta global confirmada de 85%."""
    config = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert '"pytest-cov>=5,<7"' in config
    assert 'addopts = "-ra --import-mode=importlib --cov ' in config
    assert "--cov-fail-under=85" in config
    assert "[tool.coverage.run]" in config
    for source in (
        "packages/event_bus/event_bus",
        "packages/observability/observability",
        "services/order_service/order_service",
        "services/inventory_service/inventory_service",
        "services/payment_service/payment_service",
        "services/notification_service/notification_service",
    ):
        assert source in config
    assert 'omit = ["*/tests/*", "*/migrations/*"]' in config
    assert "[tool.coverage.report]" in config
    assert "fail_under = 85" in config
