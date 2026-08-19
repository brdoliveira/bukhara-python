"""Provas executáveis da documentação de arquitetura."""

from pathlib import Path


ROOT = Path(__file__).parents[2]
README = (ROOT / "README.md").read_text(encoding="utf-8")


def test_readme_exibe_componentes_da_arquitetura_hld__spec_AC_022() -> None:
    """@spec:AC-022 O HLD relaciona os componentes principais da plataforma."""
    assert "## Arquitetura HLD" in README
    hld = README.split("## Arquitetura HLD", 1)[1].split("## Arquitetura LLD", 1)[0]
    assert "```mermaid" in hld
    for component in (
        "order-service",
        "inventory-service",
        "payment-service",
        "notification-service",
        "Kafka",
        "PostgreSQL",
        "LGTM",
    ):
        assert component in hld


def test_readme_detalha_saga_e_resiliencia_no_lld__spec_AC_023() -> None:
    """@spec:AC-023 O LLD mostra eventos, persistência e caminhos de falha."""
    assert "## Arquitetura LLD" in README
    lld = README.split("## Arquitetura LLD", 1)[1].split("## Fluxo de eventos", 1)[0]
    assert "sequenceDiagram" in lld
    for contract in (
        "Idempotency-Key",
        "Inbox",
        "Outbox",
        "order.created",
        "inventory.reserved",
        "payment.approved",
        "inventory.release.requested",
        "retry.1",
        "retry.2",
        "retry.3",
        "fallback",
        "DLQ",
    ):
        assert contract in lld
