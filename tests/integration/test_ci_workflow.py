"""Contrato executável do workflow de integração contínua."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def _workflow_text() -> str:
    """Carrega o workflow e produz uma falha clara quando ele não existe."""
    assert WORKFLOW.is_file(), "o workflow .github/workflows/ci.yml deve existir"
    return WORKFLOW.read_text(encoding="utf-8")


def test_push_e_pull_request_para_main_disparam_o_ci__spec_AC_032() -> None:
    """@spec:AC-032 Push e pull request para main disparam o workflow."""
    workflow = _workflow_text()

    assert "on:\n" in workflow
    assert "  push:\n    branches: [main]\n" in workflow
    assert "  pull_request:\n    branches: [main]\n" in workflow
    assert "  workflow_dispatch:\n" in workflow


def test_ci_instala_python_e_aplica_testes_com_gate_de_cobertura__spec_AC_033() -> None:
    """@spec:AC-033 O CI executa a suíte que bloqueia cobertura abaixo de 85%."""
    workflow = _workflow_text()

    assert "uses: actions/checkout@v7" in workflow
    assert "uses: actions/setup-python@v7" in workflow
    assert "python-version: \"3.12\"" in workflow
    assert "cache: pip" in workflow
    assert 'python -m pip install -e ".[dev]"' in workflow
    config = (ROOT / "onpspec.config.json").read_text(encoding="utf-8")
    assert '"testCommand": "python -B -m pytest -q -p no:cacheprovider"' in config
    assert "--cov-fail-under=85" in (ROOT / "pyproject.toml").read_text(encoding="utf-8")


def test_ci_verifica_feature_e_bloqueia_divergencia_da_spec__spec_AC_034() -> None:
    """@spec:AC-034 Verify e audit em modo CI encerram o job quando falham."""
    workflow = _workflow_text()

    assert "permissions:\n  contents: read\n" in workflow
    assert "uses: actions/setup-node@v7" in workflow
    assert "node-version: \"24\"" in workflow
    for feature in (
        "fluxo-pedidos",
        "observabilidade",
        "documentacao-arquitetura",
        "qualidade-codigo",
        "ci-github-actions",
    ):
        assert feature in workflow
    assert 'npx --yes @onovoprogramador/onp-spec@0.9.0 verify "$feature"' in workflow
    assert "npx --yes @onovoprogramador/onp-spec@0.9.0 audit --ci" in workflow
