"""Provas da documentação pública e do guia de referência."""

import ast
from pathlib import Path


ROOT = Path(__file__).parents[2]


def test_apis_publicas_possuem_docstrings_verificaveis__spec_AC_024() -> None:
    """@spec:AC-024 Cada módulo e símbolo público de primeiro nível é documentado."""
    missing: list[str] = []
    for path in sorted((ROOT / "services").rglob("*.py")) + sorted((ROOT / "packages").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if ast.get_docstring(tree) is None:
            missing.append(f"{path}: módulo")
        for node in tree.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_"):
                if ast.get_docstring(node) is None:
                    missing.append(f"{path}: {node.name}")
    assert not missing, "APIs públicas sem docstring: " + ", ".join(missing)


def test_guia_de_referencia_orienta_navegacao__spec_AC_025() -> None:
    """@spec:AC-025 O guia lista pacotes, serviços, componentes e testes."""
    guide = (ROOT / "docs/code-reference.md").read_text(encoding="utf-8")
    for term in ("event_bus", "observability", "order_service", "inventory_service", "payment_service", "notification_service", "persistence.py", "outbox.py", "pytest"):
        assert term in guide
