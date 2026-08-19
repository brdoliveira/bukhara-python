import ast
from pathlib import Path

import pytest

from services.notification_service.notification_service.adapter import NotificationAdapter
from services.notification_service.notification_service.handler import NotificationHandler
from services.notification_service.notification_service.persistence import NotificationRepository


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-006"], ids=str)
def test_ac_006_pagamento_aprovado_processa_notificacao_de_pedido_concluido__spec_AC_006(_spec_tag):
    adapter = NotificationAdapter()
    handler = NotificationHandler(adapter, NotificationRepository())

    handler.handle({
        "event_id": "evt-payment", "type": "payment.approved", "order_id": "order-1",
        "correlation_id": "corr-1", "payload": {"amount": 42},
    })

    assert adapter.sent_notifications == [{
        "order_id": "order-1", "correlation_id": "corr-1", "kind": "order_completed",
    }]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-024"], ids=str)
def test_ac_024_apis_publicas_do_servico_de_notificacao_possuem_docstrings__spec_AC_024(_spec_tag):
    """Mantém verificável a documentação dos módulos e APIs públicas desta tarefa."""
    package = Path(__file__).parents[1] / "notification_service"
    documented_modules = ("adapter.py", "consumer.py", "handler.py", "main.py", "persistence.py")
    missing = []

    for name in documented_modules:
        path = package / name
        module = ast.parse(path.read_text(encoding="utf-8"))
        if not ast.get_docstring(module):
            missing.append(f"{path.name} (module)")
        for node in module.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_"):
                if not ast.get_docstring(node):
                    missing.append(f"{path.name}:{node.name}")

    assert missing == []
