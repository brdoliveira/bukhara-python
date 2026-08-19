import asyncio
import json

import pytest

from services.payment_service.payment_service.main import DependencyProbe, create_app


def test_apis_publicas_do_pagamento_possuem_docstrings__spec_AC_024():
    """@spec:AC-024 Módulos, classes e funções públicas explicam sua responsabilidade."""
    import ast
    from pathlib import Path

    source_dir = Path(__file__).parents[1] / "payment_service"
    modules = ("consumer.py", "handler.py", "main.py", "outbox.py", "persistence.py")
    undocumented: list[str] = []
    for module_name in modules:
        tree = ast.parse((source_dir / module_name).read_text(encoding="utf-8"))
        if ast.get_docstring(tree) is None:
            undocumented.append(module_name)
        for node in tree.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_"):
                if ast.get_docstring(node) is None:
                    undocumented.append(f"{module_name}:{node.name}")

    assert undocumented == []


async def request(app, path):
    messages = []

    async def receive():
        return {"type": "http.request"}

    async def send(message):
        messages.append(message)

    await app(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": b"",
            "headers": [],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "scheme": "http",
        },
        receive,
        send,
    )
    return messages[0]["status"], json.loads(messages[1]["body"])


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-012"], ids=str)
def test_ac_012_health_indica_processo_vivo_mesmo_sem_kafka__spec_AC_012(_spec_tag):
    app = create_app(kafka=DependencyProbe(False), postgres=DependencyProbe(True))

    status, body = asyncio.run(request(app, "/health"))

    assert status == 200
    assert body == {"status": "alive"}


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-012"], ids=str)
def test_ac_012_ready_so_tem_sucesso_com_kafka_e_postgres_disponiveis__spec_AC_012(_spec_tag):
    unavailable = create_app(kafka=DependencyProbe(False), postgres=DependencyProbe(True))
    ready = create_app(kafka=DependencyProbe(True), postgres=DependencyProbe(True))

    unavailable_status, unavailable_body = asyncio.run(request(unavailable, "/ready"))
    ready_status, ready_body = asyncio.run(request(ready, "/ready"))

    assert unavailable_status == 503
    assert unavailable_body["kafka"] == "unavailable"
    assert ready_status == 200
    assert ready_body == {"status": "ready", "kafka": "available", "postgres": "available"}
