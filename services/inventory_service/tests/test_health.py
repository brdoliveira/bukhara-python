import asyncio
import json

import pytest

from services.inventory_service.inventory_service.main import DependencyProbe, create_app


async def request(app, path):
    messages = []

    async def receive():
        return {"type": "http.request"}

    async def send(message):
        messages.append(message)

    await app({"type": "http", "path": path}, receive, send)
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
