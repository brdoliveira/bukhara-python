import asyncio
import json

import pytest

from services.notification_service.notification_service.main import DependencyProbe, create_app


async def request(app, path):
    messages = []

    async def receive():
        return {"type": "http.request"}

    async def send(message):
        messages.append(message)

    await app({"type": "http", "path": path}, receive, send)
    return messages[0]["status"], json.loads(messages[1]["body"])


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-012"], ids=str)
def test_ac_012_health_indica_processo_vivo_sem_kafka__spec_AC_012(_spec_tag):
    status, body = asyncio.run(request(create_app(kafka=DependencyProbe(False)), "/health"))

    assert status == 200
    assert body == {"status": "alive"}


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-012"], ids=str)
def test_ac_012_ready_exige_kafka_e_postgres_disponiveis__spec_AC_012(_spec_tag):
    unavailable_status, unavailable_body = asyncio.run(
        request(create_app(kafka=DependencyProbe(False), postgres=DependencyProbe(True)), "/ready")
    )
    ready_status, ready_body = asyncio.run(
        request(create_app(kafka=DependencyProbe(True), postgres=DependencyProbe(True)), "/ready")
    )

    assert unavailable_status == 503
    assert unavailable_body["kafka"] == "unavailable"
    assert ready_status == 200
    assert ready_body == {"status": "ready", "kafka": "available", "postgres": "available"}
