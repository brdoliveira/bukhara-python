import pytest
from fastapi.testclient import TestClient

from services.notification_service.notification_service.main import DependencyProbe, create_app


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-012"], ids=str)
def test_ac_012_health_indica_processo_vivo_sem_kafka__spec_AC_012(_spec_tag):
    with TestClient(create_app(kafka=DependencyProbe(False))) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-012"], ids=str)
def test_ac_012_ready_exige_kafka_e_postgres_disponiveis__spec_AC_012(_spec_tag):
    with TestClient(create_app(kafka=DependencyProbe(False), postgres=DependencyProbe(True))) as client:
        unavailable = client.get("/ready")
    with TestClient(create_app(kafka=DependencyProbe(True), postgres=DependencyProbe(True))) as client:
        ready = client.get("/ready")

    assert unavailable.status_code == 503
    assert unavailable.json()["kafka"] == "unavailable"
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready", "kafka": "available", "postgres": "available"}
