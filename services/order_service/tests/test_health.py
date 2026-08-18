"""Provas dos endpoints operacionais."""

from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parents[1]))

from order_service.main import create_app
from order_service.persistence import OrderStore
from order_service.producer import InMemoryProducer


def test_health_is_live_but_ready_requires_kafka_spec_ac_012():
    """@spec:AC-012 Saúde e prontidão distinguem processo vivo de Kafka disponível."""
    client = TestClient(create_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(available=False)))

    assert client.get("/health").json() == {"status": "live"}
    assert client.get("/ready").status_code == 503

    ready_client = TestClient(create_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(available=True)))
    assert ready_client.get("/ready").status_code == 200
