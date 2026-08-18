"""Provas dos endpoints operacionais."""

from pathlib import Path
import sys

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parents[1]))

from order_service.main import create_app
from order_service.persistence import OrderStore
from order_service.producer import InMemoryProducer, KafkaProducer


def test_health_is_live_but_ready_requires_kafka_spec_ac_012():
    """@spec:AC-012 Saúde e prontidão distinguem processo vivo de Kafka disponível."""
    client = TestClient(create_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(available=False)))

    assert client.get("/health").json() == {"status": "live"}
    assert client.get("/ready").status_code == 503

    ready_client = TestClient(create_app(OrderStore("sqlite+pysqlite:///:memory:"), InMemoryProducer(available=True)))
    assert ready_client.get("/ready").status_code == 200


def test_lifespan_starts_real_kafka_adapter_and_readiness_uses_its_connection():
    class KafkaDouble:
        def __init__(self, **_: object) -> None:
            self.started = False
            self.stopped = False

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.stopped = True

        async def send_and_wait(self, *_: object, **__: object) -> None:
            return None

    doubles: list[KafkaDouble] = []

    def factory(**kwargs: object) -> KafkaDouble:
        double = KafkaDouble(**kwargs)
        doubles.append(double)
        return double

    producer = KafkaProducer("kafka:9092", producer_factory=factory)
    with TestClient(create_app(OrderStore("sqlite+pysqlite:///:memory:"), producer)) as client:
        assert doubles[0].started
        assert client.get("/health").json() == {"status": "live"}
        assert client.get("/ready").status_code == 200
    assert doubles[0].stopped
