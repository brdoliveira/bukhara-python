"""Provas herméticas do worker Kafka do serviço de notificações."""

from __future__ import annotations

import asyncio

import pytest

from services.notification_service.notification_service.adapter import NotificationAdapter, TransientDependencyError
from services.notification_service.notification_service.consumer import KafkaNotificationWorker
from services.notification_service.notification_service.handler import NotificationHandler
from services.notification_service.notification_service.persistence import NotificationRepository


def payment_event(**overrides):
    """Cria um evento de pagamento aprovado compatível com o worker."""
    event = {
        "event_id": "evt-1",
        "event_type": "payment.approved",
        "order_id": "order-1",
        "correlation_id": "corr-1",
        "payload": {"amount": 42},
    }
    event.update(overrides)
    return event


class MessageDouble:
    """Representa a parte da mensagem Kafka que o worker consulta."""

    def __init__(self, value):
        self.value = value
        self.headers = []


class ConsumerDouble:
    """Iterador assíncrono controlado com contagem de commits e encerramentos."""

    def __init__(self, messages=()):
        self.messages = list(messages)
        self.commits = 0
        self.stops = 0

    def __aiter__(self):
        return self._messages()

    async def _messages(self):
        for message in self.messages:
            yield message

    async def commit(self):
        self.commits += 1

    async def stop(self):
        self.stops += 1


class ProducerDouble:
    """Captura publicações e encerramentos sem precisar de um broker real."""

    def __init__(self):
        self.messages = []
        self.stops = 0

    async def send_and_wait(self, topic, event, *, key, headers=None):
        self.messages.append((topic, event, key, headers))

    async def stop(self):
        self.stops += 1


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-030"], ids=str)
def test_ac_030_worker_normaliza_retries_e_publica_notificacao_sem_kafka_local__spec_AC_030(_spec_tag, monkeypatch):
    async def no_delay(_):
        return None

    monkeypatch.setattr(
        "services.notification_service.notification_service.consumer.asyncio.sleep", no_delay,
    )
    repository = NotificationRepository()
    adapter = NotificationAdapter(send_failures=[TransientDependencyError()])
    worker = KafkaNotificationWorker("kafka:9092", NotificationHandler(adapter, repository), repository)
    worker.consumer = ConsumerDouble([MessageDouble(payment_event())])
    worker.producer = ProducerDouble()

    asyncio.run(worker._run())

    assert worker.consumer.commits == 1
    assert worker.producer.messages == [
        ("notification.retry.1", {
            "event_id": "evt-1", "event_type": "payment.approved", "order_id": "order-1",
            "correlation_id": "corr-1", "payload": {"amount": 42}, "type": "payment.approved",
            "retry_attempt": 1,
        }, b"order-1", None),
    ]


@pytest.mark.parametrize("_spec_tag", ["@spec:AC-030"], ids=str)
def test_ac_030_worker_stop_e_idempotente_e_fecha_dependencias_uma_vez__spec_AC_030(_spec_tag):
    async def scenario():
        repository = NotificationRepository()
        worker = KafkaNotificationWorker(
            "kafka:9092", NotificationHandler(NotificationAdapter(), repository), repository,
        )
        consumer = ConsumerDouble()
        producer = ProducerDouble()
        worker.consumer = consumer
        worker.producer = producer
        worker.ready = True
        worker.task = asyncio.create_task(asyncio.Event().wait())
        await asyncio.sleep(0)
        await worker.stop()
        await worker.stop()
        return consumer, producer, worker.ready, worker.task

    consumer, producer, ready, task = asyncio.run(scenario())

    assert consumer.stops == 1
    assert producer.stops == 1
    assert ready is False
    assert task is None
