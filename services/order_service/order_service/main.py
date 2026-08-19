"""Fábrica ASGI do order-service com ciclo de vida de Kafka e Outbox."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from fastapi import FastAPI
from observability.logging import configure_logging, get_logger
from observability.telemetry import Telemetry, TelemetrySettings, configure_telemetry, instrument_fastapi

from .api import register_routes
from .persistence import OrderStore
from .producer import EventProducer, KafkaProducer, OutboxPublisher


async def _publish_outbox_forever(publisher: OutboxPublisher) -> None:
    while True:
        try:
            await publisher.publish_pending_async()
        except ConnectionError:
            # A Outbox permanece pendente e será tentada novamente.
            pass
        await asyncio.sleep(1)


def create_app(
    store: Optional[OrderStore] = None,
    producer: Optional[EventProducer] = None,
    *,
    telemetry: Optional[Telemetry] = None,
) -> FastAPI:
    """Cria a aplicação ASGI com telemetria, rotas e recuperação da Outbox.

    Dependências podem ser injetadas para execução hermética; em runtime elas
    são resolvidas a partir das configurações de ambiente.
    """
    resolved_store = store or (OrderStore.from_environment() if os.getenv("DATABASE_URL") else OrderStore())
    resolved_telemetry = telemetry or configure_telemetry(TelemetrySettings(service_name="order-service"))
    configure_logging(service_name="order-service")
    resolved_producer = producer or KafkaProducer.from_environment(telemetry=resolved_telemetry)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        start = getattr(resolved_producer, "start", None)
        if start is not None:
            await start()
        publisher: OutboxPublisher = app.state.outbox_publisher
        try:
            await publisher.publish_pending_async()
        except ConnectionError:
            pass
        recovery_task = asyncio.create_task(_publish_outbox_forever(publisher))
        try:
            yield
        finally:
            recovery_task.cancel()
            try:
                await recovery_task
            except asyncio.CancelledError:
                pass
            stop = getattr(resolved_producer, "stop", None)
            if stop is not None:
                await stop()
            resolved_store.close()

    app = FastAPI(title="order-service", lifespan=lifespan)
    app.state.telemetry = resolved_telemetry
    app.state.logger = get_logger("order-service", service_name="order-service")
    app.state.outbox_publisher = register_routes(app, resolved_store, resolved_producer)
    instrument_fastapi(app, resolved_telemetry)
    return app


app = create_app()
