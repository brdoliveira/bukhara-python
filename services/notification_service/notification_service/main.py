"""Aplicação FastAPI e lifecycle do notification-service."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Callable

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from observability.logging import configure_logging
from observability.telemetry import Telemetry, TelemetrySettings, configure_telemetry, instrument_fastapi

from .adapter import LoggingNotificationAdapter
from .consumer import KafkaNotificationWorker
from .handler import NotificationHandler
from .persistence import NotificationRepository, PostgresNotificationRepository


@dataclass
class DependencyProbe:
    available: bool = True

    def is_available(self) -> bool:
        return self.available


class CallableProbe:
    def __init__(self, check: Callable[[], bool]) -> None:
        self.check = check

    def is_available(self) -> bool:
        try:
            return bool(self.check())
        except Exception:
            return False


def create_app(
    kafka: DependencyProbe | CallableProbe | None = None,
    postgres: DependencyProbe | CallableProbe | None = None,
    worker: KafkaNotificationWorker | None = None,
    *,
    telemetry: Telemetry | None = None,
) -> FastAPI:
    database_url = os.getenv("DATABASE_URL")
    repository = PostgresNotificationRepository(database_url) if database_url else NotificationRepository()
    resolved_telemetry = telemetry or configure_telemetry(TelemetrySettings(service_name="notification-service"))
    configure_logging(service_name="notification-service")
    runtime_worker = worker
    if runtime_worker is None and os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        runtime_worker = KafkaNotificationWorker(
            os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            NotificationHandler(LoggingNotificationAdapter(), repository),
            repository,
            resolved_telemetry,
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if runtime_worker:
            await runtime_worker.start()
        try:
            yield
        finally:
            if runtime_worker:
                await runtime_worker.stop()

    app = FastAPI(title="notification-service", lifespan=lifespan)
    app.state.telemetry = resolved_telemetry
    instrument_fastapi(app, resolved_telemetry)
    kafka_probe = kafka or CallableProbe(lambda: bool(runtime_worker and runtime_worker.ready))
    postgres_probe = postgres or CallableProbe(repository.is_available)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/ready")
    async def ready():
        kafka_state = "available" if kafka_probe.is_available() else "unavailable"
        postgres_state = "available" if postgres_probe.is_available() else "unavailable"
        body = {
            "status": "ready" if kafka_state == postgres_state == "available" else "not_ready",
            "kafka": kafka_state,
            "postgres": postgres_state,
        }
        return JSONResponse(body, status_code=200 if body["status"] == "ready" else 503)

    return app


app = create_app()
