"""Pontos operacionais do payment-service."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass
class DependencyProbe:
    available: bool = True

    def is_available(self) -> bool:
        return self.available


class PaymentApplication:
    def __init__(self, kafka: DependencyProbe, postgres: DependencyProbe) -> None:
        self.kafka = kafka
        self.postgres = postgres

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope.get("type") != "http":
            return
        if scope.get("path") == "/health":
            status, body = 200, {"status": "alive"}
        elif scope.get("path") == "/ready":
            kafka = "available" if self.kafka.is_available() else "unavailable"
            postgres = "available" if self.postgres.is_available() else "unavailable"
            status = 200 if kafka == postgres == "available" else 503
            body = {"status": "ready" if status == 200 else "not_ready", "kafka": kafka, "postgres": postgres}
        else:
            status, body = 404, {"detail": "not found"}
        await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json")]})
        await send({"type": "http.response.body", "body": json.dumps(body).encode("utf-8")})


def create_app(kafka: DependencyProbe | None = None, postgres: DependencyProbe | None = None) -> PaymentApplication:
    return PaymentApplication(kafka or DependencyProbe(), postgres or DependencyProbe())


app = create_app()

