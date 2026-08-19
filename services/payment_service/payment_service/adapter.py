"""Adaptadores determinísticos do pagamento e da mensageria."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Iterable


class TransientDependencyError(RuntimeError):
    """A adquirente falhou temporariamente e a cobrança pode ser repetida."""


class PaymentDeclinedError(ValueError):
    """A cobrança foi recusada definitivamente; não deve entrar em retry."""


@dataclass
class InMemoryBroker:
    """Porta Kafka observável, usada pelos testes unitários."""

    published: list[dict[str, Any]] = field(default_factory=list)
    retries: list[dict[str, Any]] = field(default_factory=list)
    dlq: list[dict[str, Any]] = field(default_factory=list)
    acknowledged: list[str] = field(default_factory=list)

    def publish(self, event: dict[str, Any]) -> None:
        self.published.append(deepcopy(event))

    def schedule_retry(self, event: dict[str, Any], delay_seconds: int) -> None:
        self.retries.append({"event": deepcopy(event), "delay_seconds": delay_seconds})

    def send_to_dlq(
        self,
        event: dict[str, Any],
        *,
        error_type: str,
        origin_service: str,
        attempts: int,
        validation_errors: list[str] | None = None,
    ) -> None:
        self.dlq.append(
            {
                "event": deepcopy(event),
                "error_type": error_type,
                "origin_service": origin_service,
                "attempts": attempts,
                "validation_errors": list(validation_errors or []),
            }
        )

    def acknowledge(self, event_id: str) -> None:
        self.acknowledged.append(event_id)


class PaymentAdapter:
    """Simula a adquirente e permite injetar erros determinísticos."""

    def __init__(self, *, charge_failures: Iterable[Exception] = ()) -> None:
        self._charge_failures = list(charge_failures)
        self.charges: list[dict[str, Any]] = []

    def charge(self, *, order_id: str, payload: dict[str, Any]) -> None:
        if self._charge_failures:
            raise self._charge_failures.pop(0)
        self.charges.append({"order_id": order_id, "payload": deepcopy(payload)})

