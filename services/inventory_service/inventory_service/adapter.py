"""Adaptadores determinísticos para estoque e mensageria.

As interfaces são pequenas de propósito: o adaptador Kafka/PostgreSQL real pode
substituí-las sem alterar o consumidor ou as regras de negócio.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Iterable

from .persistence import InventoryRepository


class TransientDependencyError(RuntimeError):
    """Indica que a mensagem pode ser processada novamente com segurança."""


@dataclass
class InMemoryBroker:
    """Porta de mensageria observável usada pelo serviço e pelos testes."""

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


class InventoryAdapter:
    """Encapsula a dependência de estoque e permite falhas transitórias simuladas."""

    def __init__(
        self,
        repository: InventoryRepository,
        *,
        reserve_failures: Iterable[Exception] = (),
        release_failures: Iterable[Exception] = (),
    ) -> None:
        self.repository = repository
        self._reserve_failures = list(reserve_failures)
        self._release_failures = list(release_failures)

    def reserve(self, order_id: str, items: list[dict[str, Any]]) -> None:
        if self._reserve_failures:
            raise self._reserve_failures.pop(0)
        self.repository.reserve(order_id, items)

    def release(self, order_id: str) -> None:
        if self._release_failures:
            raise self._release_failures.pop(0)
        self.repository.release(order_id)
