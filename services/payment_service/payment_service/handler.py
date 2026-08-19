"""Regras de negócio do pagamento e compensação da saga."""

from __future__ import annotations

from typing import Any

from .adapter import PaymentAdapter, PaymentDeclinedError
from .outbox import InMemoryOutbox
from .persistence import PaymentRepository


class PaymentHandler:
    def __init__(self, adapter: PaymentAdapter, outbox: InMemoryOutbox, repository: PaymentRepository) -> None:
        self.adapter = adapter
        self.outbox = outbox
        self.repository = repository

    def handle(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        if event["type"] != "inventory.reserved":
            raise ValueError(f"unsupported event type: {event['type']}")
        try:
            self.adapter.charge(order_id=event["order_id"], payload=event["payload"])
        except PaymentDeclinedError:
            return self._failure_events(event, reason="payment_declined")
        return [
            self.outbox.add(
                "payment.approved",
                order_id=event["order_id"],
                correlation_id=event["correlation_id"],
                payload={},
                causation_id=event["event_id"],
            )
        ]

    def fallback(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """Falha técnica terminal não vira sucesso: solicita compensação."""
        self.repository.record_fallback(event["event_id"])
        return self._failure_events(event, reason="payment_unavailable")

    def _failure_events(self, event: dict[str, Any], *, reason: str) -> list[dict[str, Any]]:
        common = {
            "order_id": event["order_id"],
            "correlation_id": event["correlation_id"],
            "causation_id": event["event_id"],
        }
        return [
            self.outbox.add("payment.failed", payload={"reason": reason}, **common),
            self.outbox.add("inventory.release.requested", payload={"reason": reason}, **common),
        ]
