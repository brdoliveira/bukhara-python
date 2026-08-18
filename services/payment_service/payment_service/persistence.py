"""Inbox, efeitos de cobrança e marcações de falha do payment-service.

Em produção, estas operações representam a mesma transação PostgreSQL que
grava a Inbox e a Outbox. A versão em memória preserva a semântica idempotente
para o MVP e para os testes unitários.
"""

from __future__ import annotations

from collections import Counter
import json
from typing import Any
from uuid import uuid4


class PaymentRepository:
    def __init__(self) -> None:
        self._processed: set[str] = set()
        self._terminal_failures: set[str] = set()
        self._fallbacks: Counter[str] = Counter()

    def was_processed(self, event_id: str) -> bool:
        return event_id in self._processed

    def mark_processed(self, event_id: str) -> None:
        self._processed.add(event_id)

    def mark_terminal_failure(self, event_id: str) -> bool:
        if event_id in self._terminal_failures:
            return False
        self._terminal_failures.add(event_id)
        return True

    def has_terminal_failure(self, event_id: str) -> bool:
        return event_id in self._terminal_failures

    def record_fallback(self, event_id: str) -> None:
        self._fallbacks[event_id] += 1

    def fallback_count(self, event_id: str) -> int:
        return self._fallbacks[event_id]


class PostgresPaymentRepository:
    """Inbox e efeitos locais persistidos no banco pertencente ao serviço."""

    def __init__(self, database_url: str) -> None:
        from sqlalchemy import create_engine

        self.engine = create_engine(database_url, pool_pre_ping=True)

    def initialize(self) -> None:
        from sqlalchemy import text

        statements = (
            "CREATE TABLE IF NOT EXISTS payment_inbox (event_id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
            "CREATE TABLE IF NOT EXISTS payment_effects (event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, outcome TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
            "CREATE TABLE IF NOT EXISTS payment_outbox (id BIGSERIAL PRIMARY KEY, event_id TEXT UNIQUE NOT NULL, topic TEXT NOT NULL, payload JSONB NOT NULL, available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), published_at TIMESTAMPTZ)",
        )
        with self.engine.begin() as connection:
            for statement in statements:
                connection.execute(text(statement))

    def is_available(self) -> bool:
        from sqlalchemy import text

        with self.engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True

    def is_terminal(self, event_id: str) -> bool:
        from sqlalchemy import text

        with self.engine.connect() as connection:
            return connection.execute(
                text("SELECT 1 FROM payment_inbox WHERE event_id = :event_id AND status IN ('processed', 'terminal')"),
                {"event_id": event_id},
            ).first() is not None

    def persist_outcome(self, event: dict[str, Any], outcome: str, events: list[tuple[str, dict[str, Any]]], *, terminal: bool = False) -> bool:
        """Grava Inbox, efeito e Outbox na mesma transação PostgreSQL."""
        from sqlalchemy import text

        status = "terminal" if terminal else "processed"
        with self.engine.begin() as connection:
            existing = connection.execute(
                text("SELECT status FROM payment_inbox WHERE event_id = :event_id FOR UPDATE"), {"event_id": event["event_id"]}
            ).first()
            if existing is not None:
                return False
            connection.execute(
                text("INSERT INTO payment_inbox (event_id, status) VALUES (:event_id, :status)"),
                {"event_id": event["event_id"], "status": status},
            )
            connection.execute(
                text("INSERT INTO payment_effects (event_id, order_id, outcome) VALUES (:event_id, :order_id, :outcome)"),
                {"event_id": event["event_id"], "order_id": event["order_id"], "outcome": outcome},
            )
            for topic, outgoing in events:
                connection.execute(
                    text(
                        "INSERT INTO payment_outbox (event_id, topic, payload) "
                        "VALUES (:event_id, :topic, CAST(:payload AS jsonb)) ON CONFLICT (event_id) DO NOTHING"
                    ),
                    {"event_id": str(uuid4()), "topic": topic, "payload": json.dumps(outgoing)},
                )
        return True
