"""Inbox, efeitos de cobrança e marcações de falha do payment-service.

Em produção, estas operações representam a mesma transação PostgreSQL que
grava a Inbox e a Outbox. A versão em memória preserva a semântica idempotente
para o MVP e para os testes unitários.
"""

from __future__ import annotations

from collections import Counter


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

