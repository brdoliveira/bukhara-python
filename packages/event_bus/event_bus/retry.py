"""Política limitada de retry, fallback idempotente e registros de DLQ."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

from .envelope import EnvelopeValidationError, EventEnvelope

MAX_RETRY_ATTEMPTS = 3


class FailureKind(str, Enum):
    """Categories that decide retry, fallback, or immediate DLQ handling."""
    TRANSIENT = "transient"
    VALIDATION = "validation"
    BUSINESS = "business"
    UNKNOWN = "unknown"


class TransientDependencyError(RuntimeError):
    """Erro temporário que pode entrar na fila de retry."""


class BusinessRuleError(RuntimeError):
    """Resultado de negócio que deve ser tratado sem retry."""


def classify_failure(error: Exception) -> FailureKind:
    """Classify known failures and safely group any unknown exception."""
    if isinstance(error, TransientDependencyError):
        return FailureKind.TRANSIENT
    if isinstance(error, EnvelopeValidationError):
        return FailureKind.VALIDATION
    if isinstance(error, BusinessRuleError):
        return FailureKind.BUSINESS
    return FailureKind.UNKNOWN


@dataclass(frozen=True)
class RetryPolicy:
    """Fixed exponential retry schedule shared by event consumers."""
    max_attempts: int = MAX_RETRY_ATTEMPTS
    base_delay_seconds: float = 1.0
    multiplier: float = 5.0

    def __post_init__(self) -> None:
        if self.max_attempts != MAX_RETRY_ATTEMPTS:
            raise ValueError(f"max_attempts must be {MAX_RETRY_ATTEMPTS}")
        if self.base_delay_seconds <= 0 or self.multiplier <= 1:
            raise ValueError("retry delays must be positive and increasing")

    def delay_for(self, attempt: int) -> float:
        """Return the delay for a one-based attempt within the configured limit."""
        if not 1 <= attempt <= self.max_attempts:
            raise ValueError("attempt is outside the retry policy")
        return self.base_delay_seconds * (self.multiplier ** (attempt - 1))


@dataclass(frozen=True)
class DeadLetter:
    """Failure record emitted when an event reaches a terminal state."""
    event: EventEnvelope
    error_type: str
    origin_service: str
    attempts: int
    validation_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class RetryOutcome:
    """Observable decision made by :class:`RetryCoordinator` for one delivery."""
    action: str
    attempt: int
    delay_seconds: Optional[float] = None


@dataclass
class FallbackRegistry:
    """Garante fallback e DLQ uma vez por evento, mesmo com redelivery."""

    _finalized_event_ids: set[str] = field(default_factory=set)

    def claim(self, event_id: str) -> bool:
        """Claim finalization once, preventing duplicate fallback and DLQ effects."""
        if event_id in self._finalized_event_ids:
            return False
        self._finalized_event_ids.add(event_id)
        return True


class RetryCoordinator:
    """Apply retry policy and run terminal effects once per event."""

    def __init__(self, policy: Optional[RetryPolicy] = None, registry: Optional[FallbackRegistry] = None) -> None:
        """Use injected policy or registry when a consumer needs shared state."""
        self.policy = policy or RetryPolicy()
        self.registry = registry or FallbackRegistry()

    def handle_transient(self, event: EventEnvelope, error: Exception, *, origin_service: str, schedule_retry: Callable[[EventEnvelope, float], None], fallback: Callable[[EventEnvelope], None], send_to_dlq: Callable[[DeadLetter], None]) -> RetryOutcome:
        """Schedule a bounded retry or run terminal fallback and DLQ handling."""
        current_attempt = event.retry_attempt
        if current_attempt < self.policy.max_attempts:
            next_attempt = current_attempt + 1
            delay = self.policy.delay_for(next_attempt)
            schedule_retry(event.for_retry(next_attempt), delay)
            return RetryOutcome("retry", next_attempt, delay)
        if self.registry.claim(event.event_id):
            fallback(event)
            send_to_dlq(DeadLetter(event=event, error_type=type(error).__name__, origin_service=origin_service, attempts=current_attempt))
        return RetryOutcome("dlq", current_attempt)

    def send_invalid_to_dlq(self, event: EventEnvelope, error: EnvelopeValidationError, *, origin_service: str, send_to_dlq: Callable[[DeadLetter], None]) -> None:
        """Route an invalid envelope directly to the DLQ, without retrying it."""
        if self.registry.claim(event.event_id):
            send_to_dlq(DeadLetter(event=event, error_type=type(error).__name__, origin_service=origin_service, attempts=event.retry_attempt, validation_errors=tuple(error.errors)))
