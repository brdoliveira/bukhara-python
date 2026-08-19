"""Contratos compartilhados para eventos e resiliência da saga de pedidos."""

from .envelope import EventEnvelope, EnvelopeValidationError
from .health import HealthEndpoints, ReadinessReport
from .inbox import InMemoryInbox, InboxRepository
from .outbox import InMemoryOutbox, OutboxRepository, OutboxRecord
from .retry import MAX_RETRY_ATTEMPTS, DeadLetter, FallbackRegistry, RetryCoordinator, RetryPolicy, TransientDependencyError

__all__ = ["DeadLetter", "EnvelopeValidationError", "EventEnvelope", "FallbackRegistry", "HealthEndpoints", "InMemoryInbox", "InMemoryOutbox", "InboxRepository", "MAX_RETRY_ATTEMPTS", "OutboxRecord", "OutboxRepository", "ReadinessReport", "RetryCoordinator", "RetryPolicy", "TransientDependencyError"]
