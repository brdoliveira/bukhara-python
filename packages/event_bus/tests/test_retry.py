from __future__ import annotations

import pytest

from event_bus.envelope import EnvelopeValidationError, EventEnvelope
from event_bus.retry import (
    BusinessRuleError,
    DeadLetter,
    FailureKind,
    RetryCoordinator,
    RetryPolicy,
    TransientDependencyError,
    classify_failure,
)

EVENT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
CORRELATION_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def event(retry_attempt: int = 0) -> EventEnvelope:
    return EventEnvelope.new(
        event_id=EVENT_ID,
        event_type="inventory.reserved",
        producer="inventory-service",
        correlation_id=CORRELATION_ID,
        payload={"order_id": "order-1"},
    ).for_retry(retry_attempt)


def test_falha_transitoria_usa_retry_exponencial_limitado__spec_AC_008() -> None:
    """@spec:AC-008 Falha transitória usa retry exponencial limitado."""
    retries: list[tuple[EventEnvelope, float]] = []
    coordinator = RetryCoordinator()

    for attempt in range(3):
        outcome = coordinator.handle_transient(
            event(attempt),
            TransientDependencyError("kafka unavailable"),
            origin_service="payment-service",
            schedule_retry=lambda message, delay: retries.append((message, delay)),
            fallback=lambda _: None,
            send_to_dlq=lambda _: None,
        )
        assert outcome.action == "retry"

    assert [delay for _, delay in retries] == [1.0, 5.0, 25.0]
    assert [(message.event_id, message.correlation_id, message.retry_attempt) for message, _ in retries] == [
        (EVENT_ID, CORRELATION_ID, 1),
        (EVENT_ID, CORRELATION_ID, 2),
        (EVENT_ID, CORRELATION_ID, 3),
    ]


def test_retry_esgotado_executa_fallback_e_dlq_uma_vez__spec_AC_009() -> None:
    """@spec:AC-009 Retry esgotado usa fallback e DLQ."""
    fallback_calls: list[str] = []
    dlq: list[DeadLetter] = []
    coordinator = RetryCoordinator()

    for _ in range(2):
        result = coordinator.handle_transient(
            event(3),
            TransientDependencyError("database unavailable"),
            origin_service="inventory-service",
            schedule_retry=lambda *_: (_ for _ in ()).throw(AssertionError("retry must be exhausted")),
            fallback=lambda message: fallback_calls.append(message.event_id),
            send_to_dlq=dlq.append,
        )
        assert result.action == "dlq"

    assert fallback_calls == [EVENT_ID]
    assert [(entry.error_type, entry.origin_service, entry.attempts) for entry in dlq] == [
        ("TransientDependencyError", "inventory-service", 3)
    ]


def test_retry_classifica_erros_e_rejeita_politicas_invalidas__spec_AC_026() -> None:
    """@spec:AC-026 A política limita tentativas e separa falhas terminais."""
    assert classify_failure(TransientDependencyError("temporary")) is FailureKind.TRANSIENT
    assert classify_failure(EnvelopeValidationError(["bad payload"])) is FailureKind.VALIDATION
    assert classify_failure(BusinessRuleError("declined")) is FailureKind.BUSINESS
    assert classify_failure(RuntimeError("unknown")) is FailureKind.UNKNOWN

    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=2)
    with pytest.raises(ValueError, match="positive and increasing"):
        RetryPolicy(base_delay_seconds=0)
    with pytest.raises(ValueError, match="outside"):
        RetryPolicy().delay_for(4)


def test_envelope_invalido_vai_para_dlq_uma_vez_sem_retry__spec_AC_026() -> None:
    """@spec:AC-026 Validação é terminal e redelivery não duplica a DLQ."""
    coordinator = RetryCoordinator()
    dlq: list[DeadLetter] = []
    validation_error = EnvelopeValidationError(["payload must be an object"])

    coordinator.send_invalid_to_dlq(event(), validation_error, origin_service="inventory-service", send_to_dlq=dlq.append)
    coordinator.send_invalid_to_dlq(event(), validation_error, origin_service="inventory-service", send_to_dlq=dlq.append)

    assert [(entry.attempts, entry.validation_errors) for entry in dlq] == [(0, ("payload must be an object",))]
