from __future__ import annotations

from event_bus.envelope import EventEnvelope
from event_bus.retry import DeadLetter, RetryCoordinator, TransientDependencyError

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
