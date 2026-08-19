from services.payment_service.payment_service.persistence import PaymentRepository


def test_persistencia_mantem_inbox_e_fallback_terminal():
    repository = PaymentRepository()

    repository.mark_processed("evt-1")
    assert repository.mark_terminal_failure("evt-2")
    repository.record_fallback("evt-2")

    assert repository.was_processed("evt-1")
    assert repository.has_terminal_failure("evt-2")
    assert repository.fallback_count("evt-2") == 1


def test_repositorio_preserva_deduplicacao_e_falha_terminal__spec_AC_029():
    """@spec:AC-029 Inbox não perde a decisão terminal nem repete compensação."""
    repository = PaymentRepository()

    repository.mark_processed("approved")
    assert repository.was_processed("approved")
    assert repository.mark_terminal_failure("unavailable") is True
    assert repository.mark_terminal_failure("unavailable") is False
    repository.record_fallback("unavailable")

    assert repository.has_terminal_failure("unavailable")
    assert repository.fallback_count("unavailable") == 1

