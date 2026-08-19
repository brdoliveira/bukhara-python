from services.payment_service.payment_service.persistence import PaymentRepository


def test_persistencia_mantem_inbox_e_fallback_terminal():
    repository = PaymentRepository()

    repository.mark_processed("evt-1")
    assert repository.mark_terminal_failure("evt-2")
    repository.record_fallback("evt-2")

    assert repository.was_processed("evt-1")
    assert repository.has_terminal_failure("evt-2")
    assert repository.fallback_count("evt-2") == 1

