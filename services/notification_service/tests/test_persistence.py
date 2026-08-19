from services.notification_service.notification_service.persistence import NotificationRepository, PostgresNotificationRepository


def test_persistencia_mantem_inbox_e_fallback_terminal():
    repository = NotificationRepository()

    repository.mark_processed("evt-1")
    assert repository.mark_terminal_failure("evt-2")
    assert not repository.mark_terminal_failure("evt-2")
    repository.record_fallback("evt-2")

    assert repository.was_processed("evt-1")
    assert repository.has_terminal_failure("evt-2")
    assert repository.fallback_count("evt-2") == 1


def test_repositorio_sql_persiste_inbox_e_fallback():
    repository = PostgresNotificationRepository("sqlite+pysqlite:///:memory:")
    repository.mark_processed("evt-sql")
    assert repository.mark_terminal_failure("evt-fail")
    assert not repository.mark_terminal_failure("evt-fail")
    repository.record_fallback("evt-fail")

    assert repository.was_processed("evt-sql")
    assert repository.has_terminal_failure("evt-fail")
    assert repository.fallback_count("evt-fail") == 1
