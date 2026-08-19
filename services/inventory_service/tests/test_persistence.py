from __future__ import annotations

import pytest

from services.inventory_service.inventory_service.persistence import InventoryRepository, PostgresInventoryRepository


def test_persistencia_mantem_reserva_e_inbox_processada():
    repository = InventoryRepository({"tea": 2})

    repository.reserve("order-1", [{"sku": "tea", "quantity": 1}])
    repository.mark_processed("evt-1")

    assert repository.available("tea") == 1
    assert repository.was_processed("evt-1")


def test_repositorio_em_memoria_preserva_deduplicacao_fallback_e_outbox__spec_AC_028():
    """@spec:AC-028 Inbox e fallback são idempotentes mesmo sem dependência externa."""
    repository = InventoryRepository({"tea": 2})

    repository.mark_processed("processed")
    assert repository.was_processed("processed")
    assert repository.mark_terminal_failure("terminal") is True
    assert repository.mark_terminal_failure("terminal") is False
    repository.record_fallback("terminal")
    repository.record_fallback("terminal")

    assert repository.has_terminal_failure("terminal")
    assert repository.fallback_count("terminal") == 2


class _Cursor:
    """Cursor que grava SQL para testar o adaptador PostgreSQL sem servidor."""

    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection
        self.row: object | None = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, params: object = ()) -> None:
        self.connection.statements.append((sql, params))

    def fetchone(self) -> object | None:
        return self.row

    def fetchall(self) -> list[object]:
        return []

    def close(self) -> None:
        self.connection.closed_cursors += 1


class _Connection:
    """Conexão transacional determinística para o repositório PostgreSQL."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.closed_cursors = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def test_repositorio_postgres_inicializa_e_respeita_limites_transacionais_com_double__spec_AC_028():
    """@spec:AC-028 SQL de Inbox/Outbox permanece transacional sem precisar de PostgreSQL real."""
    repository = PostgresInventoryRepository("postgresql+psycopg://inventory")
    connection = _Connection()
    repository._connection = connection

    repository.initialize()
    repository.seed_stock({"tea": 3})
    with repository.transaction():
        repository.add_outbox(
            {"event_id": "outbox-1", "order_id": "order-1", "payload": {}},
            topic="inventory.events",
        )

    assert repository.database_url == "postgresql://inventory"
    assert connection.commits == 3
    assert any("inventory_inbox" in sql and "inventory_outbox" in sql for sql, _ in connection.statements)
    assert any("INSERT INTO inventory_outbox" in sql for sql, _ in connection.statements)

    with pytest.raises(RuntimeError):
        with repository.transaction():
            raise RuntimeError("rollback")
    assert connection.rollbacks == 1
    repository.close()
    assert connection.closed is True
