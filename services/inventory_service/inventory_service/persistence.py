"""Persist\u00eancia de estoque; a implementa\u00e7\u00e3o PostgreSQL \u00e9 o runtime padr\u00e3o."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
import json
from typing import Any, Iterator


class InsufficientStockError(ValueError):
    """Falha de neg\u00f3cio: n\u00e3o deve entrar em retry."""


class InventoryRepository:
    """Double em mem\u00f3ria usado somente por testes unit\u00e1rios."""

    durable = False

    def __init__(self, stock: dict[str, int] | None = None) -> None:
        self._stock = dict(stock or {})
        self._reservations: dict[str, list[dict[str, Any]]] = {}
        self._processed: set[str] = set()
        self._terminal_failures: set[str] = set()
        self._fallbacks: Counter[str] = Counter()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        yield

    def available(self, sku: str) -> int:
        """Retorna a quantidade disponível do SKU, ou zero quando ausente."""
        return self._stock.get(sku, 0)

    def reserve(self, order_id: str, items: list[dict[str, Any]]) -> None:
        """Reserva todos os itens de uma vez ou levanta erro sem alterar o estoque."""
        if order_id in self._reservations:
            return
        requested = _requested_items(items)
        missing = [sku for sku, quantity in requested.items() if self.available(sku) < quantity]
        if missing:
            raise InsufficientStockError("insufficient_stock")
        for sku, quantity in requested.items():
            self._stock[sku] -= quantity
        self._reservations[order_id] = deepcopy(items)

    def release(self, order_id: str) -> bool:
        """Desfaz uma reserva existente e informa se havia algo para liberar."""
        items = self._reservations.pop(order_id, None)
        if items is None:
            return False
        for item in items:
            self._stock[item["sku"]] = self.available(item["sku"]) + item["quantity"]
        return True

    def was_processed(self, event_id: str) -> bool:
        """Indica se a Inbox já confirmou o processamento do evento."""
        return event_id in self._processed

    def mark_processed(self, event_id: str) -> None:
        """Registra o evento na Inbox para impedir uma nova aplicação."""
        self._processed.add(event_id)

    def mark_terminal_failure(self, event_id: str) -> bool:
        """Reivindica atomamente o fallback terminal de um evento."""
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


def _requested_items(items: list[dict[str, Any]]) -> Counter[str]:
    requested: Counter[str] = Counter()
    for item in items:
        sku = item.get("sku")
        quantity = item.get("quantity")
        if not isinstance(sku, str) or not sku or not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("invalid inventory item")
        requested[sku] += quantity
    if not requested:
        raise ValueError("order must contain items")
    return requested


class PostgresInventoryRepository:
    """Inbox, reserva e Outbox dur\u00e1veis no banco pertencente ao inventory-service."""

    durable = True

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        self._connection: Any | None = None
        self._depth = 0

    @property
    def connection(self) -> Any:
        if self._connection is None:
            import psycopg

            self._connection = psycopg.connect(self.database_url)
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def initialize(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inventory_stock (
                    sku TEXT PRIMARY KEY, quantity INTEGER NOT NULL CHECK (quantity >= 0)
                );
                CREATE TABLE IF NOT EXISTS inventory_reservations (
                    order_id TEXT PRIMARY KEY, items JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inventory_inbox (
                    event_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                    fallback_count INTEGER NOT NULL DEFAULT 0,
                    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS inventory_outbox (
                    event_id TEXT PRIMARY KEY, topic TEXT NOT NULL, event JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ NULL
                );
            """)
        self.connection.commit()

    def seed_stock(self, stock: dict[str, int]) -> None:
        """Insere estoque inicial apenas quando o SKU ainda não existe."""
        with self.connection.cursor() as cursor:
            for sku, quantity in stock.items():
                cursor.execute(
                    "INSERT INTO inventory_stock (sku, quantity) VALUES (%s, %s) ON CONFLICT (sku) DO NOTHING",
                    (sku, quantity),
                )
        self.connection.commit()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._depth += 1
        try:
            yield
            if self._depth == 1:
                self.connection.commit()
        except Exception:
            if self._depth == 1:
                self.connection.rollback()
            raise
        finally:
            self._depth -= 1

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        cursor = self.connection.cursor()
        cursor.execute(sql, params)
        if self._depth == 0:
            self.connection.commit()
        return cursor

    def available(self, sku: str) -> int:
        with self._execute("SELECT quantity FROM inventory_stock WHERE sku = %s", (sku,)) as cursor:
            row = cursor.fetchone()
        return int(row[0]) if row else 0

    def reserve(self, order_id: str, items: list[dict[str, Any]]) -> None:
        requested = _requested_items(items)
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM inventory_reservations WHERE order_id = %s", (order_id,))
            if cursor.fetchone():
                return
            for sku, quantity in requested.items():
                cursor.execute("SELECT quantity FROM inventory_stock WHERE sku = %s FOR UPDATE", (sku,))
                row = cursor.fetchone()
                if row is None or row[0] < quantity:
                    raise InsufficientStockError("insufficient_stock")
            for sku, quantity in requested.items():
                cursor.execute("UPDATE inventory_stock SET quantity = quantity - %s WHERE sku = %s", (quantity, sku))
            cursor.execute("INSERT INTO inventory_reservations (order_id, items) VALUES (%s, %s::jsonb)", (order_id, json.dumps(items)))
        if self._depth == 0:
            self.connection.commit()

    def release(self, order_id: str) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM inventory_reservations WHERE order_id = %s RETURNING items", (order_id,))
            row = cursor.fetchone()
            if not row:
                return False
            items = row[0] if isinstance(row[0], list) else json.loads(row[0])
            for item in items:
                cursor.execute(
                    "INSERT INTO inventory_stock (sku, quantity) VALUES (%s, %s) "
                    "ON CONFLICT (sku) DO UPDATE SET quantity = inventory_stock.quantity + EXCLUDED.quantity",
                    (item["sku"], item["quantity"]),
                )
        if self._depth == 0:
            self.connection.commit()
        return True

    def was_processed(self, event_id: str) -> bool:
        with self._execute("SELECT 1 FROM inventory_inbox WHERE event_id = %s AND state = 'processed'", (event_id,)) as cursor:
            return cursor.fetchone() is not None

    def mark_processed(self, event_id: str) -> None:
        self._execute("INSERT INTO inventory_inbox (event_id, state) VALUES (%s, 'processed') ON CONFLICT (event_id) DO NOTHING", (event_id,)).close()

    def mark_terminal_failure(self, event_id: str) -> bool:
        with self._execute("INSERT INTO inventory_inbox (event_id, state) VALUES (%s, 'terminal') ON CONFLICT (event_id) DO NOTHING RETURNING event_id", (event_id,)) as cursor:
            return cursor.fetchone() is not None

    def has_terminal_failure(self, event_id: str) -> bool:
        with self._execute("SELECT 1 FROM inventory_inbox WHERE event_id = %s AND state = 'terminal'", (event_id,)) as cursor:
            return cursor.fetchone() is not None

    def record_fallback(self, event_id: str) -> None:
        self._execute("UPDATE inventory_inbox SET fallback_count = fallback_count + 1 WHERE event_id = %s", (event_id,)).close()

    def fallback_count(self, event_id: str) -> int:
        with self._execute("SELECT fallback_count FROM inventory_inbox WHERE event_id = %s", (event_id,)) as cursor:
            row = cursor.fetchone()
        return int(row[0]) if row else 0

    def add_outbox(self, event: dict[str, Any], *, topic: str) -> None:
        """Persiste um evento de saída antes de qualquer publicação Kafka."""
        self._execute(
            "INSERT INTO inventory_outbox (event_id, topic, event) VALUES (%s, %s, %s::jsonb) ON CONFLICT (event_id) DO NOTHING",
            (event["event_id"], topic, json.dumps(event)),
        ).close()

    def pending_outbox(self) -> list[dict[str, Any]]:
        """Lista eventos de saída ainda não confirmados pelo produtor."""
        with self._execute("SELECT event FROM inventory_outbox WHERE published_at IS NULL ORDER BY created_at") as cursor:
            rows = cursor.fetchall()
        return [row[0] if isinstance(row[0], dict) else json.loads(row[0]) for row in rows]

    def mark_outbox_published(self, event_id: str) -> None:
        self._execute("UPDATE inventory_outbox SET published_at = now() WHERE event_id = %s AND published_at IS NULL", (event_id,)).close()
