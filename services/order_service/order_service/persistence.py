"""Persistência transacional de pedidos e da outbox.

SQLite é usado nos testes; a mesma camada usa uma URL PostgreSQL em produção.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Optional

from sqlalchemy import Boolean, Column, DateTime, MetaData, String, Table, Text, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool


metadata = MetaData()
orders = Table(
    "orders", metadata,
    Column("order_id", String(36), primary_key=True),
    Column("idempotency_key", String(255), nullable=False, unique=True),
    Column("correlation_id", String(36), nullable=False),
    Column("payload", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
outbox = Table(
    "outbox", metadata,
    Column("event_id", String(36), primary_key=True),
    Column("order_id", String(36), nullable=False),
    Column("correlation_id", String(36), nullable=False),
    Column("topic", String(128), nullable=False),
    Column("payload", Text, nullable=False),
    Column("published", Boolean, nullable=False, default=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


@dataclass(frozen=True)
class StoredOrder:
    """Agrupa os dados de um pedido antes de sua persistência transacional."""

    order_id: str
    correlation_id: str
    payload: dict[str, Any]
    event_id: str


@dataclass(frozen=True)
class OutboxEvent:
    """Representa um evento de pedido que ainda pode precisar de publicação."""

    event_id: str
    order_id: str
    correlation_id: str
    topic: str
    payload: dict[str, Any]
    created_at: datetime

    def message(self) -> dict[str, Any]:
        """Serializa o evento no envelope esperado pelos consumidores Kafka."""
        return {
            "event_id": self.event_id,
            "event_type": "order.created",
            "type": "order.created",
            "event_version": 1,
            "occurred_at": self.created_at.isoformat(),
            "producer": "order-service",
            "causation_id": None,
            "order_id": self.order_id,
            "correlation_id": self.correlation_id,
            "payload": {"order_id": self.order_id, **self.payload},
            **self.payload,
        }


class OrderStore:
    """Persiste pedidos idempotentes e eventos da Outbox na mesma transação."""

    def __init__(self, database_url: Optional[str] = None) -> None:
        database_url = database_url or os.getenv("DATABASE_URL", "sqlite+pysqlite:///:memory:")
        options: dict[str, Any] = {"future": True}
        # TestClient atende endpoints em outra thread. StaticPool faz o banco
        # SQLite em memória permanecer o mesmo sem afetar URLs PostgreSQL.
        if database_url.startswith("sqlite") and ":memory:" in database_url:
            options.update(connect_args={"check_same_thread": False}, poolclass=StaticPool)
        self.engine: Engine = create_engine(database_url, **options)
        metadata.create_all(self.engine)

    @classmethod
    def from_environment(cls) -> "OrderStore":
        """Cria o repositório usando a URL de banco obrigatória do runtime."""
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is required for the order-service runtime")
        return cls(database_url)

    def create_order_with_outbox(self, order: StoredOrder, idempotency_key: str) -> tuple[StoredOrder, bool]:
        """Cria pedido e evento na mesma transação; uma chave só pode criar uma vez."""
        now = datetime.now(UTC)
        serialized = json.dumps(order.payload, sort_keys=True)
        try:
            with self.engine.begin() as connection:
                connection.execute(orders.insert().values(
                    order_id=order.order_id, idempotency_key=idempotency_key,
                    correlation_id=order.correlation_id, payload=serialized, created_at=now,
                ))
                connection.execute(outbox.insert().values(
                    event_id=order.event_id, order_id=order.order_id,
                    correlation_id=order.correlation_id, topic="orders.events",
                    payload=serialized, published=False, created_at=now,
                ))
            return order, True
        except IntegrityError:
            with self.engine.connect() as connection:
                row = connection.execute(select(orders).where(orders.c.idempotency_key == idempotency_key)).mappings().one()
                event = connection.execute(select(outbox).where(outbox.c.order_id == row["order_id"])).mappings().one()
            if row["payload"] != serialized:
                raise ValueError("idempotency key was already used with a different order")
            return StoredOrder(row["order_id"], row["correlation_id"], json.loads(row["payload"]), event["event_id"]), False

    def pending_events(self) -> list[OutboxEvent]:
        """Retorna os eventos que ainda não receberam confirmação de publicação."""
        with self.engine.connect() as connection:
            rows = connection.execute(select(outbox).where(outbox.c.published.is_(False)).order_by(outbox.c.created_at)).mappings()
            return [OutboxEvent(r["event_id"], r["order_id"], r["correlation_id"], r["topic"], json.loads(r["payload"]), r["created_at"]) for r in rows]

    def mark_published(self, event_id: str) -> None:
        """Marca um evento como publicado somente após confirmação do produtor."""
        with self.engine.begin() as connection:
            connection.execute(outbox.update().where(outbox.c.event_id == event_id).values(published=True, published_at=datetime.now(UTC)))

    def is_available(self) -> bool:
        """Informa se o banco aceita uma consulta simples de prontidão."""
        try:
            with self.engine.connect() as connection:
                connection.execute(select(1))
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Libera as conexões mantidas pelo repositório."""
        self.engine.dispose()
