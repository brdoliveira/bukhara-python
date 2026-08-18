"""Inbox e efeitos duráveis do notification-service."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool


class NotificationRepository:
    """Double em memória usado pelos testes unitários."""

    def __init__(self) -> None:
        self._processed: set[str] = set()
        self._terminal_failures: set[str] = set()
        self._fallbacks: Counter[str] = Counter()

    def was_processed(self, event_id: str) -> bool:
        return event_id in self._processed

    def mark_processed(self, event_id: str) -> None:
        self._processed.add(event_id)

    def mark_terminal_failure(self, event_id: str) -> bool:
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

    def is_available(self) -> bool:
        return True


metadata = MetaData()
inbox = Table(
    "notification_inbox", metadata,
    Column("event_id", String(64), primary_key=True),
    Column("processed_at", DateTime(timezone=True), nullable=False),
)
terminal_failures = Table(
    "notification_terminal_failures", metadata,
    Column("event_id", String(64), primary_key=True),
    Column("failed_at", DateTime(timezone=True), nullable=False),
)
fallbacks = Table(
    "notification_fallbacks", metadata,
    Column("event_id", String(64), primary_key=True),
    Column("count", Integer, nullable=False, default=0),
)


class PostgresNotificationRepository:
    """Implementação SQLAlchemy usada pelo processo de produção."""

    def __init__(self, database_url: str) -> None:
        options: dict = {"future": True, "pool_pre_ping": True}
        if database_url.startswith("sqlite") and ":memory:" in database_url:
            options.update(connect_args={"check_same_thread": False}, poolclass=StaticPool)
        self.engine: Engine = create_engine(database_url, **options)
        metadata.create_all(self.engine)

    def was_processed(self, event_id: str) -> bool:
        with self.engine.connect() as connection:
            return connection.execute(select(inbox.c.event_id).where(inbox.c.event_id == event_id)).first() is not None

    def mark_processed(self, event_id: str) -> None:
        try:
            with self.engine.begin() as connection:
                connection.execute(inbox.insert().values(event_id=event_id, processed_at=datetime.now(UTC)))
        except IntegrityError:
            return

    def mark_terminal_failure(self, event_id: str) -> bool:
        try:
            with self.engine.begin() as connection:
                connection.execute(terminal_failures.insert().values(event_id=event_id, failed_at=datetime.now(UTC)))
            return True
        except IntegrityError:
            return False

    def has_terminal_failure(self, event_id: str) -> bool:
        with self.engine.connect() as connection:
            return connection.execute(
                select(terminal_failures.c.event_id).where(terminal_failures.c.event_id == event_id)
            ).first() is not None

    def record_fallback(self, event_id: str) -> None:
        with self.engine.begin() as connection:
            row = connection.execute(select(fallbacks).where(fallbacks.c.event_id == event_id)).mappings().first()
            if row is None:
                connection.execute(fallbacks.insert().values(event_id=event_id, count=1))
            else:
                connection.execute(fallbacks.update().where(fallbacks.c.event_id == event_id).values(count=row["count"] + 1))

    def fallback_count(self, event_id: str) -> int:
        with self.engine.connect() as connection:
            row = connection.execute(select(fallbacks.c.count).where(fallbacks.c.event_id == event_id)).first()
            return int(row[0]) if row else 0

    def is_available(self) -> bool:
        try:
            with self.engine.connect() as connection:
                connection.execute(select(1))
            return True
        except Exception:
            return False
