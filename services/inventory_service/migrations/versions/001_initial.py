"""Esquema inicial de Inbox, reservas e Outbox do inventory-service."""

revision = "001_initial"
down_revision = None

TABLES = ("inventory_stock", "inventory_reservations", "inventory_inbox", "inventory_outbox")


def upgrade() -> None:
    # Importa tarde para que o m\u00f3dulo tamb\u00e9m seja leg\u00edvel nos testes unit\u00e1rios.
    from alembic import op

    op.execute("CREATE TABLE inventory_stock (sku TEXT PRIMARY KEY, quantity INTEGER NOT NULL CHECK (quantity >= 0))")
    op.execute("CREATE TABLE inventory_reservations (order_id TEXT PRIMARY KEY, items JSONB NOT NULL)")
    op.execute("CREATE TABLE inventory_inbox (event_id TEXT PRIMARY KEY, state TEXT NOT NULL, fallback_count INTEGER NOT NULL DEFAULT 0, processed_at TIMESTAMPTZ NOT NULL DEFAULT now())")
    op.execute("CREATE TABLE inventory_outbox (event_id TEXT PRIMARY KEY, topic TEXT NOT NULL, event JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ NULL)")


def downgrade() -> None:
    from alembic import op

    for table in reversed(TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table}")
