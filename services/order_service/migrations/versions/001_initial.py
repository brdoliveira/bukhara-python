"""Initial order and outbox tables."""

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    from order_service.persistence import metadata
    from alembic import op
    metadata.create_all(op.get_bind())


def downgrade() -> None:
    from alembic import op
    from order_service.persistence import metadata
    metadata.drop_all(op.get_bind())
