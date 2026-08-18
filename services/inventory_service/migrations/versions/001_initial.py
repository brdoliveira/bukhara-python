"""Esquema inicial de Inbox, reservas e Outbox do inventory-service."""

revision = "001_initial"
down_revision = None

TABLES = ("inventory_stock", "inventory_reservations", "inventory_inbox", "inventory_outbox")
