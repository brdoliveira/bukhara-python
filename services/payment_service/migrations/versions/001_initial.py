"""Esquema inicial de Inbox, cobranças e Outbox do payment-service."""

revision = "001_initial"
down_revision = None

TABLES = ("payment_inbox", "payment_charges", "payment_outbox")

