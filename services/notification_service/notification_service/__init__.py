"""Serviço de notificação da saga de pedidos."""

from .consumer import NotificationConsumer
from .handler import NotificationHandler

__all__ = ["NotificationConsumer", "NotificationHandler"]
