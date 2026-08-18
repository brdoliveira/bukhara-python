"""Serviço de estoque da saga de pedidos."""

from .consumer import InventoryConsumer
from .handler import InventoryHandler

__all__ = ["InventoryConsumer", "InventoryHandler"]
