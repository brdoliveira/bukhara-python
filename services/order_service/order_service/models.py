"""Contratos HTTP e de persistência do serviço de pedidos."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class OrderItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=0)


class CreateOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[OrderItem] = Field(min_length=1)


class OrderAccepted(BaseModel):
    order_id: str
    status: str = "accepted"
    correlation_id: str


def order_payload(items: list[OrderItem]) -> dict[str, Any]:
    """Produz payload JSON estável para o evento e para checagem idempotente."""
    return {"items": [item.model_dump(mode="json") for item in items]}
