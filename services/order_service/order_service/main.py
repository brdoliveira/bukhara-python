"""Fábrica ASGI do order-service."""

from fastapi import FastAPI

from .api import register_routes
from .persistence import OrderStore
from .producer import EventProducer, InMemoryProducer


def create_app(store: OrderStore | None = None, producer: EventProducer | None = None) -> FastAPI:
    app = FastAPI(title="order-service")
    register_routes(app, store or OrderStore(), producer or InMemoryProducer())
    return app


app = create_app()
