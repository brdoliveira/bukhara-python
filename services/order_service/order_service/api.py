"""Rotas HTTP do serviço de pedidos."""

from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request, Response, status

from .models import CreateOrderRequest, OrderAccepted, order_payload
from .producer import EventProducer, OutboxPublisher
from .persistence import OrderStore, StoredOrder


def register_routes(app: FastAPI, store: OrderStore, producer: EventProducer) -> OutboxPublisher:
    """Registra os endpoints HTTP e devolve o publicador da Outbox associado.

    A criação de pedidos é idempotente: o pedido e seu evento são persistidos
    antes de qualquer tentativa de publicação, permitindo recuperação posterior.
    """
    publisher = OutboxPublisher(store, producer)

    @app.post("/orders", response_model=OrderAccepted, status_code=status.HTTP_202_ACCEPTED)
    async def create_order(
        request: CreateOrderRequest,
        http_request: Request,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
    ) -> OrderAccepted:
        candidate = StoredOrder(
            order_id=str(uuid4()),
            correlation_id=getattr(http_request.state, "correlation_id", str(uuid4())),
            payload=order_payload(request.items),
            event_id=str(uuid4()),
        )
        try:
            stored, created = store.create_order_with_outbox(candidate, idempotency_key)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        if created:
            try:
                await publisher.publish_pending_async()
            except ConnectionError:
                # A transação já confirmou. O recuperador publicará depois.
                pass
        app.state.logger.info(
            "order accepted",
            order_id=stored.order_id,
            correlation_id=stored.correlation_id,
            event_type="order.created",
        )
        return OrderAccepted(order_id=stored.order_id, correlation_id=stored.correlation_id)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/ready")
    def ready(response: Response) -> dict[str, str]:
        if store.is_available() and producer.is_available():
            return {"status": "ready"}
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not_ready"}

    return publisher
