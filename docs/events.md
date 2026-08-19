# Contrato operacional de eventos

Todo evento carrega `event_id`, `event_type`, `event_version`, `producer`, `order_id`, `correlation_id`, `causation_id` e `payload`. O `event_id` permite deduplicação; `correlation_id` acompanha toda a saga. Consumidores aceitam o alias legado `type` apenas na borda.

## Fluxo principal

`order.created` inicia a saga. Estoque publica `inventory.reserved` ou `inventory.rejected`. Pagamento reage à reserva e publica `payment.approved` ou `payment.failed`; a falha definitiva também solicita `inventory.release.requested`, que resulta em `inventory.released`. Notificação reage a `payment.approved`.

```json
{
  "event_id": "evt-123",
  "event_type": "order.created",
  "event_version": 1,
  "producer": "order-service",
  "order_id": "ord-123",
  "correlation_id": "corr-123",
  "causation_id": null,
  "payload": {"items": [{"sku": "tea", "quantity": 1}]}
}
```

Os fatos trafegam em `orders.events`, `inventory.events`, `payments.events` e `notifications.events`. Consumidores preservam `event_id` e `correlation_id` em retries; `retry_attempt` começa em zero e aumenta a cada tentativa.

O serviço `kafka-init` cria todos os tópicos no Compose. Para inspecioná-los:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
docker compose logs -f order-service inventory-service payment-service notification-service
```
