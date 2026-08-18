# Contrato operacional de eventos

Todo evento carrega um envelope com `event_id`, `type`, `order_id`, `correlation_id` e `payload`. `event_id` identifica a mensagem para deduplicação; `correlation_id` acompanha toda a saga.

## Fluxo principal

`order.created` inicia a saga. Estoque publica `inventory.reserved` ou `inventory.rejected`. Pagamento reage à reserva e publica `payment.approved` ou `payment.failed`; a falha definitiva também solicita `inventory.release.requested`, que resulta em `inventory.released`. A notificação reage a `payment.approved`.

```json
{
  "event_id": "evt-123",
  "type": "order.created",
  "order_id": "ord-123",
  "correlation_id": "corr-123",
  "payload": {"items": [{"sku": "tea", "quantity": 1}]}
}
```

Consumidores preservam `event_id` e `correlation_id` ao reagendar. `retry_attempt` começa em zero e é incrementado a cada retry.

O serviço `kafka-init` cria os tópicos no Compose. Para inspeção:

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose logs -f inventory-service payment-service notification-service
```
