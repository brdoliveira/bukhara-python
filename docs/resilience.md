# Operação e resiliência

## Retry e DLQ (AC-008, AC-009)

Falhas transitórias são reagendadas no máximo três vezes, com atraso exponencial de `1s`, `2s` e `4s`. A mensagem mantém `event_id`, `correlation_id` e registra a tentativa em `retry_attempt`.

Quando a terceira tentativa falha, o consumidor executa o fallback seguro uma única vez e envia a mensagem à DLQ do serviço. A entrada registra tipo do erro, serviço de origem e total de tentativas. Entregas posteriores são confirmadas sem repetir o fallback.

```bash
pytest -q tests/integration/test_retry_and_dlq.py
docker compose logs --since=10m inventory-service payment-service notification-service
```

Mensagens inválidas vão diretamente para a DLQ com erros de validação; não entram no retry.

## Saúde e prontidão (AC-012)

`GET /health` responde sucesso quando o processo está vivo, mesmo sem Kafka. `GET /ready` só responde sucesso quando Kafka e PostgreSQL estão disponíveis; caso contrário responde 503.

```bash
curl -i http://localhost:8000/health
curl -i http://localhost:8000/ready
curl -i http://localhost:8001/health
curl -i http://localhost:8001/ready
```

Kafka e PostgreSQL possuem healthchecks no Compose. Em caso de falha, examine `docker compose ps` e os logs antes de reenviar pedidos.

## Limites do MVP

O fluxo oferece processamento efetivamente uma vez por idempotência local, não garantia global de exatamente uma vez. Integrações externas são adaptadores simulados; Kubernetes, autenticação e observabilidade distribuída estão fora do escopo.
