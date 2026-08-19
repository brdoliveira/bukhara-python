# Referência de código

Este guia aponta os contratos públicos e os pontos de extensão da saga. Os
dois pacotes compartilhados são reutilizados pelos quatro serviços; os nomes
abaixo levam ao código-fonte correspondente.

## Pacotes compartilhados

- [`event_bus`](../packages/event_bus/event_bus/): envelopes versionados,
  Inbox, Outbox, retry, DLQ e endpoints de saúde.
- [`observability`](../packages/observability/observability/): logs
  estruturados, traces, métricas e propagação de contexto Kafka.

## Serviços

- [`order_service`](../services/order_service/order_service/): API de pedidos,
  modelos, persistência, produtor e recuperação da Outbox.
- [`inventory_service`](../services/inventory_service/inventory_service/):
  consumidor e handler de reservas, persistência, Outbox e runtime.
- [`payment_service`](../services/payment_service/payment_service/):
  consumidor, decisões de pagamento, compensação, persistência e runtime.
- [`notification_service`](../services/notification_service/notification_service/):
  consumidor, handler, adaptador, persistência e worker.

## Dependências e navegação

Os serviços dependem dos pacotes compartilhados e usam FastAPI, Kafka,
PostgreSQL e OpenTelemetry. Para seguir um fluxo, comece pela API ou pelo
`main.py` do serviço, acompanhe o handler e consulte `persistence.py` e
`outbox.py`; os consumidores mostram a entrada de eventos e os adaptadores
isolam dependências externas.

## Comandos de teste

Na raiz do projeto, instale as dependências de desenvolvimento e execute:

```bash
py -3.12 -m pip install -e ".[dev]"
py -3.12 -B -m pytest -q -p no:cacheprovider
```

O pytest-cov mede somente os seis diretórios de produção dos pacotes e
serviços, excluindo testes e migrações. A suíte é bloqueada quando essa
cobertura global de produção fica abaixo de 85%.
