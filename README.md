# fluxo-pedidos

Saga distribuída de pedidos, com Kafka, PostgreSQL e quatro serviços independentes.

## Executar localmente

Pré-requisitos: Docker com Compose e Python 3.11+. Suba a infraestrutura com:

```bash
docker compose up --build
```

O serviço de pedidos fica em `http://localhost:8000`; estoque, pagamento e notificação usam as portas 8001, 8002 e 8003. Consulte [`docs/events.md`](docs/events.md) para os eventos e [`docs/resilience.md`](docs/resilience.md) para retry, DLQ e saúde.

Para executar a suíte local:

```bash
py -3.12 -B -m pytest -q -p no:cacheprovider
```

Os testes anotados com `@spec:AC-008`, `@spec:AC-009` e `@spec:AC-012` são a prova executável dos procedimentos documentados.

Para subir e investigar a observabilidade local, consulte o [runbook de observabilidade](docs/observability.md).
