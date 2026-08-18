# Tasks: Saga distribuída de pedidos

> feature: fluxo-pedidos

## T-001 — Fundação, contratos e resiliência compartilhada [pendente]

- Refs: US-001, US-002, US-003, AC-008, AC-009, AC-010, AC-011, AC-012, AC-013
- Arquivos: pyproject.toml, packages/event_bus/event_bus/__init__.py, packages/event_bus/event_bus/envelope.py, packages/event_bus/event_bus/retry.py, packages/event_bus/event_bus/inbox.py, packages/event_bus/event_bus/outbox.py, packages/event_bus/event_bus/health.py, packages/event_bus/tests/test_envelope.py, packages/event_bus/tests/test_retry.py, packages/event_bus/tests/test_inbox.py, packages/event_bus/tests/test_outbox.py, packages/event_bus/tests/test_health.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Define envelope versionado, cabeçalhos de retry, classificação de falhas, DLQ, contratos Inbox/Outbox e health/readiness reutilizados pelos serviços.

## T-002 — API e produtor do serviço de pedidos [pendente]

- Refs: US-001, US-003, AC-001, AC-002, AC-003, AC-012, AC-013
- Arquivos: services/order_service/order_service/__init__.py, services/order_service/order_service/main.py, services/order_service/order_service/api.py, services/order_service/order_service/models.py, services/order_service/order_service/producer.py, services/order_service/order_service/persistence.py, services/order_service/order_service/outbox.py, services/order_service/migrations/env.py, services/order_service/migrations/versions/001_initial.py, services/order_service/tests/test_api.py, services/order_service/tests/test_outbox.py, services/order_service/tests/test_health.py
- Modelo: gpt-5.6-terra
- Esforço: medio
- Notas: Expõe POST /orders, /health e /ready; persiste pedido e evento atomicamente e publica a Outbox com recuperação.

## T-003 — Consumidor do serviço de estoque [pendente]

- Refs: US-002, US-003, AC-004, AC-005, AC-007, AC-008, AC-009, AC-010, AC-011, AC-012
- Arquivos: services/inventory_service/inventory_service/__init__.py, services/inventory_service/inventory_service/main.py, services/inventory_service/inventory_service/consumer.py, services/inventory_service/inventory_service/handler.py, services/inventory_service/inventory_service/adapter.py, services/inventory_service/inventory_service/persistence.py, services/inventory_service/inventory_service/outbox.py, services/inventory_service/migrations/env.py, services/inventory_service/migrations/versions/001_initial.py, services/inventory_service/tests/test_consumer.py, services/inventory_service/tests/test_handler.py, services/inventory_service/tests/test_persistence.py, services/inventory_service/tests/test_health.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Trata criação e compensação, diferenciando rejeição de negócio de falha transitória.

## T-004 — Consumidor do serviço de pagamento [pendente]

- Refs: US-002, US-003, AC-006, AC-007, AC-008, AC-009, AC-010, AC-011, AC-012
- Arquivos: services/payment_service/payment_service/__init__.py, services/payment_service/payment_service/main.py, services/payment_service/payment_service/consumer.py, services/payment_service/payment_service/handler.py, services/payment_service/payment_service/adapter.py, services/payment_service/payment_service/persistence.py, services/payment_service/payment_service/outbox.py, services/payment_service/migrations/env.py, services/payment_service/migrations/versions/001_initial.py, services/payment_service/tests/test_consumer.py, services/payment_service/tests/test_handler.py, services/payment_service/tests/test_persistence.py, services/payment_service/tests/test_health.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Publica aprovação; em falha definitiva executa fallback com payment.failed e pedido de liberação de estoque.

## T-005 — Consumidor do serviço de notificação [pendente]

- Refs: US-002, US-003, AC-006, AC-008, AC-009, AC-010, AC-011, AC-012
- Arquivos: services/notification_service/notification_service/__init__.py, services/notification_service/notification_service/main.py, services/notification_service/notification_service/consumer.py, services/notification_service/notification_service/handler.py, services/notification_service/notification_service/adapter.py, services/notification_service/notification_service/persistence.py, services/notification_service/migrations/env.py, services/notification_service/migrations/versions/001_initial.py, services/notification_service/tests/test_consumer.py, services/notification_service/tests/test_handler.py, services/notification_service/tests/test_persistence.py, services/notification_service/tests/test_health.py
- Modelo: gpt-5.6-terra
- Esforço: medio
- Notas: Reage a resultados finais, aplica deduplicação e envia falhas definitivas à DLQ.

## T-006 — Infraestrutura local e teste da saga [pendente]

- Refs: US-001, US-002, US-003, AC-001, AC-004, AC-005, AC-006, AC-007, AC-008, AC-009, AC-010, AC-011, AC-012, AC-013
- Arquivos: docker-compose.yml, Dockerfile, .dockerignore, .env.example, scripts/create-topics.sh, scripts/init-databases.sql, tests/integration/test_order_saga.py, tests/integration/test_retry_and_dlq.py, tests/integration/test_outbox_recovery.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Sobe Kafka KRaft, PostgreSQL e os quatro serviços, cria tópicos e bancos e prova saga, compensação, retry, DLQ e recuperação da Outbox.

## T-007 — Documentação operacional [pendente]

- Refs: US-001, US-002, US-003, AC-008, AC-009, AC-012
- Arquivos: README.md, docs/events.md, docs/resilience.md
- Modelo: gpt-5.6-luna
- Esforço: baixo
- Notas: Explica execução local, contratos, cenários de falha, inspeção de retry/DLQ e limites do MVP.
