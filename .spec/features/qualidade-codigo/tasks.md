# Tasks: Documentação e qualidade do código

> feature: qualidade-codigo

## T-013 — Documentar e testar pacotes compartilhados [concluida]
- Refs: US-008, US-009, AC-024, AC-026
- Arquivos: packages/event_bus/event_bus/envelope.py, packages/event_bus/event_bus/health.py, packages/event_bus/event_bus/inbox.py, packages/event_bus/event_bus/outbox.py, packages/event_bus/event_bus/retry.py, packages/event_bus/tests/test_envelope.py, packages/event_bus/tests/test_health.py, packages/event_bus/tests/test_inbox.py, packages/event_bus/tests/test_outbox.py, packages/event_bus/tests/test_retry.py, packages/observability/observability/logging.py, packages/observability/observability/telemetry.py, packages/observability/tests/test_logging.py, packages/observability/tests/test_telemetry.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: cobrir validação, estados terminais, degradação do exportador e contratos de retry sem I/O externo.

## T-014 — Documentar e testar o serviço de pedidos [concluida]
- Refs: US-008, US-009, AC-024, AC-027
- Arquivos: services/order_service/order_service/api.py, services/order_service/order_service/main.py, services/order_service/order_service/models.py, services/order_service/order_service/outbox.py, services/order_service/order_service/persistence.py, services/order_service/order_service/producer.py, services/order_service/tests/test_api.py, services/order_service/tests/test_health.py, services/order_service/tests/test_observability.py, services/order_service/tests/test_outbox.py, services/order_service/tests/test_recovery.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: priorizar conflito idempotente, falha de publicação, recovery loop e lifecycle do produtor.

## T-015 — Documentar e testar o serviço de estoque [concluida]
- Refs: US-008, US-009, AC-024, AC-028
- Arquivos: services/inventory_service/inventory_service/consumer.py, services/inventory_service/inventory_service/handler.py, services/inventory_service/inventory_service/main.py, services/inventory_service/inventory_service/outbox.py, services/inventory_service/inventory_service/persistence.py, services/inventory_service/tests/test_consumer.py, services/inventory_service/tests/test_handler.py, services/inventory_service/tests/test_health.py, services/inventory_service/tests/test_observability.py, services/inventory_service/tests/test_persistence.py, services/inventory_service/tests/test_runtime.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: cobrir branches de runtime, Postgres por doubles, retry, DLQ, Outbox e encerramento idempotente.

## T-016 — Documentar e testar o serviço de pagamento [concluida]
- Refs: US-008, US-009, AC-024, AC-029
- Arquivos: services/payment_service/payment_service/consumer.py, services/payment_service/payment_service/handler.py, services/payment_service/payment_service/main.py, services/payment_service/payment_service/outbox.py, services/payment_service/payment_service/persistence.py, services/payment_service/tests/test_consumer.py, services/payment_service/tests/test_handler.py, services/payment_service/tests/test_health.py, services/payment_service/tests/test_observability.py, services/payment_service/tests/test_persistence.py, services/payment_service/tests/test_runtime.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: cobrir lifecycle, readiness, publicação pendente, compensação e falhas terminais.

## T-017 — Documentar e testar o serviço de notificação [concluida]
- Refs: US-008, US-009, AC-024, AC-030
- Arquivos: services/notification_service/notification_service/adapter.py, services/notification_service/notification_service/consumer.py, services/notification_service/notification_service/handler.py, services/notification_service/notification_service/main.py, services/notification_service/notification_service/persistence.py, services/notification_service/tests/test_consumer.py, services/notification_service/tests/test_handler.py, services/notification_service/tests/test_health.py, services/notification_service/tests/test_observability.py, services/notification_service/tests/test_persistence.py, services/notification_service/tests/test_worker.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: cobrir worker Kafka, normalização de envelopes, retry, fallback, DLQ e stop seguro.

## T-018 — Publicar referência e ativar gate de cobertura [concluida]
- Refs: US-008, US-009, AC-024, AC-025, AC-031
- Arquivos: pyproject.toml, README.md, docs/code-reference.md, tests/integration/test_code_documentation.py, tests/integration/test_quality_gate.py
- Modelo: gpt-5.6-luna
- Esforço: baixo
- Notas: executar após T-013–T-017; declarar pytest-cov, documentar os módulos e fazer a suíte falhar abaixo da meta confirmada.
