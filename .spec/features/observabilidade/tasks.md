# Tasks: Observabilidade da saga distribuída

> feature: observabilidade

## T-008 — Biblioteca compartilhada e contratos de telemetria [concluida]
- Refs: US-004, US-005, AC-014, AC-015, AC-016, AC-017, AC-018
- Arquivos: pyproject.toml, packages/observability/observability/__init__.py, packages/observability/observability/telemetry.py, packages/observability/observability/logging.py, packages/observability/tests/test_telemetry.py, packages/observability/tests/test_logging.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Configura SDK OpenTelemetry e exportadores OTLP assíncronos, correlação de logs e instrumentos de baixa cardinalidade. Deve terminar antes de T-009.

## T-009 — Instrumentação dos quatro microsserviços [concluida]
- Refs: US-004, US-005, AC-014, AC-015, AC-016, AC-017, AC-018
- Arquivos: packages/observability/observability/telemetry.py, services/order_service/order_service/main.py, services/order_service/order_service/api.py, services/order_service/order_service/producer.py, services/order_service/tests/test_observability.py, services/inventory_service/inventory_service/main.py, services/inventory_service/tests/test_observability.py, services/payment_service/payment_service/main.py, services/payment_service/tests/test_observability.py, services/notification_service/notification_service/main.py, services/notification_service/notification_service/consumer.py, services/notification_service/tests/test_observability.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Integra HTTP, Kafka, Inbox/Outbox e resiliência. O arquivo compartilhado com T-008 força execução posterior à fundação.

## T-010 — Stack LGTM e dashboard provisionado [concluida]
- Refs: US-005, US-006, AC-017, AC-019, AC-020
- Arquivos: docker-compose.yml, Dockerfile, .env.example, observability/grafana/dashboards.yaml, observability/grafana/order-saga.json, tests/integration/test_observability_stack.py
- Modelo: gpt-5.6-terra
- Esforço: alto
- Notas: Usa `grafana/otel-lgtm:0.30.0`, endpoint OTLP/HTTP e dashboard local como código.

## T-011 — Runbook de investigação [concluida]
- Refs: US-006, AC-020, AC-021
- Arquivos: README.md, docs/observability.md, tests/integration/test_observability_docs.py
- Modelo: gpt-5.6-luna
- Esforço: baixo
- Notas: Documenta acesso, geração de tráfego, consultas por correlação e limites do perfil local.
