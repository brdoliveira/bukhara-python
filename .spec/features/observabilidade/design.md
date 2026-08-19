# Design: Observabilidade da saga distribuída

## Decisão

O perfil local usa `grafana/otel-lgtm:0.30.0`, imagem de desenvolvimento que reúne OpenTelemetry Collector, Grafana, Mimir/Prometheus, Loki e Tempo. Os processos Python enviam traces, métricas e logs por OTLP/HTTP para `http://otel-lgtm:4318`.

## Instrumentação

- O SDK OpenTelemetry recebe `service.name`, `deployment.environment=local` e versão do serviço como atributos de recurso.
- FastAPI, aiokafka, SQLAlchemy e psycopg usam instrumentação OpenTelemetry; o propagador W3C `traceparent` conecta publicação e consumo Kafka.
- Uma biblioteca em `packages/observability` concentra inicialização, logs estruturados e métricas de negócio.
- Exportadores usam processadores em lote. Erros de exportação são assíncronos e nunca participam de transações de negócio.

## Métricas

| Instrumento | Tipo | Dimensões permitidas |
|---|---|---|
| `saga.events.processed` | counter | service, event.type, outcome |
| `saga.retries.scheduled` | counter | service, event.type, attempt |
| `saga.fallbacks.executed` | counter | service, event.type |
| `saga.dlq.messages` | counter | service, error.type |
| `saga.outbox.published` | counter | service, topic |

Identificadores de pedido, evento e correlação ficam em spans e logs, nunca em labels de métrica.

## Logs e correlação

Logs continuam no stdout em JSON e também seguem pelo pipeline OTLP. Cada registro inclui serviço, severidade, mensagem, trace e span atuais. O contexto de evento acrescenta `order_id` e `correlation_id` como atributos, permitindo navegar do dashboard para Tempo e Loki.

## Dashboard

O dashboard `Order Saga / Overview` é provisionado no Grafana e mostra sinais RED de HTTP, eventos processados, retries, fallback e DLQ. Links de dados abrem o Explore nos backends Tempo e Loki. O Grafana local usa `admin/admin` apenas no perfil de estudo.

## Falhas

- Sem `OTEL_EXPORTER_OTLP_ENDPOINT`, o código usa providers sem exportador de rede e preserva testes unitários.
- Backend OTLP indisponível não altera readiness dos serviços nem bloqueia handlers.
- A stack LGTM não é dependência de inicialização de Kafka/PostgreSQL; os serviços podem iniciar sem ela.
