# Spec: Observabilidade da saga distribuída

> feature: observabilidade
> status: auditada

## Contexto

A saga possui quatro microsserviços e processamento assíncrono por Kafka. Sem telemetria correlacionada, uma operação precisa cruzar manualmente logs, bancos e tópicos para descobrir onde um pedido parou. A feature adiciona uma stack local LGTM completa e instrumenta HTTP, Kafka e operações de resiliência com padrões OpenTelemetry.

## Histórias

### US-004 — Operação acompanha um pedido entre os microsserviços

Como pessoa de operação, quero seguir um pedido do HTTP até a notificação, para localizar rapidamente o serviço e a etapa responsáveis por uma falha.

#### AC-014 — Requisições HTTP geram telemetria identificada por serviço

- **Dado** qualquer microsserviço em execução com a exportação OTLP habilitada
- **Quando** uma rota HTTP é chamada
- **Então** são produzidos trace e métricas com `service.name`, rota, método, status e duração
- **E** a resposta expõe o identificador de correlação usado nos logs

#### AC-015 — Contexto do trace atravessa os eventos Kafka

- **Dado** um evento publicado durante um trace ativo
- **Quando** outro microsserviço consome esse evento
- **Então** o processamento aparece como descendente do mesmo trace distribuído
- **E** `order_id` e `correlation_id` aparecem como atributos pesquisáveis do span

#### AC-016 — Logs estruturados permitem correlação com traces e pedidos

- **Dado** uma requisição ou evento em processamento
- **Quando** o serviço registra uma mensagem de operação
- **Então** o log enviado por OTLP contém `service.name`, severidade, `trace_id` e `span_id`
- **E** inclui `order_id` e `correlation_id` quando esses identificadores existem

### US-005 — Operação enxerga a saúde da resiliência

Como pessoa de operação, quero medir processamento, retries, fallback, DLQ e Outbox, para perceber degradação antes de perder pedidos.

#### AC-017 — Métricas de negócio e resiliência são exportadas

- **Dado** um consumidor processando eventos com sucesso ou falha
- **Quando** ocorre processamento, retry, fallback, envio para DLQ ou drenagem da Outbox
- **Então** contadores OpenTelemetry registram o serviço, tipo do evento e resultado
- **E** as métricas não usam `order_id`, `event_id` ou `correlation_id` como labels de alta cardinalidade

#### AC-018 — Indisponibilidade da observabilidade não interrompe a saga

- **Dado** o backend OTLP indisponível
- **Quando** uma requisição ou evento de negócio é processado
- **Então** o efeito de negócio continua e a telemetria falha de forma assíncrona
- **E** nenhum endpoint de negócio passa a depender da saúde do backend de observabilidade

### US-006 — Pessoa desenvolvedora investiga o sistema localmente

Como pessoa desenvolvedora, quero subir uma interface única com métricas, logs e traces, para estudar o comportamento da saga sem configurar serviços externos.

#### AC-019 — Stack LGTM sobe pronta para receber OTLP

- **Dado** o ambiente Docker Compose local
- **Quando** a stack é iniciada
- **Então** Grafana, Mimir/Prometheus, Loki, Tempo e OpenTelemetry Collector ficam disponíveis no serviço `otel-lgtm`
- **E** os quatro microsserviços exportam OTLP/HTTP para esse serviço

#### AC-020 — Dashboard da saga é provisionado automaticamente

- **Dado** telemetria produzida pelos microsserviços
- **Quando** a pessoa abre o Grafana local
- **Então** encontra um dashboard provisionado com taxa, erros, duração, eventos, retries e DLQ por serviço
- **E** consegue partir do dashboard para consultar logs no Loki e traces no Tempo

#### AC-021 — Execução e investigação estão documentadas

- **Dado** uma pessoa nova no projeto
- **Quando** consulta a documentação operacional
- **Então** encontra comandos de inicialização, URL e credenciais locais do Grafana e um roteiro de investigação por `correlation_id`
- **E** entende que a stack local não define retenção, autenticação ou dimensionamento de produção

## Fora de escopo

- Retenção durável, alta disponibilidade e dimensionamento da stack de telemetria.
- Autenticação, TLS e envio para Grafana Cloud ou outro SaaS.
- SLOs contratuais, plantão e integrações externas de alertas.
- Profiling contínuo e instrumentação do host fora do Docker Compose.

## Suposições

| ID | Suposição | Status | Resolução |
|---|---|---|---|
| ASM-005 | A observabilidade pedida é para desenvolvimento, demonstração e estudo local, não uma topologia de produção. | confirmada | A opção 1A escolhida pelo usuário adota a stack LGTM local completa. |
| ASM-006 | O ambiente local pode amostrar 100% dos traces por ter baixo volume. | confirmada | O perfil local privilegia aprendizado e diagnóstico; a taxa permanecerá configurável por variável de ambiente. |

## Perguntas em aberto

| ID | Pergunta | Status | Resposta |
|---|---|---|---|
| Q-003 | A entrega deve usar LGTM completo ou somente métricas e logs? | respondida | LGTM completo: OpenTelemetry, Grafana, Loki, Tempo e Prometheus/Mimir (opção 1A). |
