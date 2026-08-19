# Plano de execução — fluxo-pedidos

> gerado por `onp-spec plano` em 2026-08-18 19:11 — NÃO edite à mão;
> mudou tasks.md ou a config? Regenere: `onp-spec plano fluxo-pedidos`

## Resumo — o que vai acontecer

- **7 tarefa(s) pendente(s)**: 7 em 7 faixa(s) paralela(s) + 0 sequencial(is)
- **1 faixa = 1 worktree + 1 branch + 1 janela de contexto limpa** — faixas não compartilham nenhum arquivo entre si
- prefere outra seleção ou uma após a outra? Regenere com `onp-spec plano fluxo-pedidos --paralelizar T-xxx,T-yyy` ou `--sequencial`
- tudo acontece na branch de trabalho `spec/fluxo-pedidos`; levar para a main é decisão sua

## Faixas e ondas

### Onda 1 — faixa-1 ∥ faixa-2 ∥ faixa-3

#### faixa-1 — branch `spec/fluxo-pedidos-faixa-1` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-1`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-001 | Fundação, contratos e resiliência compartilhada | `gpt-5.6-terra` | high | `pyproject.toml`, `packages/event_bus/event_bus/__init__.py`, `packages/event_bus/event_bus/envelope.py`, `packages/event_bus/event_bus/retry.py`, `packages/event_bus/event_bus/inbox.py`, `packages/event_bus/event_bus/outbox.py`, `packages/event_bus/event_bus/health.py`, `packages/event_bus/tests/test_envelope.py`, `packages/event_bus/tests/test_retry.py`, `packages/event_bus/tests/test_inbox.py`, `packages/event_bus/tests/test_outbox.py`, `packages/event_bus/tests/test_health.py` |

#### faixa-2 — branch `spec/fluxo-pedidos-faixa-2` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-2`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-002 | API e produtor do serviço de pedidos | `gpt-5.6-terra` | medium | `services/order_service/order_service/__init__.py`, `services/order_service/order_service/main.py`, `services/order_service/order_service/api.py`, `services/order_service/order_service/models.py`, `services/order_service/order_service/producer.py`, `services/order_service/order_service/persistence.py`, `services/order_service/order_service/outbox.py`, `services/order_service/migrations/env.py`, `services/order_service/migrations/versions/001_initial.py`, `services/order_service/tests/test_api.py`, `services/order_service/tests/test_outbox.py`, `services/order_service/tests/test_health.py` |

#### faixa-3 — branch `spec/fluxo-pedidos-faixa-3` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-3`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-003 | Consumidor do serviço de estoque | `gpt-5.6-terra` | high | `services/inventory_service/inventory_service/__init__.py`, `services/inventory_service/inventory_service/main.py`, `services/inventory_service/inventory_service/consumer.py`, `services/inventory_service/inventory_service/handler.py`, `services/inventory_service/inventory_service/adapter.py`, `services/inventory_service/inventory_service/persistence.py`, `services/inventory_service/inventory_service/outbox.py`, `services/inventory_service/migrations/env.py`, `services/inventory_service/migrations/versions/001_initial.py`, `services/inventory_service/tests/test_consumer.py`, `services/inventory_service/tests/test_handler.py`, `services/inventory_service/tests/test_persistence.py`, `services/inventory_service/tests/test_health.py` |

### Onda 2 — faixa-4 ∥ faixa-5 ∥ faixa-6

#### faixa-4 — branch `spec/fluxo-pedidos-faixa-4` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-4`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-004 | Consumidor do serviço de pagamento | `gpt-5.6-terra` | high | `services/payment_service/payment_service/__init__.py`, `services/payment_service/payment_service/main.py`, `services/payment_service/payment_service/consumer.py`, `services/payment_service/payment_service/handler.py`, `services/payment_service/payment_service/adapter.py`, `services/payment_service/payment_service/persistence.py`, `services/payment_service/payment_service/outbox.py`, `services/payment_service/migrations/env.py`, `services/payment_service/migrations/versions/001_initial.py`, `services/payment_service/tests/test_consumer.py`, `services/payment_service/tests/test_handler.py`, `services/payment_service/tests/test_persistence.py`, `services/payment_service/tests/test_health.py` |

#### faixa-5 — branch `spec/fluxo-pedidos-faixa-5` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-5`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-005 | Consumidor do serviço de notificação | `gpt-5.6-terra` | medium | `services/notification_service/notification_service/__init__.py`, `services/notification_service/notification_service/main.py`, `services/notification_service/notification_service/consumer.py`, `services/notification_service/notification_service/handler.py`, `services/notification_service/notification_service/adapter.py`, `services/notification_service/notification_service/persistence.py`, `services/notification_service/migrations/env.py`, `services/notification_service/migrations/versions/001_initial.py`, `services/notification_service/tests/test_consumer.py`, `services/notification_service/tests/test_handler.py`, `services/notification_service/tests/test_persistence.py`, `services/notification_service/tests/test_health.py` |

#### faixa-6 — branch `spec/fluxo-pedidos-faixa-6` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-6`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-006 | Infraestrutura local e teste da saga | `gpt-5.6-terra` | high | `docker-compose.yml`, `Dockerfile`, `.dockerignore`, `.env.example`, `scripts/create-topics.sh`, `scripts/init-databases.sql`, `tests/integration/test_order_saga.py`, `tests/integration/test_retry_and_dlq.py`, `tests/integration/test_outbox_recovery.py` |

### Onda 3 — faixa-7

#### faixa-7 — branch `spec/fluxo-pedidos-faixa-7` — worktree `../onp-worktrees/bukhara-python-fluxo-pedidos-faixa-7`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-007 | Documentação operacional | `gpt-5.6-luna` | low | `README.md`, `docs/events.md`, `docs/resilience.md` |

## Gestão de branches e commits

1. branch de trabalho `spec/fluxo-pedidos` criada do ponto atual (se ainda não existir)
2. cada faixa nasce dela como branch própria e roda no seu worktree — **1 tarefa = 1 commit** (`T-xxx feature: título`)
3. terminou a onda → merge `--no-ff` de cada faixa de volta, na ordem; conflito interrompe a faixa e pede resolução humana
4. faixa mesclada → worktree removido, branch apagada, tarefa marcada `[concluida]` no tasks.md
5. gate final na branch de trabalho: `onp-spec verify fluxo-pedidos` + `onp-spec audit --ci` — **exit 0 ou não está pronto**

## Como executar

### ▶ Execução — Codex headless (codex exec)

```bash
bash .spec/features/fluxo-pedidos/executar-tarefas.sh
```

Cada faixa roda `codex exec` com **janela de contexto limpa**, no seu worktree, com
`--model` e `model_reasoning_effort` já definidos por tarefa e sandbox `workspace-write`. Os prompts exatos estão
embutidos no script — quer rodar uma faixa na mão, é só copiá-los de lá.
Logs: `../onp-worktrees/bukhara-python-fluxo-pedidos-logs/`.

**Confirmação de custos — antes de executar**: os modelos e esforços por
tarefa estão nas tabelas acima; o agente CONFIRMA com o usuário se estão
dentro da licença/cota dele (modelo forte + esforço alto torra tokens).
Para gastar menos: `onp-spec plano fluxo-pedidos --modelo gpt-5.6-luna --esforco baixo`
(tudo) ou por tarefa `onp-spec tarefa fluxo-pedidos T-xxx --modelo <m> --esforco <nível>` — e regenere o plano.

### 📣 Acompanhamento — tabela + resumo no chat (a cada 1 min)

O script roda em **background**: o agente AVISA o usuário antes de iniciar e,
enquanto roda, posta no chat a cada ~1 minuto a **tabela de andamento** (qual
tarefa está rodando, qual não está, o que concluiu/falhou) junto com o
**resumo geral de andamento** (escrito por IA; sem IA, o motor resume). Ao
final, o usuário recebe o resumo completo da execução. A qualquer momento:

```bash
onp-spec resumo fluxo-pedidos --tabela   # a tabela de andamento
onp-spec resumo fluxo-pedidos            # o resumo em texto
```

