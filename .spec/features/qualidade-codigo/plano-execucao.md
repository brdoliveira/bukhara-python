# Plano de execução — qualidade-codigo

> gerado por `onp-spec plano` em 2026-08-19 19:07 — NÃO edite à mão;
> mudou tasks.md ou a config? Regenere: `onp-spec plano qualidade-codigo --paralelizar T-013,T-014,T-015,T-016,T-017`

## Resumo — o que vai acontecer

- **6 tarefa(s) pendente(s)**: 5 em 5 faixa(s) paralela(s) + 1 sequencial(is)
- **seleção do usuário**: paralelizar só T-013, T-014, T-015, T-016, T-017 — as demais rodam uma após a outra, ao final
- **1 faixa = 1 worktree + 1 branch + 1 janela de contexto limpa** — faixas não compartilham nenhum arquivo entre si
- prefere outra seleção ou uma após a outra? Regenere com `onp-spec plano qualidade-codigo --paralelizar T-xxx,T-yyy` ou `--sequencial`
- tudo acontece na branch de trabalho `spec/qualidade-codigo`; levar para a main é decisão sua

## Faixas e ondas

### Onda 1 — faixa-1 ∥ faixa-2 ∥ faixa-3

#### faixa-1 — branch `spec/qualidade-codigo-faixa-1` — worktree `../onp-worktrees/bukhara-python-qualidade-codigo-faixa-1`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-013 | Documentar e testar pacotes compartilhados | `gpt-5.6-terra` | high | `packages/event_bus/event_bus/envelope.py`, `packages/event_bus/event_bus/health.py`, `packages/event_bus/event_bus/inbox.py`, `packages/event_bus/event_bus/outbox.py`, `packages/event_bus/event_bus/retry.py`, `packages/event_bus/tests/test_envelope.py`, `packages/event_bus/tests/test_health.py`, `packages/event_bus/tests/test_inbox.py`, `packages/event_bus/tests/test_outbox.py`, `packages/event_bus/tests/test_retry.py`, `packages/observability/observability/logging.py`, `packages/observability/observability/telemetry.py`, `packages/observability/tests/test_logging.py`, `packages/observability/tests/test_telemetry.py` |

#### faixa-2 — branch `spec/qualidade-codigo-faixa-2` — worktree `../onp-worktrees/bukhara-python-qualidade-codigo-faixa-2`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-014 | Documentar e testar o serviço de pedidos | `gpt-5.6-terra` | high | `services/order_service/order_service/api.py`, `services/order_service/order_service/main.py`, `services/order_service/order_service/models.py`, `services/order_service/order_service/outbox.py`, `services/order_service/order_service/persistence.py`, `services/order_service/order_service/producer.py`, `services/order_service/tests/test_api.py`, `services/order_service/tests/test_health.py`, `services/order_service/tests/test_observability.py`, `services/order_service/tests/test_outbox.py`, `services/order_service/tests/test_recovery.py` |

#### faixa-3 — branch `spec/qualidade-codigo-faixa-3` — worktree `../onp-worktrees/bukhara-python-qualidade-codigo-faixa-3`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-015 | Documentar e testar o serviço de estoque | `gpt-5.6-terra` | high | `services/inventory_service/inventory_service/consumer.py`, `services/inventory_service/inventory_service/handler.py`, `services/inventory_service/inventory_service/main.py`, `services/inventory_service/inventory_service/outbox.py`, `services/inventory_service/inventory_service/persistence.py`, `services/inventory_service/tests/test_consumer.py`, `services/inventory_service/tests/test_handler.py`, `services/inventory_service/tests/test_health.py`, `services/inventory_service/tests/test_observability.py`, `services/inventory_service/tests/test_persistence.py`, `services/inventory_service/tests/test_runtime.py` |

### Onda 2 — faixa-4 ∥ faixa-5

#### faixa-4 — branch `spec/qualidade-codigo-faixa-4` — worktree `../onp-worktrees/bukhara-python-qualidade-codigo-faixa-4`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-016 | Documentar e testar o serviço de pagamento | `gpt-5.6-terra` | high | `services/payment_service/payment_service/consumer.py`, `services/payment_service/payment_service/handler.py`, `services/payment_service/payment_service/main.py`, `services/payment_service/payment_service/outbox.py`, `services/payment_service/payment_service/persistence.py`, `services/payment_service/tests/test_consumer.py`, `services/payment_service/tests/test_handler.py`, `services/payment_service/tests/test_health.py`, `services/payment_service/tests/test_observability.py`, `services/payment_service/tests/test_persistence.py`, `services/payment_service/tests/test_runtime.py` |

#### faixa-5 — branch `spec/qualidade-codigo-faixa-5` — worktree `../onp-worktrees/bukhara-python-qualidade-codigo-faixa-5`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-017 | Documentar e testar o serviço de notificação | `gpt-5.6-terra` | high | `services/notification_service/notification_service/adapter.py`, `services/notification_service/notification_service/consumer.py`, `services/notification_service/notification_service/handler.py`, `services/notification_service/notification_service/main.py`, `services/notification_service/notification_service/persistence.py`, `services/notification_service/tests/test_consumer.py`, `services/notification_service/tests/test_handler.py`, `services/notification_service/tests/test_health.py`, `services/notification_service/tests/test_observability.py`, `services/notification_service/tests/test_persistence.py`, `services/notification_service/tests/test_worker.py` |

## Tarefas sequenciais (após as ondas, na árvore principal)

| tarefa | título | modelo | esforço | por que sequencial |
|---|---|---|---|---|
| T-018 | Publicar referência e ativar gate de cobertura | `gpt-5.6-luna` | low | fora da seleção do usuário |

## Gestão de branches e commits

1. branch de trabalho `spec/qualidade-codigo` criada do ponto atual (se ainda não existir)
2. cada faixa nasce dela como branch própria e roda no seu worktree — **1 tarefa = 1 commit** (`T-xxx feature: título`)
3. terminou a onda → merge `--no-ff` de cada faixa de volta, na ordem; conflito interrompe a faixa e pede resolução humana
4. faixa mesclada → worktree removido, branch apagada, tarefa marcada `[concluida]` no tasks.md
5. gate final na branch de trabalho: `onp-spec verify qualidade-codigo` + `onp-spec audit --ci` — **exit 0 ou não está pronto**

## Como executar

### ▶ Execução — Codex headless (codex exec)

```bash
bash .spec/features/qualidade-codigo/executar-tarefas.sh
```

Cada faixa roda `codex exec` com **janela de contexto limpa**, no seu worktree, com
`--model` e `model_reasoning_effort` já definidos por tarefa e sandbox `workspace-write`. Os prompts exatos estão
embutidos no script — quer rodar uma faixa na mão, é só copiá-los de lá.
Logs: `../onp-worktrees/bukhara-python-qualidade-codigo-logs/`.

**Confirmação de custos — antes de executar**: os modelos e esforços por
tarefa estão nas tabelas acima; o agente CONFIRMA com o usuário se estão
dentro da licença/cota dele (modelo forte + esforço alto torra tokens).
Para gastar menos: `onp-spec plano qualidade-codigo --modelo gpt-5.6-luna --esforco baixo`
(tudo) ou por tarefa `onp-spec tarefa qualidade-codigo T-xxx --modelo <m> --esforco <nível>` — e regenere o plano.

### 📣 Acompanhamento — tabela + resumo no chat (a cada 1 min)

O script roda em **background**: o agente AVISA o usuário antes de iniciar e,
enquanto roda, posta no chat a cada ~1 minuto a **tabela de andamento** (qual
tarefa está rodando, qual não está, o que concluiu/falhou) junto com o
**resumo geral de andamento** (escrito por IA; sem IA, o motor resume). Ao
final, o usuário recebe o resumo completo da execução. A qualquer momento:

```bash
onp-spec resumo qualidade-codigo --tabela   # a tabela de andamento
onp-spec resumo qualidade-codigo            # o resumo em texto
```

