# Plano de execução — observabilidade

> gerado por `onp-spec plano` em 2026-08-19 00:30 — NÃO edite à mão;
> mudou tasks.md ou a config? Regenere: `onp-spec plano observabilidade`

## Resumo — o que vai acontecer

- **4 tarefa(s) pendente(s)**: 4 em 3 faixa(s) paralela(s) + 0 sequencial(is)
- **1 faixa = 1 worktree + 1 branch + 1 janela de contexto limpa** — faixas não compartilham nenhum arquivo entre si
- prefere outra seleção ou uma após a outra? Regenere com `onp-spec plano observabilidade --paralelizar T-xxx,T-yyy` ou `--sequencial`
- tudo acontece na branch de trabalho `spec/observabilidade`; levar para a main é decisão sua

## Faixas e ondas

### Onda 1 — faixa-1 ∥ faixa-2 ∥ faixa-3

#### faixa-1 — branch `spec/observabilidade-faixa-1` — worktree `../onp-worktrees/bukhara-python-observabilidade-faixa-1`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-008 | Biblioteca compartilhada e contratos de telemetria | `gpt-5.6-terra` | high | `pyproject.toml`, `packages/observability/observability/__init__.py`, `packages/observability/observability/telemetry.py`, `packages/observability/observability/logging.py`, `packages/observability/tests/test_telemetry.py`, `packages/observability/tests/test_logging.py` |
| T-009 | Instrumentação dos quatro microsserviços | `gpt-5.6-terra` | high | `packages/observability/observability/telemetry.py`, `services/order_service/order_service/main.py`, `services/order_service/order_service/api.py`, `services/order_service/order_service/producer.py`, `services/order_service/tests/test_observability.py`, `services/inventory_service/inventory_service/main.py`, `services/inventory_service/tests/test_observability.py`, `services/payment_service/payment_service/main.py`, `services/payment_service/tests/test_observability.py`, `services/notification_service/notification_service/main.py`, `services/notification_service/notification_service/consumer.py`, `services/notification_service/tests/test_observability.py` |

#### faixa-2 — branch `spec/observabilidade-faixa-2` — worktree `../onp-worktrees/bukhara-python-observabilidade-faixa-2`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-010 | Stack LGTM e dashboard provisionado | `gpt-5.6-terra` | high | `docker-compose.yml`, `Dockerfile`, `.env.example`, `observability/grafana/dashboards.yaml`, `observability/grafana/order-saga.json`, `tests/integration/test_observability_stack.py` |

#### faixa-3 — branch `spec/observabilidade-faixa-3` — worktree `../onp-worktrees/bukhara-python-observabilidade-faixa-3`

| tarefa | título | modelo | esforço | arquivos |
|---|---|---|---|---|
| T-011 | Runbook de investigação | `gpt-5.6-luna` | low | `README.md`, `docs/observability.md`, `tests/integration/test_observability_docs.py` |

## Gestão de branches e commits

1. branch de trabalho `spec/observabilidade` criada do ponto atual (se ainda não existir)
2. cada faixa nasce dela como branch própria e roda no seu worktree — **1 tarefa = 1 commit** (`T-xxx feature: título`)
3. terminou a onda → merge `--no-ff` de cada faixa de volta, na ordem; conflito interrompe a faixa e pede resolução humana
4. faixa mesclada → worktree removido, branch apagada, tarefa marcada `[concluida]` no tasks.md
5. gate final na branch de trabalho: `onp-spec verify observabilidade` + `onp-spec audit --ci` — **exit 0 ou não está pronto**

## Como executar

### ▶ Execução — Codex headless (codex exec)

```bash
bash .spec/features/observabilidade/executar-tarefas.sh
```

Cada faixa roda `codex exec` com **janela de contexto limpa**, no seu worktree, com
`--model` e `model_reasoning_effort` já definidos por tarefa e sandbox `workspace-write`. Os prompts exatos estão
embutidos no script — quer rodar uma faixa na mão, é só copiá-los de lá.
Logs: `../onp-worktrees/bukhara-python-observabilidade-logs/`.

**Confirmação de custos — antes de executar**: os modelos e esforços por
tarefa estão nas tabelas acima; o agente CONFIRMA com o usuário se estão
dentro da licença/cota dele (modelo forte + esforço alto torra tokens).
Para gastar menos: `onp-spec plano observabilidade --modelo gpt-5.6-luna --esforco baixo`
(tudo) ou por tarefa `onp-spec tarefa observabilidade T-xxx --modelo <m> --esforco <nível>` — e regenere o plano.

### 📣 Acompanhamento — tabela + resumo no chat (a cada 1 min)

O script roda em **background**: o agente AVISA o usuário antes de iniciar e,
enquanto roda, posta no chat a cada ~1 minuto a **tabela de andamento** (qual
tarefa está rodando, qual não está, o que concluiu/falhou) junto com o
**resumo geral de andamento** (escrito por IA; sem IA, o motor resume). Ao
final, o usuário recebe o resumo completo da execução. A qualquer momento:

```bash
onp-spec resumo observabilidade --tabela   # a tabela de andamento
onp-spec resumo observabilidade            # o resumo em texto
```

