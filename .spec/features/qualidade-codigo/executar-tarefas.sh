#!/usr/bin/env bash
# executar-tarefas.sh — gerado por `onp-spec plano qualidade-codigo` em 2026-08-19 19:07
# NÃO edite à mão: mudou tasks.md ou a config, regenere o plano.
#
# uso:
#   bash executar-tarefas.sh                  tudo (ondas → sequenciais → gate)
#   bash executar-tarefas.sh --faixa <id>     reexecuta UMA faixa (+ merge + gate)
#   bash executar-tarefas.sh --seq <T-xxx>    reexecuta UMA tarefa sequencial
#   bash executar-tarefas.sh --gate           só o gate (verify + audit)
#   bash executar-tarefas.sh --listar         mostra faixas, tarefas e estados
#   (acrescente --sem-gate para não rodar o gate ao final)
#
# resumo do que está rolando, a qualquer momento: onp-spec resumo qualidade-codigo
set -u
set -o pipefail

RUN_ID='bukhara-python-qualidade-codigo-mt0grh2r'
FEATURE='qualidade-codigo'
BASE_BRANCH='spec/qualidade-codigo'
ENGINE='C:\Users\brufe\.agents\skills\onp-spec-driven\scripts\onp-spec.mjs'
CODEX_FLAGS=(--sandbox 'workspace-write')
STREAM_FLAGS=(--json)
FALHAS=""
COM_GATE=1
RESUMO_MODEL='gpt-5.6-luna'
RESUMO_PID=""

verde()    { printf '\033[32m%s\033[0m\n' "$*"; }
vermelho() { printf '\033[31m%s\033[0m\n' "$*"; }
amarelo()  { printf '\033[33m%s\033[0m\n' "$*"; }
info()     { printf '· %s\n' "$*"; }
falhar()   { vermelho "✘ $*"; exit 1; }

# eventos vão para o ledger GLOBAL (~/.onp-spec/painel/ledger.jsonl):
# um arquivo para todos os projetos, é o que o onp-spec resumo lê
evento() { node "$ENGINE" evento --run "$RUN_ID" "$@" >/dev/null 2>&1 || true; }

# ── ambiente (todos os modos passam por aqui) ────────────────────────
preparar_ambiente() {
  command -v git >/dev/null 2>&1 || falhar "git não encontrado"
  command -v node >/dev/null 2>&1 || falhar "node não encontrado"
  command -v codex >/dev/null 2>&1 || falhar "Codex CLI (codex) não encontrado — instale-o ou siga o modo manual em plano-execucao.md"
  TOPLEVEL=$(git rev-parse --show-toplevel 2>/dev/null) || falhar "fora de um repositório git"
  cd "$TOPLEVEL" || exit 1
  # artefatos recém-gerados pelo `onp-spec plano` são sujeira esperada:
  # se forem a ÚNICA sujeira, o script mesmo commita; qualquer outra, aborta
  if [ -n "$(git status --porcelain)" ]; then
    if [ -z "$(git status --porcelain | grep -v -e 'plano-execucao\.' -e 'plano\.json' -e 'executar-tarefas\.sh')" ]; then
      git add -A
      git commit -q -m "plano de execução: $FEATURE (artefatos gerados)"
      info "artefatos do plano commitados"
    else
      falhar "árvore suja além dos artefatos do plano — commite ou faça git stash antes (os worktrees partem do último commit)"
    fi
  fi
  git ls-files --error-unmatch -- '.spec/features/qualidade-codigo/spec.md' >/dev/null 2>&1 || falhar "spec.md não está commitada — os worktrees das faixas precisam dela no git"
  ATUAL=$(git rev-parse --abbrev-ref HEAD)
  [ "$ATUAL" != "HEAD" ] || falhar "HEAD destacado — troque para uma branch"
  if [ "$ATUAL" != "$BASE_BRANCH" ]; then
    if git show-ref --verify --quiet "refs/heads/$BASE_BRANCH"; then
      git checkout -q "$BASE_BRANCH" || falhar "não consegui trocar para $BASE_BRANCH"
    else
      git checkout -q -b "$BASE_BRANCH" || falhar "não consegui criar $BASE_BRANCH"
    fi
    info "branch de trabalho: $BASE_BRANCH (a partir de $ATUAL)"
  fi
  git worktree prune
  LOG_DIR="$(dirname "$TOPLEVEL")/onp-worktrees/bukhara-python-qualidade-codigo-logs"
  WT_BASE="$(dirname "$TOPLEVEL")/onp-worktrees/bukhara-python-qualidade-codigo"
  STREAMS_DIR="${ONP_SPEC_HOME:-$HOME/.onp-spec}/painel/streams/$RUN_ID"
  mkdir -p "$LOG_DIR" "$STREAMS_DIR"
}

# worktree limpo mesmo depois de uma tentativa que falhou
preparar_worktree() { # $1=faixa $2=branch $3=worktree
  git worktree prune
  if [ -e "$3" ]; then git worktree remove --force "$3" >/dev/null 2>&1; rm -rf "$3"; fi
  if git show-ref --verify --quiet "refs/heads/$2"; then git branch -D "$2" >/dev/null 2>&1; fi
  git worktree add "$3" -b "$2" >/dev/null 2>&1 || { vermelho "✘ não consegui criar o worktree de $1 em $3"; return 1; }
}

tentativa() { # $1=faixa — conta reexecuções (vai para o ledger)
  local arq="$LOG_DIR/.tentativa-$1"
  local n=1
  [ -f "$arq" ] && n=$(( $(cat "$arq") + 1 ))
  printf "%s" "$n" > "$arq"
  printf "%s" "$n"
}

# uma tarefa = uma sessão codex exec headless com contexto limpo.
# o JSONL da sessão vira o stream da tarefa no ledger
rodar_tarefa() { # $1=escopo(faixa|seq) $2=T-xxx $3=prompt $4=modelo $5=esforço
  local chave="$1--$2"
  local stream="$STREAMS_DIR/$chave.jsonl"
  evento --tipo tarefa --tarefa "$2" --faixa "$1" --estado executando --stream "$chave"
  info "$2 — codex exec ($4 · $5) · stream: $chave"
  # --add-dir: o .git compartilhado dos worktrees mora no repo principal —
  # sem ele o sandbox workspace-write bloquearia o commit da tarefa
  if codex exec "$3" --model "$4" -c model_reasoning_effort="$5" "${STREAM_FLAGS[@]}" "${CODEX_FLAGS[@]}" --add-dir "$TOPLEVEL" > "$stream" 2>>"$LOG_DIR/$1.log"; then
    evento --tipo tarefa --tarefa "$2" --faixa "$1" --estado concluida --stream "$chave"
    node "$ENGINE" stream-resumo "$RUN_ID" "$chave" 2>/dev/null || true
    return 0
  fi
  evento --tipo tarefa --tarefa "$2" --faixa "$1" --estado falhou --stream "$chave"
  node "$ENGINE" stream-resumo "$RUN_ID" "$chave" 2>/dev/null || true
  return 1
}

mesclar_faixa() { # $1=faixa $2=branch $3=worktree $4=exit-da-faixa
  if [ "$4" -ne 0 ]; then
    evento --tipo faixa --faixa "$1" --estado falhou
    vermelho "✘ $1 falhou (log: $LOG_DIR/$1.log) — worktree mantido para inspeção: $3"
    amarelo "  reexecute só ela: bash .spec/features/qualidade-codigo/executar-tarefas.sh --faixa $1"
    FALHAS="$FALHAS $1"; return 1
  fi
  evento --tipo faixa --faixa "$1" --estado mesclando
  if git merge --no-ff "$2" -m "merge $1 ($FEATURE)"; then
    git worktree remove --force "$3" >/dev/null 2>&1
    git branch -d "$2" >/dev/null 2>&1
    evento --tipo faixa --faixa "$1" --estado mesclada
    verde "✔ $1 mesclada em $BASE_BRANCH"
  else
    git merge --abort >/dev/null 2>&1
    evento --tipo faixa --faixa "$1" --estado conflito
    vermelho "✘ conflito ao mesclar $1 — resolva na mão: git merge $2 (worktree mantido: $3)"
    FALHAS="$FALHAS $1"; return 1
  fi
}

marcar_concluidas() { # $@=T-xxx
  for t in "$@"; do node "$ENGINE" tarefa "$FEATURE" "$t" concluida >/dev/null || true; done
}

# ── resumo geral de andamento: 1/min enquanto a execução roda ─────────
# escrito por IA (codex exec somente leitura) com fallback do motor; vai
# para o terminal e para o ledger — o agente repassa o texto no chat.
gerar_resumo() {
  local ctx ia
  ctx=$(node "$ENGINE" resumo "$FEATURE" --contexto 2>/dev/null) || ctx=""
  [ -n "$ctx" ] || return 0
  ia=$(codex exec "Você narra, para o dono do produto, uma execução de tarefas de código em andamento. Estado mecânico:

$ctx

Escreva o RESUMO GERAL DE ANDAMENTO: um parágrafo único de 2 a 4 frases, em português simples, dizendo o que está acontecendo agora, o que já terminou, o que falhou e se o usuário precisa agir. Sem markdown, sem listas." --model "$RESUMO_MODEL" --sandbox read-only --ephemeral 2>/dev/null)
  if [ -n "$ia" ]; then
    node "$ENGINE" resumo "$FEATURE" --gravar --origem ia --texto "$ia" >/dev/null 2>&1 || true
    printf '\n📣 resumo (IA): %s\n' "$ia"
  else
    node "$ENGINE" resumo "$FEATURE" --gravar >/dev/null 2>&1 || true
    printf '\n📣 resumo: %s\n' "$(node "$ENGINE" resumo "$FEATURE" 2>/dev/null)"
  fi
}

# mata o loop E o sleep filho — senão o sleep herda o stdout e quem chamou
# o script via pipe fica esperando EOF por até 60s depois do exit
parar_resumos() {
  [ -n "$RESUMO_PID" ] || return 0
  command -v pkill >/dev/null 2>&1 && pkill -P "$RESUMO_PID" 2>/dev/null
  kill "$RESUMO_PID" 2>/dev/null
  RESUMO_PID=""
}

iniciar_resumos() {
  ( while :; do sleep 60; gerar_resumo; done ) &
  RESUMO_PID=$!
  # ao sair: para o loop e grava um último resumo (o estado final, do motor)
  trap 'parar_resumos; node "$ENGINE" resumo "$FEATURE" --gravar >/dev/null 2>&1 || true' EXIT
}

# ── faixa-1: T-013 ──
executar_faixa_1() {
  local WT="$WT_BASE-faixa-1"
  preparar_worktree 'faixa-1' 'spec/qualidade-codigo-faixa-1' "$WT" || return 1
  evento --tipo faixa --faixa 'faixa-1' --estado executando --tentativa "$(tentativa 'faixa-1')"
  : > "$LOG_DIR/faixa-1.log"
  (
    cd "$WT" || exit 9
    rodar_tarefa 'faixa-1' 'T-013' 'Você executa UMA tarefa da feature "qualidade-codigo" (fluxo onp-spec, spec-anchored).
Leia primeiro: .spec/features/qualidade-codigo/spec.md, .spec/features/qualidade-codigo/tasks.md e .spec/constituicao.md.

Sua tarefa (somente ela):
T-013 — "Documentar e testar pacotes compartilhados"
  critérios/refs: AC-024 (APIs públicas possuem documentação verificável), AC-026 (Pacotes compartilhados cobrem seus casos-limite)
  arquivos permitidos (e seus testes): packages/event_bus/event_bus/envelope.py, packages/event_bus/event_bus/health.py, packages/event_bus/event_bus/inbox.py, packages/event_bus/event_bus/outbox.py, packages/event_bus/event_bus/retry.py, packages/event_bus/tests/test_envelope.py, packages/event_bus/tests/test_health.py, packages/event_bus/tests/test_inbox.py, packages/event_bus/tests/test_outbox.py, packages/event_bus/tests/test_retry.py, packages/observability/observability/logging.py, packages/observability/observability/telemetry.py, packages/observability/tests/test_logging.py, packages/observability/tests/test_telemetry.py
  mensagem de commit: "T-013 qualidade-codigo: Documentar e testar pacotes compartilhados"

Regras inegociáveis:
- Todo critério de aceite referenciado vira teste com @spec:AC-xxx no título.
- NUNCA enfraqueça, pule (skip/todo) ou apague um teste para passar — teste pulado não é prova e o audit acusa.
- Rode os testes localmente com `py -3.12 -B -m pytest -q -p no:cacheprovider` até passarem.
- NÃO edite tasks.md, NÃO rode onp-spec verify/audit e NÃO toque em outras tarefas — o orquestrador cuida disso.
- Ao final de CADA tarefa: `git add` só no que você tocou e um commit próprio.' 'gpt-5.6-terra' high
  ) >> "$LOG_DIR/faixa-1.log" 2>&1
  local st=$?
  mesclar_faixa 'faixa-1' 'spec/qualidade-codigo-faixa-1' "$WT" "$st" || return 1
  marcar_concluidas T-013
  return 0
}

# ── faixa-2: T-014 ──
executar_faixa_2() {
  local WT="$WT_BASE-faixa-2"
  preparar_worktree 'faixa-2' 'spec/qualidade-codigo-faixa-2' "$WT" || return 1
  evento --tipo faixa --faixa 'faixa-2' --estado executando --tentativa "$(tentativa 'faixa-2')"
  : > "$LOG_DIR/faixa-2.log"
  (
    cd "$WT" || exit 9
    rodar_tarefa 'faixa-2' 'T-014' 'Você executa UMA tarefa da feature "qualidade-codigo" (fluxo onp-spec, spec-anchored).
Leia primeiro: .spec/features/qualidade-codigo/spec.md, .spec/features/qualidade-codigo/tasks.md e .spec/constituicao.md.

Sua tarefa (somente ela):
T-014 — "Documentar e testar o serviço de pedidos"
  critérios/refs: AC-024 (APIs públicas possuem documentação verificável), AC-027 (Serviço de pedidos cobre recuperação e erros)
  arquivos permitidos (e seus testes): services/order_service/order_service/api.py, services/order_service/order_service/main.py, services/order_service/order_service/models.py, services/order_service/order_service/outbox.py, services/order_service/order_service/persistence.py, services/order_service/order_service/producer.py, services/order_service/tests/test_api.py, services/order_service/tests/test_health.py, services/order_service/tests/test_observability.py, services/order_service/tests/test_outbox.py, services/order_service/tests/test_recovery.py
  mensagem de commit: "T-014 qualidade-codigo: Documentar e testar o serviço de pedidos"

Regras inegociáveis:
- Todo critério de aceite referenciado vira teste com @spec:AC-xxx no título.
- NUNCA enfraqueça, pule (skip/todo) ou apague um teste para passar — teste pulado não é prova e o audit acusa.
- Rode os testes localmente com `py -3.12 -B -m pytest -q -p no:cacheprovider` até passarem.
- NÃO edite tasks.md, NÃO rode onp-spec verify/audit e NÃO toque em outras tarefas — o orquestrador cuida disso.
- Ao final de CADA tarefa: `git add` só no que você tocou e um commit próprio.' 'gpt-5.6-terra' high
  ) >> "$LOG_DIR/faixa-2.log" 2>&1
  local st=$?
  mesclar_faixa 'faixa-2' 'spec/qualidade-codigo-faixa-2' "$WT" "$st" || return 1
  marcar_concluidas T-014
  return 0
}

# ── faixa-3: T-015 ──
executar_faixa_3() {
  local WT="$WT_BASE-faixa-3"
  preparar_worktree 'faixa-3' 'spec/qualidade-codigo-faixa-3' "$WT" || return 1
  evento --tipo faixa --faixa 'faixa-3' --estado executando --tentativa "$(tentativa 'faixa-3')"
  : > "$LOG_DIR/faixa-3.log"
  (
    cd "$WT" || exit 9
    rodar_tarefa 'faixa-3' 'T-015' 'Você executa UMA tarefa da feature "qualidade-codigo" (fluxo onp-spec, spec-anchored).
Leia primeiro: .spec/features/qualidade-codigo/spec.md, .spec/features/qualidade-codigo/tasks.md e .spec/constituicao.md.

Sua tarefa (somente ela):
T-015 — "Documentar e testar o serviço de estoque"
  critérios/refs: AC-024 (APIs públicas possuem documentação verificável), AC-028 (Serviço de estoque cobre runtime e persistência)
  arquivos permitidos (e seus testes): services/inventory_service/inventory_service/consumer.py, services/inventory_service/inventory_service/handler.py, services/inventory_service/inventory_service/main.py, services/inventory_service/inventory_service/outbox.py, services/inventory_service/inventory_service/persistence.py, services/inventory_service/tests/test_consumer.py, services/inventory_service/tests/test_handler.py, services/inventory_service/tests/test_health.py, services/inventory_service/tests/test_observability.py, services/inventory_service/tests/test_persistence.py, services/inventory_service/tests/test_runtime.py
  mensagem de commit: "T-015 qualidade-codigo: Documentar e testar o serviço de estoque"

Regras inegociáveis:
- Todo critério de aceite referenciado vira teste com @spec:AC-xxx no título.
- NUNCA enfraqueça, pule (skip/todo) ou apague um teste para passar — teste pulado não é prova e o audit acusa.
- Rode os testes localmente com `py -3.12 -B -m pytest -q -p no:cacheprovider` até passarem.
- NÃO edite tasks.md, NÃO rode onp-spec verify/audit e NÃO toque em outras tarefas — o orquestrador cuida disso.
- Ao final de CADA tarefa: `git add` só no que você tocou e um commit próprio.' 'gpt-5.6-terra' high
  ) >> "$LOG_DIR/faixa-3.log" 2>&1
  local st=$?
  mesclar_faixa 'faixa-3' 'spec/qualidade-codigo-faixa-3' "$WT" "$st" || return 1
  marcar_concluidas T-015
  return 0
}

# ── faixa-4: T-016 ──
executar_faixa_4() {
  local WT="$WT_BASE-faixa-4"
  preparar_worktree 'faixa-4' 'spec/qualidade-codigo-faixa-4' "$WT" || return 1
  evento --tipo faixa --faixa 'faixa-4' --estado executando --tentativa "$(tentativa 'faixa-4')"
  : > "$LOG_DIR/faixa-4.log"
  (
    cd "$WT" || exit 9
    rodar_tarefa 'faixa-4' 'T-016' 'Você executa UMA tarefa da feature "qualidade-codigo" (fluxo onp-spec, spec-anchored).
Leia primeiro: .spec/features/qualidade-codigo/spec.md, .spec/features/qualidade-codigo/tasks.md e .spec/constituicao.md.

Sua tarefa (somente ela):
T-016 — "Documentar e testar o serviço de pagamento"
  critérios/refs: AC-024 (APIs públicas possuem documentação verificável), AC-029 (Serviço de pagamento cobre runtime e persistência)
  arquivos permitidos (e seus testes): services/payment_service/payment_service/consumer.py, services/payment_service/payment_service/handler.py, services/payment_service/payment_service/main.py, services/payment_service/payment_service/outbox.py, services/payment_service/payment_service/persistence.py, services/payment_service/tests/test_consumer.py, services/payment_service/tests/test_handler.py, services/payment_service/tests/test_health.py, services/payment_service/tests/test_observability.py, services/payment_service/tests/test_persistence.py, services/payment_service/tests/test_runtime.py
  mensagem de commit: "T-016 qualidade-codigo: Documentar e testar o serviço de pagamento"

Regras inegociáveis:
- Todo critério de aceite referenciado vira teste com @spec:AC-xxx no título.
- NUNCA enfraqueça, pule (skip/todo) ou apague um teste para passar — teste pulado não é prova e o audit acusa.
- Rode os testes localmente com `py -3.12 -B -m pytest -q -p no:cacheprovider` até passarem.
- NÃO edite tasks.md, NÃO rode onp-spec verify/audit e NÃO toque em outras tarefas — o orquestrador cuida disso.
- Ao final de CADA tarefa: `git add` só no que você tocou e um commit próprio.' 'gpt-5.6-terra' high
  ) >> "$LOG_DIR/faixa-4.log" 2>&1
  local st=$?
  mesclar_faixa 'faixa-4' 'spec/qualidade-codigo-faixa-4' "$WT" "$st" || return 1
  marcar_concluidas T-016
  return 0
}

# ── faixa-5: T-017 ──
executar_faixa_5() {
  local WT="$WT_BASE-faixa-5"
  preparar_worktree 'faixa-5' 'spec/qualidade-codigo-faixa-5' "$WT" || return 1
  evento --tipo faixa --faixa 'faixa-5' --estado executando --tentativa "$(tentativa 'faixa-5')"
  : > "$LOG_DIR/faixa-5.log"
  (
    cd "$WT" || exit 9
    rodar_tarefa 'faixa-5' 'T-017' 'Você executa UMA tarefa da feature "qualidade-codigo" (fluxo onp-spec, spec-anchored).
Leia primeiro: .spec/features/qualidade-codigo/spec.md, .spec/features/qualidade-codigo/tasks.md e .spec/constituicao.md.

Sua tarefa (somente ela):
T-017 — "Documentar e testar o serviço de notificação"
  critérios/refs: AC-024 (APIs públicas possuem documentação verificável), AC-030 (Serviço de notificação cobre worker e falhas)
  arquivos permitidos (e seus testes): services/notification_service/notification_service/adapter.py, services/notification_service/notification_service/consumer.py, services/notification_service/notification_service/handler.py, services/notification_service/notification_service/main.py, services/notification_service/notification_service/persistence.py, services/notification_service/tests/test_consumer.py, services/notification_service/tests/test_handler.py, services/notification_service/tests/test_health.py, services/notification_service/tests/test_observability.py, services/notification_service/tests/test_persistence.py, services/notification_service/tests/test_worker.py
  mensagem de commit: "T-017 qualidade-codigo: Documentar e testar o serviço de notificação"

Regras inegociáveis:
- Todo critério de aceite referenciado vira teste com @spec:AC-xxx no título.
- NUNCA enfraqueça, pule (skip/todo) ou apague um teste para passar — teste pulado não é prova e o audit acusa.
- Rode os testes localmente com `py -3.12 -B -m pytest -q -p no:cacheprovider` até passarem.
- NÃO edite tasks.md, NÃO rode onp-spec verify/audit e NÃO toque em outras tarefas — o orquestrador cuida disso.
- Ao final de CADA tarefa: `git add` só no que você tocou e um commit próprio.' 'gpt-5.6-terra' high
  ) >> "$LOG_DIR/faixa-5.log" 2>&1
  local st=$?
  mesclar_faixa 'faixa-5' 'spec/qualidade-codigo-faixa-5' "$WT" "$st" || return 1
  marcar_concluidas T-017
  return 0
}

# ── sequencial T-018 (fora da seleção do usuário) ──
executar_seq_T_018() {
  info 'sequencial T-018 — Publicar referência e ativar gate de cobertura'
  if rodar_tarefa seq 'T-018' 'Você executa UMA tarefa da feature "qualidade-codigo" (fluxo onp-spec, spec-anchored).
Leia primeiro: .spec/features/qualidade-codigo/spec.md, .spec/features/qualidade-codigo/tasks.md e .spec/constituicao.md.

Sua tarefa (somente ela):
T-018 — "Publicar referência e ativar gate de cobertura"
  critérios/refs: AC-024 (APIs públicas possuem documentação verificável), AC-025 (Guia de referência orienta a navegação), AC-031 (Cobertura mínima impede regressão silenciosa)
  arquivos permitidos (e seus testes): pyproject.toml, README.md, docs/code-reference.md, tests/integration/test_code_documentation.py, tests/integration/test_quality_gate.py
  mensagem de commit: "T-018 qualidade-codigo: Publicar referência e ativar gate de cobertura"

Regras inegociáveis:
- Todo critério de aceite referenciado vira teste com @spec:AC-xxx no título.
- NUNCA enfraqueça, pule (skip/todo) ou apague um teste para passar — teste pulado não é prova e o audit acusa.
- Rode os testes localmente com `py -3.12 -B -m pytest -q -p no:cacheprovider` até passarem.
- NÃO edite tasks.md, NÃO rode onp-spec verify/audit e NÃO toque em outras tarefas — o orquestrador cuida disso.
- Ao final de CADA tarefa: `git add` só no que você tocou e um commit próprio.' 'gpt-5.6-luna' low >> "$LOG_DIR/seq.log" 2>&1; then
    # commit de segurança se o agente esqueceu (rastreabilidade > perfeição)
    if [ -n "$(git status --porcelain)" ]; then
      git add -A && git commit -q -m 'T-018 qualidade-codigo: Publicar referência e ativar gate de cobertura (auto-commit do plano)'
    fi
    marcar_concluidas T-018
    verde "✔ T-018 concluída"
    return 0
  fi
  vermelho "✘ T-018 falhou (log: $LOG_DIR/seq.log)"
  amarelo "  reexecute só ela: bash .spec/features/qualidade-codigo/executar-tarefas.sh --seq T-018"
  FALHAS="$FALHAS T-018"
  return 1
}

# ── gate: quem decide é a máquina ────────────────────────────────────
rodar_gate() {
  echo
  info "gate: verify + audit --ci"
  evento --tipo gate --etapa inicio
  node "$ENGINE" verify "$FEATURE"
  local v=$?
  evento --tipo gate --etapa verify --exit "$v"
  node "$ENGINE" audit --ci
  AUDIT=$?
  evento --tipo gate --etapa audit --exit "$AUDIT"
  # fecha a contabilidade: status das tarefas + prova do verify no git
  if [ -n "$(git status --porcelain -- '.spec')" ]; then
    git add -A -- '.spec'
    git commit -q -m "$FEATURE: status das tarefas + prova do verify (plano)"
    info "status das tarefas e prova do verify commitados"
  fi
  return "$AUDIT"
}

encerrar() { # $1=escopo
  echo
  if [ -n "$FALHAS" ]; then vermelho "faixas/tarefas com falha:$FALHAS"; fi
  # sem gate não existe veredito: NUNCA anunciar alinhamento sem o audit
  if [ "$COM_GATE" -eq 0 ]; then
    evento --tipo fim --exit 1 --escopo "$1"
    if [ -z "$FALHAS" ]; then
      amarelo "○ trabalho de '$1' terminou SEM o gate (--sem-gate) — isto NÃO é prova de nada"
      amarelo "  para o veredito: bash .spec/features/qualidade-codigo/executar-tarefas.sh --gate"
      exit 0
    fi
    vermelho "e ainda há falhas — conserte e rode o gate"
    exit 1
  fi
  rodar_gate
  local audit=$?
  if [ "$audit" -eq 0 ] && [ -z "$FALHAS" ]; then
    evento --tipo fim --exit 0 --escopo "$1"
    verde "✔ plano concluído — especificação e código alinhados (audit exit 0) na branch $BASE_BRANCH"
    info "próximo passo: revise e leve para a main quando quiser (git merge $BASE_BRANCH)"
    exit 0
  fi
  evento --tipo fim --exit 1 --escopo "$1"
  vermelho "plano terminou com pendências — leia a saída do audit acima e os logs em $LOG_DIR"
  amarelo "dica: reexecute só o que falhou (--faixa <id> / --seq <T-xxx>)"
  exit 1
}

executar_tudo() {
  evento --tipo inicio --escopo tudo
  iniciar_resumos
  info "logs em: $LOG_DIR"
  info "resumo geral de andamento: a cada 1 min aqui no terminal (e via: onp-spec resumo)"
  # onda 1: faixa-1 ∥ faixa-2 ∥ faixa-3
  info "onda 1: faixa-1 ∥ faixa-2 ∥ faixa-3 — janelas limpas em paralelo"
  executar_faixa_1 & PID_FAIXA_1=$!
  executar_faixa_2 & PID_FAIXA_2=$!
  executar_faixa_3 & PID_FAIXA_3=$!
  wait "$PID_FAIXA_1" || true
  wait "$PID_FAIXA_2" || true
  wait "$PID_FAIXA_3" || true
  # onda 2: faixa-4 ∥ faixa-5
  info "onda 2: faixa-4 ∥ faixa-5 — janelas limpas em paralelo"
  executar_faixa_4 & PID_FAIXA_4=$!
  executar_faixa_5 & PID_FAIXA_5=$!
  wait "$PID_FAIXA_4" || true
  wait "$PID_FAIXA_5" || true
  executar_seq_T_018 || true
  encerrar tudo
}

listar() {
  echo "execução: $RUN_ID (feature $FEATURE, branch $BASE_BRANCH)"
  echo "  faixa-1  onda 1  T-013"
  echo "  faixa-2  onda 1  T-014"
  echo "  faixa-3  onda 1  T-015"
  echo "  faixa-4  onda 2  T-016"
  echo "  faixa-5  onda 2  T-017"
  echo "  seq       T-018 (sequencial)"
  echo
  echo "reexecutar uma faixa:    --faixa <id>"
  echo "reexecutar sequencial:   --seq <T-xxx>"
  echo "só o gate:               --gate"
}

MODO="tudo"
ALVO=""
while [ $# -gt 0 ]; do
  case "$1" in
    --listar) MODO="listar" ;;
    --gate) MODO="gate" ;;
    --sem-gate) COM_GATE=0 ;;
    --faixa) MODO="faixa"; ALVO="${2:-}"; shift ;;
    --seq) MODO="seq"; ALVO="${2:-}"; shift ;;
    -h|--help) sed -n "2,14p" "$0"; exit 0 ;;
    *) vermelho "argumento desconhecido: $1"; sed -n "2,14p" "$0"; exit 2 ;;
  esac
  shift
done

if [ "$MODO" = "listar" ]; then listar; exit 0; fi

preparar_ambiente

case "$MODO" in
  tudo) executar_tudo ;;
  gate) COM_GATE=1; iniciar_resumos; encerrar gate ;;
  faixa)
    case "$ALVO" in
      faixa-1) evento --tipo inicio --escopo "faixa:faixa-1"; iniciar_resumos; executar_faixa_1 || true; encerrar "faixa:faixa-1" ;;
      faixa-2) evento --tipo inicio --escopo "faixa:faixa-2"; iniciar_resumos; executar_faixa_2 || true; encerrar "faixa:faixa-2" ;;
      faixa-3) evento --tipo inicio --escopo "faixa:faixa-3"; iniciar_resumos; executar_faixa_3 || true; encerrar "faixa:faixa-3" ;;
      faixa-4) evento --tipo inicio --escopo "faixa:faixa-4"; iniciar_resumos; executar_faixa_4 || true; encerrar "faixa:faixa-4" ;;
      faixa-5) evento --tipo inicio --escopo "faixa:faixa-5"; iniciar_resumos; executar_faixa_5 || true; encerrar "faixa:faixa-5" ;;
      *) falhar "faixa desconhecida: '$ALVO' — veja as disponíveis com --listar" ;;
    esac ;;
  seq)
    case "$ALVO" in
      T-018) evento --tipo inicio --escopo "seq:T-018"; iniciar_resumos; executar_seq_T_018 || true; encerrar "seq:T-018" ;;
      *) falhar "tarefa sequencial desconhecida: '$ALVO' — veja as disponíveis com --listar" ;;
    esac ;;
esac
