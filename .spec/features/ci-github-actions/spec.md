# Spec: Integração contínua no GitHub

> feature: ci-github-actions
> status: auditada

## Contexto

Os testes, a cobertura mínima e a auditoria da especificação funcionam
localmente, mas ainda dependem de execução manual. Uma regressão pode chegar a
`main` sem que esses gates sejam executados pelo GitHub.

## Histórias

### US-010 — Validar cada mudança antes da integração

Como pessoa mantenedora, quero que o GitHub valide automaticamente cada pull
request e envio para `main`, para que regressões sejam bloqueadas antes de
serem incorporadas ao projeto.

#### AC-032 — Mudanças em main sempre disparam o CI

- **Dado** um push ou pull request direcionado para `main`
- **Quando** o GitHub recebe a mudança
- **Então** o workflow de integração contínua é disparado automaticamente

#### AC-033 — Testes e cobertura são gates obrigatórios

- **Dado** o workflow em execução com Python 3.12 e dependências de desenvolvimento
- **Quando** a validação da feature é executada
- **Então** os testes herméticos rodam e o job falha se a cobertura de produção ficar abaixo de 85%

#### AC-034 — Alinhamento entre especificação e código bloqueia o job

- **Dado** que as provas da feature foram produzidas pelo test runner
- **Quando** o gate final do workflow é executado
- **Então** a auditoria mecânica roda em modo CI e qualquer divergência encerra o job com falha

## Fora de escopo

- Executar a saga end-to-end com containers Kafka e PostgreSQL.
- Configurar a regra de proteção de branch no GitHub.
- Publicar imagens ou realizar deploy.

## Suposições

| ID | Suposição | Status | Resolução |
|---|---|---|---|
| ASM-011 | GitHub Actions é o provedor de CI e `main` é a branch protegida pelo fluxo. | confirmada | O repositório remoto é GitHub e o usuário pediu explicitamente o primeiro ponto para `main`. |
| ASM-012 | A suíte de CI não precisa iniciar Docker. | confirmada | A suíte existente é hermética e usa doubles para dependências externas. |

## Perguntas em aberto

Nenhuma.
