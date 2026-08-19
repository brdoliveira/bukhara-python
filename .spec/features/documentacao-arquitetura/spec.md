# Spec: Documentação da arquitetura

> feature: documentacao-arquitetura
> status: auditada

## Contexto

O README explica como executar o projeto, mas ainda não permite compreender rapidamente os limites dos sistemas nem o fluxo interno da saga. A documentação precisa oferecer uma visão executiva e outra de implementação sem divergir do código.

## Histórias

### US-007 — Compreender a arquitetura do projeto

Como pessoa desenvolvedora, quero visualizar a arquitetura em diferentes níveis de detalhe, para que eu consiga navegar, executar e evoluir a saga com segurança.

#### AC-022 — Visão de alto nível da plataforma

- **Dado** que uma pessoa acessa o README do projeto
- **Quando** consulta a seção de arquitetura HLD
- **Então** encontra um diagrama que relaciona cliente, quatro microsserviços, Kafka, PostgreSQL e a stack LGTM

#### AC-023 — Visão detalhada da saga e da resiliência

- **Dado** que uma pessoa precisa investigar o processamento de um pedido
- **Quando** consulta a seção de arquitetura LLD
- **Então** encontra o fluxo de eventos, Inbox/Outbox, idempotência, retry, fallback, DLQ e compensação de estoque

## Fora de escopo

- Alterar a topologia, os microsserviços ou os contratos de eventos.
- Criar imagens binárias ou diagramas dependentes de ferramentas externas.
- Descrever uma topologia de produção, Kubernetes ou segurança de borda.

## Suposições

| ID | Suposição | Status | Resolução |
|---|---|---|---|
| ASM-007 | Mermaid é o formato adequado para os desenhos no README. | confirmada | O repositório está no GitHub, que renderiza Mermaid nativamente, mantendo os diagramas versionáveis. |

## Perguntas em aberto

Nenhuma.
