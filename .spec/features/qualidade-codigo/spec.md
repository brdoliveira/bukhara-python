# Spec: Documentação e qualidade do código

> feature: qualidade-codigo
> status: em-implementacao

## Contexto

O projeto possui 79 testes e os módulos descrevem sua finalidade, mas 46 classes e funções públicas de primeiro nível ainda não têm docstring. A cobertura inicial é 74%, concentrando lacunas nos runtimes Kafka/PostgreSQL, caminhos de recuperação e adaptadores de falha. Quem mantém o projeto precisa compreender os contratos e detectar regressões sem depender da leitura integral da implementação.

## Histórias

### US-008 — Compreender os contratos do código

Como pessoa desenvolvedora, quero encontrar documentação junto às APIs e em um guia de referência, para que eu consiga usar e evoluir cada módulo com segurança.

#### AC-024 — APIs públicas possuem documentação verificável

- **Dado** o código de produção em `services/` e `packages/`
- **Quando** a verificação de documentação analisa os módulos Python
- **Então** todos os módulos, classes e funções públicas de primeiro nível possuem docstrings que explicam sua responsabilidade

#### AC-025 — Guia de referência orienta a navegação

- **Dado** que uma pessoa precisa localizar um contrato ou ponto de extensão
- **Quando** abre a documentação de referência do código
- **Então** encontra os dois pacotes compartilhados, os quatro serviços, seus principais componentes, dependências e comandos de teste

### US-009 — Evoluir com uma rede de segurança mensurável

Como pessoa mantenedora, quero testes para caminhos de sucesso e falha dos módulos, para que regressões em resiliência, persistência e telemetria sejam detectadas antes do merge.

#### AC-026 — Pacotes compartilhados cobrem seus casos-limite

- **Dado** os pacotes de eventos e observabilidade
- **Quando** a suíte exercita entradas inválidas, falhas de exportação e estados terminais
- **Então** os contratos degradam com segurança e os testes permanecem determinísticos

#### AC-027 — Serviço de pedidos cobre recuperação e erros

- **Dado** pedidos idempotentes e eventos pendentes na Outbox
- **Quando** publicação, inicialização ou dependências falham
- **Então** os testes comprovam persistência do pedido, recuperação posterior e prontidão correta

#### AC-028 — Serviço de estoque cobre runtime e persistência

- **Dado** eventos de reserva, liberação, retry e DLQ
- **Quando** o estoque processa duplicatas, falhas transitórias e reinicialização
- **Então** os testes comprovam deduplicação, Outbox durável, fallback único e lifecycle seguro

#### AC-029 — Serviço de pagamento cobre runtime e persistência

- **Dado** eventos de reserva e decisões de pagamento
- **Quando** ocorrem aprovação, recusa, retry ou falha terminal
- **Então** os testes comprovam eventos emitidos, compensação, deduplicação e lifecycle seguro

#### AC-030 — Serviço de notificação cobre worker e falhas

- **Dado** eventos de pagamento aprovado
- **Quando** o envio é duplicado, falha temporariamente ou chega inválido
- **Então** os testes comprovam envio idempotente, retries, fallback, DLQ e encerramento seguro do worker

#### AC-031 — Cobertura mínima impede regressão silenciosa

- **Dado** a suíte completa do projeto
- **Quando** os testes são executados com cobertura
- **Então** a execução falha se a cobertura global do código de produção ficar abaixo da meta confirmada

## Fora de escopo

- Comentários que apenas repetem cada linha ou documentação de detalhes privados triviais.
- Alterar contratos de negócio apenas para elevar cobertura.
- Testes dependentes de Kafka, PostgreSQL ou Grafana externos; integrações são exercitadas com doubles determinísticos.
- Gerar um site de documentação ou publicar pacotes nesta entrega.

## Suposições

| ID | Suposição | Status | Resolução |
|---|---|---|---|
| ASM-008 | Documentar o código significa cobrir módulos e APIs públicas, priorizando contratos, efeitos e falhas, sem comentar detalhes privados triviais. | confirmada | O usuário escolheu 1A em 2026-08-19. |
| ASM-009 | A meta inicial de cobertura será 85% global, acima da linha de base de 74%. | confirmada | O usuário escolheu 2A em 2026-08-19. |
| ASM-010 | Os novos testes serão herméticos e não exigirão containers ativos. | confirmada | O usuário escolheu 2A em 2026-08-19. |

## Perguntas em aberto

| ID | Pergunta | Status | Resposta |
|---|---|---|---|
| Q-004 | A documentação deve focar APIs públicas ou incluir também todo método privado? | respondida | APIs públicas e métodos de contrato relevantes; detalhes privados triviais ficam fora. |
| Q-005 | Qual meta de cobertura deve bloquear a suíte: 85%, 90% ou nenhuma meta numérica? | respondida | 85% de cobertura global. |
| Q-006 | Os testes podem usar doubles herméticos ou precisam subir dependências reais via Docker? | respondida | Doubles herméticos, sem dependência de containers. |
