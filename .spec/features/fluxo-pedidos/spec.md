# Spec: Saga distribuída de pedidos

> feature: fluxo-pedidos
> status: auditada

## Contexto

Uma API de pedidos precisa coordenar estoque, pagamento e notificação sem acoplamento síncrono entre os serviços. O fluxo deve continuar seguro diante de indisponibilidade temporária, mensagens duplicadas e falhas definitivas.

## Histórias

### US-001 — Cliente envia um pedido

Como cliente, quero enviar um pedido e receber sua identificação imediatamente, para acompanhar o processamento assíncrono.

#### AC-001 — Pedido válido é aceito e publicado uma única vez

- **Dado** um pedido válido com uma chave de idempotência inédita
- **Quando** o cliente envia `POST /orders`
- **Então** recebe `202 Accepted` com `order_id`, `status=accepted` e `correlation_id`
- **E** um evento `order.created` com esses identificadores e os itens é publicado uma única vez

#### AC-002 — Reenvio idempotente não duplica o pedido

- **Dado** um pedido já aceito com uma chave de idempotência
- **Quando** o cliente reenvia o mesmo conteúdo com a mesma chave
- **Então** recebe o mesmo `order_id` e nenhum novo evento é publicado

#### AC-003 — Pedido inválido é rejeitado antes da mensageria

- **Dado** um pedido sem itens ou com quantidade ou preço inválido
- **Quando** o cliente envia `POST /orders`
- **Então** recebe `422 Unprocessable Entity` com os campos inválidos
- **E** nenhum evento é publicado

### US-002 — Plataforma processa a saga do pedido

Como operação, quero que estoque e pagamento avancem por eventos, para manter os microsserviços independentes.

#### AC-004 — Estoque disponível avança para pagamento

- **Dado** um evento `order.created` válido e estoque disponível
- **Quando** o serviço de estoque processa o evento
- **Então** publica uma única vez `inventory.reserved` com o mesmo `order_id` e `correlation_id`

#### AC-005 — Estoque indisponível encerra o pedido sem cobrar

- **Dado** um evento `order.created` válido e estoque insuficiente
- **Quando** o serviço de estoque processa o evento
- **Então** publica `inventory.rejected` com o motivo da rejeição
- **E** nenhum pagamento é solicitado

#### AC-006 — Pagamento aprovado conclui o pedido

- **Dado** um evento `inventory.reserved` e pagamento aprovado
- **Quando** o serviço de pagamento processa o evento
- **Então** publica uma única vez `payment.approved` com o mesmo `order_id` e `correlation_id`
- **E** a notificação de pedido concluído é processada

#### AC-007 — Falha definitiva de pagamento aciona compensação

- **Dado** um evento `inventory.reserved` cujo pagamento falha definitivamente
- **Quando** as tentativas permitidas terminam
- **Então** publica `payment.failed` e `inventory.release.requested`
- **E** o estoque publica `inventory.released` após compensar a reserva

### US-003 — Operação enfrenta falhas de mensageria com segurança

Como operação, quero retries limitados, fallback e fila de mensagens mortas, para recuperar falhas transitórias sem criar loops ou efeitos duplicados.

#### AC-008 — Falha transitória usa retry exponencial limitado

- **Dado** um evento válido cuja dependência retorna uma falha transitória
- **Quando** um consumidor não consegue processá-lo
- **Então** o evento é encaminhado por no máximo três tentativas com atrasos crescentes
- **E** preserva `event_id`, `correlation_id` e registra o número da tentativa

#### AC-009 — Retry esgotado usa fallback e DLQ

- **Dado** um evento que falhou nas três tentativas
- **Quando** o limite de retry é atingido
- **Então** o fallback seguro do serviço é executado uma única vez
- **E** a mensagem vai para a DLQ do serviço com tipo do erro, serviço de origem e total de tentativas

#### AC-010 — Evento duplicado não repete efeito de negócio

- **Dado** um `event_id` já processado com sucesso
- **Quando** o mesmo evento é entregue novamente
- **Então** o consumidor confirma a mensagem sem repetir publicação, cobrança, reserva ou notificação

#### AC-011 — Evento inválido é isolado sem derrubar o consumidor

- **Dado** um evento com envelope ou payload inválido
- **Quando** um consumidor recebe a mensagem
- **Então** a mensagem vai diretamente para a DLQ com os erros de validação
- **E** o consumidor continua apto a receber novos eventos

#### AC-012 — Saúde e prontidão distinguem processo vivo de Kafka disponível

- **Dado** qualquer microsserviço em execução
- **Quando** a operação consulta `/health` e `/ready`
- **Então** `/health` informa que o processo está vivo
- **E** `/ready` retorna sucesso somente quando Kafka e PostgreSQL necessários estão disponíveis

#### AC-013 — Outbox pendente é recuperada após indisponibilidade

- **Dado** um efeito de negócio confirmado no PostgreSQL e seu evento pendente na Outbox
- **Quando** Kafka volta a ficar disponível ou o serviço reinicia
- **Então** o publicador envia o evento preservando `event_id` e `correlation_id`
- **E** marca o registro como publicado sem perder o evento nem repetir o efeito de negócio

## Fora de escopo

- Interface web, autenticação de clientes e autorização.
- Consulta histórica de pedidos e APIs administrativas.
- Integrações reais com adquirente de pagamento, ERP, e-mail ou SMS.
- Garantia global de exatamente uma vez; o projeto oferece processamento efetivamente uma vez por idempotência local.
- Kubernetes, service mesh e observabilidade distribuída externa.

## Suposições

| ID | Suposição | Status | Resolução |
|---|---|---|---|
| ASM-001 | O ambiente local usa Kafka em modo KRaft por Docker Compose. | confirmada | O usuário não indicou preferência diferente ao escolher o fluxo recomendado em 2026-08-18. |
| ASM-002 | As integrações de estoque, pagamento e notificação são adaptadores simulados e determinísticos no MVP. | confirmada | Mantém o foco solicitado em FastAPI, Kafka e resiliência. |
| ASM-003 | A saga usa coreografia por eventos, sem orquestrador central. | confirmada | O usuário escolheu a opção 1A em 2026-08-18. |
| ASM-004 | Cada serviço usa Inbox/Outbox duráveis no PostgreSQL para deduplicação e publicação confiável. | confirmada | O usuário escolheu a opção 2A em 2026-08-18. |

## Perguntas em aberto

| ID | Pergunta | Status | Resposta |
|---|---|---|---|
| Q-001 | A saga deve permanecer coreografada ou deve existir um microsserviço orquestrador? | respondida | Coreografia por eventos. |
| Q-002 | A idempotência em memória é suficiente para estudo ou deve ser durável com PostgreSQL? | respondida | PostgreSQL com Outbox/Inbox. |
