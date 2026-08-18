# Design: Saga distribuída de pedidos

## Visão geral

A solução usa coreografia: cada serviço consome fatos já ocorridos e publica o próximo fato. Nenhum serviço chama outro por HTTP no caminho de negócio. FastAPI expõe a API de pedidos e endpoints operacionais em todos os processos; Kafka transporta eventos e comandos de compensação.

```text
POST /orders
      |
      v
 order.created --> inventory-service -- inventory.reserved --> payment-service
                         |                              |             |
                         | inventory.rejected          |             +--> payment.approved
                         |                              |             |
                         +<-- inventory.release.requested <--- payment.failed
                                      |
                                      +--> inventory.released

eventos finais -------------------------------> notification-service
```

## Componentes

- `order-service`: valida a requisição, persiste pedido e Outbox na mesma transação e publica `order.created`.
- `inventory-service`: usa Inbox/Outbox, simula reserva/liberação e publica `inventory.reserved`, `inventory.rejected` e `inventory.released`.
- `payment-service`: usa Inbox/Outbox, simula cobrança e publica `payment.approved`; diante de falha definitiva publica `payment.failed` e `inventory.release.requested`.
- `notification-service`: usa Inbox para processar os resultados finais sem interferir na saga.
- `event-bus`: pacote Python compartilhado com envelope, serialização, classificação de erros, retry, DLQ e readiness.
- `PostgreSQL`: um cluster local com banco lógico pertencente a cada serviço; tabelas nunca são lidas por outro serviço.

## Envelope de evento

Todo evento contém `event_id` UUID, `event_type`, `event_version`, `occurred_at`, `producer`, `correlation_id`, `causation_id` e `payload`. O `event_id` não muda durante retry; um novo fato recebe novo `event_id` e aponta `causation_id` para o fato causador.

## Tópicos

- Negócio: `orders.events`, `inventory.events`, `payments.events` e `notifications.events`.
- Retry por serviço: `<service>.retry.1`, `<service>.retry.2`, `<service>.retry.3`.
- DLQ por serviço: `<service>.dlq`.

Consumidores filtram `event_type` dentro de seu conjunto de tópicos. No MVP, os três níveis de retry usam atrasos configuráveis e pequenos nos testes; em execução local, os padrões são 1 s, 5 s e 15 s.

## Persistência e entrega

Cada consumidor abre uma transação PostgreSQL, registra o `event_id` na Inbox, aplica o efeito local e grava os novos eventos na Outbox. Duplicatas já presentes na Inbox são confirmadas sem novo efeito. Um publicador separado drena a Outbox, publica no Kafka com chave `order_id` e marca o registro como publicado. Falha entre publicação e marcação pode duplicar a mensagem, por isso todos os consumidores permanecem idempotentes.

O ambiente local usa um único contêiner PostgreSQL com bancos separados para reduzir custo operacional. Essa decisão não autoriza joins ou acesso cruzado entre serviços.

## Política de erros

- Erro de negócio (`out_of_stock`, `payment_declined`): não repete; publica o evento de resultado correspondente.
- Erro transitório: publica no próximo retry com `attempt` incrementado.
- Erro permanente de validação: envia diretamente à DLQ.
- Retry transitório esgotado: executa fallback seguro uma vez e envia o evento original enriquecido à DLQ.
- Mensagem duplicada: confirma sem repetir o efeito.

## Fallback e compensação

O fallback nunca transforma falha técnica em sucesso. No pagamento, ele publica `payment.failed` e solicita liberação de estoque. No estoque, publica rejeição técnica do pedido. Na notificação, registra a falha definitiva na DLQ; a conclusão do pedido não é revertida.

## Testes

Testes unitários usam adaptadores Kafka e repositórios em memória, além de relógio injetável. Testes de integração marcados executam contra Kafka e PostgreSQL do Docker Compose, inclusive reinício com Outbox pendente. Cada critério de aceite aparece no título do teste como `@spec:AC-xxx`.

## Decisões confirmadas

- Coreografia por eventos, sem orquestrador central.
- PostgreSQL com Inbox/Outbox duráveis e propriedade de dados por serviço.
