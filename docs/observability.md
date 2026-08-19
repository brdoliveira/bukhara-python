# Observabilidade local

Este runbook descreve a stack LGTM local da saga (`otel-lgtm`). Ela serve para desenvolvimento, demonstração e investigação; não é uma topologia de produção.

## Iniciar a stack

Na raiz do projeto, com Docker em execução:

```bash
docker compose up --build
```

O Grafana fica em [http://localhost:3000](http://localhost:3000), com as credenciais locais padrão `admin` / `admin`. No primeiro acesso, o Grafana pode pedir a troca da senha.

Para clientes executados no host, os coletores OTLP ficam em `localhost:14317` (gRPC) e `http://localhost:14318` (HTTP). Entre os containers, os microsserviços usam `http://otel-lgtm:4318`.

O dashboard **Order Saga / Saga da encomenda** é provisionado automaticamente pela stack. Abra-o em Dashboards para acompanhar, por serviço, taxa de processamento, erros, duração, eventos, retries e mensagens na DLQ. A partir dos painéis, use os links de investigação para abrir os logs no Loki e os traces no Tempo.

Para gerar uma operação de exemplo:

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: observability-demo-1" \
  -d '{"items":[{"product_id":"book","quantity":1,"price":"10.00"}]}'
```

Consulte [`docs/events.md`](events.md) para o contrato da requisição se o payload do ambiente tiver mudado.

## Roteiro de investigação

1. Copie o `correlation_id` devolvido pela requisição ou presente no log do pedido.
2. No dashboard, filtre o serviço e o intervalo aproximado da operação. Use a taxa, os erros e a duração para localizar a etapa que parou.
3. Abra o painel de logs no Loki e pesquise pelo mesmo `correlation_id`. Quando disponível, refine também por `order_id` e `service.name`.
4. Abra o `trace_id` encontrado no log no Tempo e siga os spans entre HTTP, Kafka e os consumidores. Verifique o primeiro span com erro ou maior duração.
5. Se houver retry, fallback ou DLQ, confira os eventos e a ordem dos spans; diferencie uma falha transitória de uma mensagem encaminhada para a DLQ.
6. Registre o serviço, o `order_id`, o `correlation_id`, o horário e o erro observado antes de reproduzir a operação.

## Limites do perfil local

Esta stack local não define retenção durável, autenticação, TLS, alta disponibilidade nem dimensionamento de produção. As credenciais são apenas locais e de desenvolvimento. Para produção, esses requisitos devem ser projetados e configurados separadamente.
