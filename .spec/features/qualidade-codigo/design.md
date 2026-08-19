# Design: documentação e qualidade do código

## Linha de base

- 79 testes aprovados.
- 2.117 statements de produção medidos; 74% de cobertura global.
- 36 módulos de produção com docstring de módulo.
- 46 classes e funções públicas de primeiro nível sem docstring.
- Maiores lacunas de cobertura: `inventory_service.main`/persistência, `payment_service.main`/persistência, worker de notificação e caminhos de degradação da telemetria.

## Política de documentação proposta

1. Todo módulo, classe e função pública de primeiro nível tem docstring.
2. Docstrings descrevem responsabilidade, invariantes, efeitos externos e exceções relevantes; não repetem a assinatura.
3. Protocolos, runtimes e repositórios documentam limites transacionais e lifecycle.
4. `docs/code-reference.md` serve como mapa de navegação, sem duplicar a implementação.
5. Um teste AST impede regressão da documentação pública.

## Estratégia de testes proposta

- Testes unitários exercitam contratos e branches com doubles assíncronos determinísticos.
- Persistência SQL é testada por SQLite quando o contrato é portável e por doubles de conexão quando a construção é específica do PostgreSQL.
- Lifecycle Kafka testa `start`, `stop`, readiness, publicação e recuperação sem broker externo.
- A cobertura considera apenas os seis diretórios de código de produção.
- `pytest-cov` fica em dependências de desenvolvimento e `pytest` aplica o limite global confirmado.
- Testes devem afirmar comportamento observável; executar linhas sem verificar resultados não conta como objetivo.

## Sequenciamento

T-013 a T-017 alteram conjuntos de arquivos disjuntos e podem executar em paralelo. T-018 é sequencial porque ativa o gate somente depois que os testes por módulo elevarem a cobertura.

## Riscos e controles

- **Docstrings excessivas:** limitar a APIs públicas e contratos não óbvios.
- **Testes acoplados à implementação:** preferir entradas, saídas, efeitos persistidos e mensagens publicadas.
- **Cobertura artificial:** revisar asserts e manter o limite global como piso, não como objetivo isolado.
- **Suíte lenta ou instável:** nenhum teste novo depende de rede, relógio real ou container.
