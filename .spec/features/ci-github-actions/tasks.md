# Tasks: Integração contínua no GitHub

> feature: ci-github-actions

## T-019 — Automatizar testes, cobertura e auditoria [concluida]
- Refs: US-010, AC-032, AC-033, AC-034
- Arquivos: .github/workflows/ci.yml, tests/integration/test_ci_workflow.py, onpspec.config.json
- Notas: workflow com permissão somente de leitura, dependências cacheadas e ferramentas fixadas por versão principal ou exata.
