---
displayed_sidebar: dataengineSidebar
title: Testes e Cobertura
---

## Estratégia de Testes

O framework utiliza o **DUnitX** e **Delphi Mocks** (opcionais) para validar as interfaces e as integrações com os drivers nativos.

- **Unitários:** Validação de comportamento das factories e criação correta das interfaces.
- **Integração:** Testes de persistência real contra bancos de dados populares (SQLite, Firebird, MySQL, MSSQL).
- **Regressão:** Testes automáticos em cada `build` de DML e DDL para garantir que alterações no Core não quebrem o comportamento original.

## Como Executar

Acesse a pasta `Test Delphi/` e abra o projeto `DataEngineTests.dproj` no Delphi IDE para rodar os testes localmente.

### Cenários Suportados

- Conexão e Desconexão.
- Execução de comandos `SELECT`, `INSERT`, `UPDATE`, `DELETE`.
- Validação do Metadata Cache (hits e saves).
- Testes de auto-invalidação de cache.
- Múltiplas transações simultâneas.
- **Bulk Loading Performance:** Benchmark dedicado em `Tests.BulkLoad.dpr` para validação de ganhos da interface de massa.
- **Connection Resiliency:** Testes simulando falhas de conexão e recuperação via guard (`Tests.Resiliency.dpr`).
- **Async Loading:** Validação de execução de queries em background com PPL (`Tests.AsyncLoading.dpr`).
