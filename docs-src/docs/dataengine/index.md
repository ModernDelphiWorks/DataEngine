---
displayed_sidebar: dataengineSidebar
title: DataEngine
---

O **DataEngine** (anteriormente conhecido como **DBEBr**) é um framework Delphi de alto desempenho projetado para prover desacoplamento total de engines de conexão através de uma interface orientada a objetos simplificada.

## Por onde começar

- [Introdução](introduction.md)
- [Quickstart](getting-started/quickstart.md)
- [Arquitetura](architecture/overview.md)
- [API Reference](reference/api.md)
- [Operações em Massa (Bulk)](guides/bulk-operations.md)
- [Observabilidade Avançada](guides/observability.md)
- [Testes](tests/overview.md)

## Escopo

- **Cobre:** Abstração de conexões (FireDAC, DBExpress, UniDAC, Zeos, etc.), execução de comandos SQL (DML/DDL), gerenciamento de transações e cache de metadados.
- **Não cobre:** Lógica de negócio específica, geração de relatórios ou persistência ORM direta (veja o [Janus ORM](https://github.com/ModernDelphiWorks/Janus)).
