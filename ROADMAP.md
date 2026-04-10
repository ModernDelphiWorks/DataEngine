# Roadmap

Este documento delineia a evolução passada e os objetivos futuros do projeto **DataEngine**.

**Última atualização:** 2026-04-10 (Conclusão do Multi-tenant Pooling - v1.0.0-Ready)

## Concluído (Histórico de Sprints)

### Q1 2026: Foundation & Drivers Standardization
- [x] **SPRINT-01: Foundation & FireDAC Reference** — Contratos Core e driver referência estável.
- [x] **SPRINT-02: Advanced Drivers** — Suporte padronizado para SQLite3, Zeos e UniDac.
- [x] **SPRINT-03: Medium Coverage Drivers** — Suporte para ADO, Memory e SQLDirect.
- [x] **SPRINT-04: Legacy & Niche Drivers** — Padronização de mais de 10 drivers adicionais.

### Q2 2026 (Início): Intelligent Caching
- [x] **SPRINT-05: Cache Study** — Auditoria técnica e definição do motor de cache.
- [x] **SPRINT-06: Cache Persistency** — Persistência em disco via SQLite.
- [x] **SPRINT-07: Auto-Invalidation** — Limpeza inteligente de cache via interceptação DML.
- [x] **SPRINT-08: Metadata Cache (Part 1: Infrastructure)** — Entrega v0.8.0 com infraestrutura base, serializer e hooks para FireDAC/UniDAC.
- [x] **SPRINT-08: Metadata Cache (Part 2: Universal Integration)** — Cobertura para drivers Zeos, DBExpress, SQLite3 e Benchmarking formal.

---

## Em Planejamento / Execução

### Q2 2026 (Próximos Passos)
- [x] **SPRINT-09: Unified Core & Template Method** — Refatoração profunda de `TDriverConnection` para eliminar boilerplate nos drivers (Baseado na issue #12).
- [x] **SPRINT-10: Bulk Operations (FastLoad)** — Interface genérica para inserções em massa de alta performance, utilizando Array DML e recursos nativos dos drivers.
- [x] **SPRINT-11: Transaction Isolation Levels** — Suporte agnóstico a níveis de isolação (ReadCommitted, Serializable, etc.) e refinamento do controle de múltiplas transações. (Issue #29)
- [x] **SPRINT-12: JSON Store Driver (Optional/NoSQL)** — Implementar driver para arquivos JSON puramente como abstração de persistência local.

### Q3 2026
- [x] **SPRINT-13: Distributed Cache Support** — Cache centralizado (Redis/Outros).
- [x] **SPRINT-14: Stream-based Loading (Foundation)** — Infraestrutura assíncrona baseada na PPL e evolução de interfaces para `OpenAsync` e `FetchOptions`. (v0.14.0)
- [x] **SPRINT-14: Native Driver Streaming** — Implementação de fetching incremental nativo nos principais drivers. (v0.14.0)
- [x] **SPRINT-15: Connection Resiliency & Health Checks** — Implementação de Retry Policy e verificações de integridade de conexão (Ping-Alive). (v0.15.0)
- [x] **SPRINT-16: Advanced Observability** — Métricas de performance de query, estruturação do MonitorCallback para telemetria e detecção de slow queries. (v0.16.0)
- [x] **SPRINT-17: Technical Debt & Stabilization** — Resolução de regressões de transação (Issue #35 e #43), refinamento de observers e padronização. (v0.17.x)
- [x] **SPRINT-18: Core Decoupling & Finalization** — Desacoplamento total do Core de engines específicos (FireDAC), substituição por `TClientDataSet` no Cache e Snapshot, e uso de driver SQLite nativo para persistência local. Este é o marco final do projeto **DataEngine**. (v1.0.0-Ready)
- [x] **SPRINT-19: Multi-tenant Connection Pooling** — Implementação de gestão de múltiplos pools para suporte a SaaS e Microserviços.



## Visão de Longo Prazo
- Tornar o DataEngine o framework de conectividade padrão para o ecossistema Delphi moderno, com foco em performance e abstração total de banco de dados.


## Banco de Ideias & Estudos Futuros

Este tópico serve como um "backlog de inovação" para o DataEngine. Aqui são registradas ideias, tendências e arquiteturas que podem ser exploradas em sprints futuras após a estabilização da V1.0.

### 1. Multi-tenant Connection Pooling (Estudo Base SPRINT-18)
*   **Conceito:** Implementar um pool de conexões agnóstico e de alta performance dentro do framework.
*   **Vantagem:** Essencial para APIs e Microsserviços (Horse, DataSnap). Evita o "exhaustion" do SGBD ao manter conexões persistentes e reutilizáveis, reduzindo o custo de handshake/autenticação TCP.
*   **Prioridade:** Alta para cenários Web/Server.

### 2. Schema Migrations & Fluent Integration
*   **Conceito:** Integrar o motor do DataEngine com o FluentSQL para fornecer uma camada de migração de banco de dados transparente.
*   **Vantagem:** Permitir versionamento de banco de dados agnóstico, onde a estrutura é definida em código e aplicada via `ExecuteAsync` com `Retry Policy`.

### 3. Framework Adapters & Eco-System Publishing
*   **Conceito:** Criar pontes nativas para outros frameworks populares.
    *   **Boss Publishing:** Empacotamento formal para instalação via `boss install`.
    *   **Horse Middlewares:** Adaptadores automáticos para gerenciar transações baseadas no ciclo de vida da requisição HTTP (Auto-Commit no Status 200, Auto-Rollback no Status 500).

### 4. Native Driver Streaming & Incremental Fetching
*   **Conceito:** Aprofundar o suporte a streaming de dados para grandes volumes, garantindo que o `FetchOptions` seja respeitado nativamente por todos os drivers (FireDAC, UniDAC, Zeos).

### 5. Distributed Cache Providers (Redis/Memcached)
*   **Conceito:** Expandir a interface `IDBCacheProvider` para suportar backends distribuídos, permitindo que instâncias diferentes de uma API compartilhem o mesmo cache de resultados.

---
## Visões e Insights Adicionais
*Qualquer nova proposta de arquitetura ou melhoria deve ser documentada aqui para avaliação de impacto antes de ser convertida em uma Sprint oficial.*