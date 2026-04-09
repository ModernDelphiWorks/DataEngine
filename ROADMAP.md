# Roadmap

Este documento delineia a evolução passada e os objetivos futuros do projeto **DataEngine**.

**Última atualização:** 2026-04-09 (Correção de regressão Issue #35)

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


## Visão de Longo Prazo
- Tornar o DataEngine o framework de conectividade padrão para o ecossistema Delphi moderno, com foco em performance e abstração total de banco de dados.


## FUTURO A ANALISAR


## Sobre o SPRINT-18 (Multi-tenant Connection Pooling)
A vantagem real: Em sistemas Desktop, cada usuário tem sua própria conexão aberta na sua máquina. O custo é baixo. Mas em APIs e microsserviços (ex: Horse/DataSnap), se chegarem 500 requisições num segundo, criar 500 conexões novas de banco ao mesmo tempo derruba qualquer SGBD por excesso de Handshake / Autenticação TCP. O Pooling faz o DataEngine manter, por exemplo, 20 conexões permanentemente abertas na memória do servidor da sua API. Quando chegam 500 usuários pedindo dados, as requisições "pegam emprestadas" as conexões livres durante alguns milissegundos e devolvem.

Resumo: Aumenta absurdamente a performance para a web/API e evita queda do banco (exhaustion). Porém, se você usa as ferramentas que dão suporte de pooling dos próprios drivers underlying, ele perde a prioridade. Se o FireDAC usar o dele, por exemplo. Mas se você quiser um agnóstico, seria SPRINT-18.

## Sobre o SPRINT-20 (Schema Migrations)
Sua visão é corretíssima. O FluentSQL cuida da "tradução", dizendo a estrutura das tabelas. O DataEngine entra com o poder bruto, executando o ExecuteAsync agnóstico com Retry Policy na base. Seria perfeito integrá-los e ter uma rotina transparente que rode tudo lendo do DB.

## Sobre o SPRINT-21 (Framework Adapters & Publish)
Isso é puramente para "disponibilização". Duas coisas que ajudam a tornar o ecossistema um padrão usado por outros:

Boss Publishing: Basicamente empacotar o DataEngine para instalar via Boss (o gerenciador de pacotes moderno do Delphi). O cara digita boss install DataEngine lá e já puxa da dependência correta.

Adapters (Middlewares): Se alguém vai criar uma API com Horse, a gente criaria um pacote que liga o DataEngine no Horse em 1 linha. Algo que abra uma transação no recebimento de um request web e faça "Commit" assim que for respondida com Status 200, ou "Rollback" se o Horse estourar um erro 500. É o que chamamos de Middleware de Transação.