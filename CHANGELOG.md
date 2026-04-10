# Changelog - DataEngine

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.
O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-br/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Added
- **Core Decoupling**: Replaced all FireDAC dependencies in `CacheManager`, `SQLiteCacheProvider`, and `DataSetSnapshot` with engine-agnostic components (`TClientDataSet`) and the native SQLite3 driver, achieving a 100% decoupling of the Core from specific frameworks (Issue #45).
- **Snapshot Serialization**: Migrated from FireDAC binary format to universal Midas binary format for cache snapshots, ensuring compatibility across all data drivers.
- **Unified Local Cache**: Refactored `TSQLiteCacheProvider` to use the embedded Native SQLite3 driver instead of FireDAC providers, reducing the final binary footprint and external dependencies.

### Fixed
- **Driver Robustness**: Standardized `IDBConnection` internal interfaces across legacy drivers and resolved namespace collisions in test fixtures (Issue #35).
- **Implicit Transactions**: Finalized support for implicit 'DEFAULT' transaction activation in `TFactoryConnection`, ensuring compatibility for lazy-initialization drivers.
- **Transaction Safety**: Resolved critical access violations in `InTransaction` state queries by implementing the Template Method pattern in the base class, ensuring `FTransactionActive` null-checks and thread-safe locking across all drivers (Issue #43).
- **Test Alignment**: Updated DBExpress and FireDAC test projects and modernized test constant naming conventions.

### Changed
- **Driver Architecture**: Refactored transaction state hooks (`_InTransaction`) for 17 database drivers, promoting centralized lifecycle management.

## [0.17.2] - 2026-04-09
### Fixed
- **Transaction Consistency**: Fixed regression in `TFactoryConnection.StartTransaction` that prevented legacy drivers (direct DBExpress) from initializing their 'DEFAULT' transaction lazily (Issue #35).
- **Graceful State Checks**: Modified `InTransaction` in DBExpress, FireDAC, and UniDAC drivers to return `False` instead of raising an exception when no transaction is active, preventing crashes in `ExecuteDirect` calls.

## [0.17.1] - 2026-04-08
### Fixed
- **Observability Refinement**: Isolamento preciso da métrica `FetchTime` no loop do cursor `TDriverDataSet<T>` mitigando falsos zeros de report em execução (Issue #46).
- **Concurrency Safety**: Implementação do padrão Lock-on-Clone/Copy-on-Read (`ToArray`) em `TDriverConnection.Notify` para prevenir data races e Access Violations em eventos PPL paralelos.

## [0.17.0] - 2026-04-08
### Added
- **Observer Management API**: Exposição de `AddObserver`, `RemoveObserver` e `SlowQueryThreshold` em `TFactoryConnection` para configuração direta no nível de factory (Issue #47).
### Changed
- **Constructor Integrity**: Parâmetro `ASlowQueryThreshold` adicionado ao construtor de `TDriverQuery` para garantir configuração no momento da criação, eliminando dependência de configuração pós-construção via factory.

## [0.16.0] - 2026-04-08
### Added
- **Advanced Observability System**: Implementação de telemetria estruturada via `IDBObserver` e `TMetricCollector` (Issue #45).
- **High-Precision Timing**: Integração de `TStopwatch` na camada de query para precisão sub-milissegundo em métricas de execução.
- **Structured Events**: Suporte a eventos de tipo `Start`, `End`, `Error` e `Metric` no pipeline de monitoramento.
- **Slow Query Tracking**: Detecção automatizada de consultas que excedem o `SlowQueryThreshold` configurável.
- **Thread-Safe Telemetry**: Implementação de proteção por `TCriticalSection` para o gerenciamento de observers, garantindo segurança em ambientes multithread.

## [0.15.0] - 2026-04-08
### Added
- **Connection Resiliency System**: Evolução do padrão `TGuardConnection` para orquestrador resiliente com suporte a retentativas automáticas (Issue #44).
- **Health Check Mechanism**: Introdução de `IsAlive` na interface `IDBConnection` para verificação proativa de integridade.
- **Resilience Policies**: Implementação da interface `IDBResiliencePolicy` e política `TDefaultResiliencePolicy` configurável via Builder.
- **Pool Safety**: Integração de health checks no `TPoolConnection`, garantindo que conexões rompidas sejam descartadas antes da entrega ao consumidor.
- **Driver Integration**: Suporte nativo a `Ping` nos drivers FireDAC e UniDAC.

## [0.14.0] - 2026-04-07
### Added
- **Async Loading Infrastructure**: Implementação do motor de execução assíncrona `TDataEngineAsync` baseado na PPL (Parallel Programming Library).
- **Interface Evolution**: Introdução de `OpenAsync(OnComplete, OnError)` na interface `IDBDataSet`.
- **Fetch Settings**: Adicionada propriedade `FetchOptions` com suporte a modos `fmAll`, `fmManual` e `fmOnDemand`.
- **UI Synchronization**: Garantia de execução de callbacks na thread principal via `TThread.Queue`.
- **Base Fallback**: Implementação de suporte assíncrono básico nas classes base `TDriverQuery` e `TDriverDataSetBase`, permitindo uso imediato em todo o ecossistema.
- **Native Driver Streaming**: Implementação de carregamento incremental nos drivers FireDAC, UniDAC e Zeos (Issue #43).

## [0.13.0] - 2026-04-07
### Added
- **Distributed Cache Support**: Implementação do `TRedisCacheProvider` para armazenamento centralizado de snapshots.
- **RESP Protocol**: Transporte leve e nativo (`TRedisTransport`) para comunicação com servidores Redis via sockets.
- **Hierarchical Invalidation**: Suporte a invalidação de cache por tabela usando Redis Sets, garantindo consistência em ambientes distribuídos.
- **Metadata Storage**: Integração de metadados no provedor Redis para compartilhamento global de esquemas.

## [0.12.0] - 2026-04-07
### Changed
- **Standard Rule Compliance**: Refatoração completa da nomenclatura de métodos protegidos e privados para aderir ao prefixo `_`, conforme as diretrizes do projeto.
- **Core Refactoring**: Padronização de hooks internos em `TDriverQuery` e `TDriverConnection` (`_TryApplyMetadataCache`, `_TrySaveMetadataCache`, `_RefreshMetadata`).
- **Documentation Alignment**: Correção dos caminhos de referência para as regras de linguagem Delphi em `.claude/references/conventions.md`.
- **Ecosystem Synchronization**: Atualização em lote de todos os 18 drivers e suas respectivas factories para garantir conformidade com as novas assinaturas da classe base.

## [0.11.0] - 2026-04-07
### Added
- **Unified Transaction Isolation Mapping**: Introdução do enum `TDBIsolationLevel` em `IDBTransaction` para gestão de transações agnóstica ao driver.
- **Provider Implementation**:
  - `FireDAC`: Suporte completo a níveis de isolamento nativos (`amReadCommitted`, `amSerializable`, etc.).
  - `UniDAC`: Mapeamento unificado com tratamento de colisão de tipos e suporte a logs de fallback.
  - `ZeosLib`: Implementação de mapeamento de isolamento com fallback inteligente para `Snapshot` (via Serializable).
- **Architecture Upgrade**: `TDriverTransaction` agora suporta `MonitorCallback` em toda a camada de drivers, permitindo rastreabilidade e logs de aviso em tempo de execução.
- **Compatibility Layer**: Atualização de todos os 18 drivers de transação para garantir integridade arquitetural e compilação do ecossistema.

## [0.10.0] - 2026-04-07
### Added
- **Bulk Operations Framework**: Introdução da interface `IDBBulkLoader` para inserções de alta performance em lote.
- **Native Driver Support**:
  - `FireDAC`: Implementação via **Array DML** (`TFDQuery`), otimizando o uso de buffers nativos do engine.
  - `UniDAC`: Implementação via **`TUniLoader`** com suporte a buffer agnóstico (push-based).
  - `ZeosLib`: Implementação via loop transacional garantindo compatibilidade genérica no Core.
- **Performance Benchmarking**: Novo projeto de teste `Tests.BulkLoad.dpr` em `Test Delphi/` para validação de ganhos de performance.
- **Core Integration**: Gancho `BulkLoader` adicionado a `IDBConnection` e `TDriverConnection` para acesso unificado.

## [0.9.0] - 2026-04-06
### Added
- **Core Orchestration**: Implementação do padrão Template Method em `TDriverConnection.ExecuteDirect` para orquestração centralizada de SQL.
- **Unified Monitoring**: Log de monitoramento e parâmetros agora disparados automaticamente pela classe base.
- **Auto-Invalidation**: Integração nativa com `TCacheManager` para invalidação de tabelas em comandos DML.
### Changed
- **Driver Migration**: Refatoração dos drivers `FireDAC`, `UniDAC`, `ZeosLib`, `DBExpress` e `SQLite3` (Native) para utilizar os novos hooks `_DoNativeExecuteDirect`.
- **Boilerplate Reduction**: Eliminação de código redundante de setup de queries temporárias nas classes descendentes.
- **Reliability**: Garantia de captura correta de `RowsAffected` em todos os drivers migrados.

## [0.8.2] - 2026-04-06
### Fixed
- Core: Fixed uninitialized `LTable` variable in `TDriverQuery.TrySaveMetadataCache` that caused metadata save failures.
- Drivers (SQLite3): Fixed misnamed implementation class `TDriverResultSetSQLite3` to match interface `TDriverDataSetSQLite3`.
- Drivers (SQLite3): Integrated `ACachedFieldDefs` into result dataset to effectively utilize metadata cache and avoid redundant engine calls.

## [0.8.1] - 2026-04-06
### Added
- **Universal Metadata Cache Support**: Extensão do sistema de cache para os drivers `ZeosLib`, `DBExpress` e `SQLite3 (Native)`.
- **Benchmark Suit**: Novo projeto de teste `Tests.Benchmark.Metadata.dpr` para validação de latência em cache hits.
### Changed
- **Core Refactoring**: Centralização da lógica de hooking em `TDriverQuery` (Core), eliminando duplicidade de código nos drivers.
- **Improved Monitoring**: Padronização dos logs de monitoramento de metadados (`[METADATA HIT]`, `[METADATA SAVED]`) em todos os drivers.
- **Driver dogfooding**: Refatoração completa de `FireDAC` e `UniDAC` para utilizarem os novos métodos base do Core.

## [0.8.0] - 2026-04-05
### Added
- **Metadata Cache Infrastructure (Part 1)**: Implementação da infraestrutura base para cache de metadados.
- Nova interface `IDBMetadataCache` para gestão de metadados serializados.
- Integrado suporte a `MetadataCache` em `IDBConnection` e `TDriverConnection`.
- Unidade `DataEngine.Metadata.Serializer.pas` para conversão de `TFieldDefs` <-> JSON.
- Unidade `DataEngine.Metadata.Manager.pas` para orquestração automática de persistência.
- Suporte a metadados no `TSQLiteCacheProvider` usando prefixo `META:`.
- **Driver Hooks**: Implementação de hooks em `TDriverQueryFireDAC` e `TDriverQueryUniDAC` para ignorar o `AutoGetFieldDefs` nativo quando houver cache HIT.
- Projeto de teste `TestsMetadataSerializer.dpr` para validação da integridade de dados.

## [0.7.0] - 2026-04-04
### Added
- **Auto-Invalidation (DML Interception)**: Invalidação automática de cache via interceptação de comandos.
- Método `InvalidateByTable` adicionado em `IDBCacheProvider`.
- Extração de nomes de tabela via Regex heurístico em `TCacheManager.ExtractTables`.
- Hooks de interceptação automática em `TDriverConnection` e `TDriverQuery` para `ExecuteDirect`.
- Mapeamento de dependências de tabela persistente em `TSQLiteCacheProvider`.
- Projeto de teste `TestsCacheInvalidation.dpr` para cobertura de cenários DML.

## [0.6.0] - 2026-04-04
### Added
- **Cache Persistency (SQLite/Disk)**: Implementação do provedor de cache em disco.
- Novo provedor `TSQLiteCacheProvider` utilizando SQLite local.
- Serialização binária de `IDBDataSetSnapshot` via formato FireDAC nativo.
- Suporte a TTL (Time To Live) persistente gravado em banco.
- Comando `Prune` para limpeza de registros expirados no disco.
- Adição de parâmetro `ATTL` opcional na interface `IDBCacheProvider.SetValue`.
- Projeto de teste CLI `TestsCachePersistency.dpr` para validação de persistência entre sessões.

## [0.5.0] - 2026-04-03
### Added
- **Cache Study & Prototype**: Introdução da infraestrutura de cache no Core.
- Interfaces `IDBCacheProvider` e `IDBDataSetSnapshot`.
- `TCacheManager` com suporte a hashing MD5 de SQL + Parâmetros.
- `TMemCacheProvider` para armazenamento LRU em memória.
- Mecanismo de isolamento de cursores via `CreateView` em snapshots de dados.
- Projeto de teste `TestsCacheStudy.dpr` para validação de performance e hits.

## [0.4.0] - 2026-04-03
### Added
- **Legacy & Niche Drivers**: Padronização completa dos drivers legados.
- Suporte a DBExpress, AbsoluteDB, IBExpress e outros (~10+ drivers).
### Changed
- Refatoração de namespaces para o padrão `DataEngine.*`.

## [0.3.0] - 2026-04-03
### Added
- **Medium Coverage Drivers**: Implementação dos drivers ADO, Memory e SQLDirect seguindo o blueprint FireDAC.

## [0.2.0] - 2026-04-03
### Added
- **Advanced Drivers**: Suporte padronizado para SQLite3, Zeos e UniDAC.

## [0.1.0] - 2026-04-03
### Added
- **Foundation**: Estrutura base do framework core.
- Driver FireDAC como referência arquitetural.
- Interfaces base para conexões, transações e queries.
