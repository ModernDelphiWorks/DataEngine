---
displayed_sidebar: dataengineSidebar
title: Overview Arquitetural
---

## Contexto

O **DataEngine** atua como um `bridge` (ponte) entre sua lógica de negócio e as bibliotecas nativas de acesso a dados (FireDAC, UniDAC, etc.). Sua responsabilidade é fornecer um conjunto de interfaces que garantam a interoperabilidade sem expor as particularidades de cada engine.

## Componentes Principais

- **Core Interfaces (`Source/Core/`):** Define o contrato (IDBConnection, IDBQuery) que deve ser respeitado por qualquer implementação de driver.
- **Drivers (`Source/Drivers/`):** Implementações concretas que traduzem as chamadas de interface para os componentes reais (ex: TFDConnection).
- **Factories:** Responsáveis pela injeção das instâncias de drivers coretos conforme a necessidade do projeto.
- **Metadata Cache:** Subsistema interno para otimizar a carga de metadados das tabelas (Performance).
- **Distributed Cache (Redis):** Provedor de cache (`TRedisCacheProvider`) para ambientes escaláveis, garantindo consistência global.
- **Bulk Operations Layer:** Interface `IDBBulkLoader` e implementações nos drivers para carregamento massivo de alta performance.
- **Async Execution Layer:** Infraestrutura `TDataEngineAsync` (baseada na PPL) para carregamento assíncrono (`OpenAsync`) e streaming de dados sem bloqueio da UI.
- **Connection Resiliency & Health Checks:** Sistema de detecção de falhas e automação de retries orquestrado pelo `TGuardConnection` e políticas de resiliência.
- **Advanced Observability System:** Coleta de métricas de alta precisão (Stopwatch), detecção de Slow Queries e telemetria via `IDBObserver`.



## Extensibilidade

Novos engines podem ser suportados criando um novo par de `Driver` + `Factory` sem necessidade de alterar o núcleo do framework.
- Herança de `TDriverConnection`
- Herança de `TDriverQuery` (os métodos internos agora utilizam o prefixo `_` como padrão, ex: `_InternalExecuteDirect`).
- Implementação da respectiva `IFactoryConnection`
