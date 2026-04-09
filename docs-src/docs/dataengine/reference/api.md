---
displayed_sidebar: dataengineSidebar
title: API - Referência
---

## Principais Interfaces e Métodos

A interface `IDBConnection` é o ponto de entrada principal do framework.

| Interface | Método | Descrição |
|-----------|--------|-----------|
| `IDBConnection` | `Connect` | Estabelece a conexão com o banco de dados. |
| `IDBConnection` | `Disconnect` | Encerra a conexão. |
| `IDBConnection` | `IsAlive` | Executa um Health Check (Ping) para validar se a conexão permanece ativa. |
| `IGuardConnection`| `Resilience` | Acesso à configuração da `IDBResiliencePolicy` (Retries, Delays). |
| `IDBConnection` | `BulkLoader` | Retorna uma interface `IDBBulkLoader` para operações em massa de alto desempenho. |
| `IDBConnection` | `RegisterObserver` | Registra um observador (`IDBObserver`) para monitorar métricas e eventos. |
| `IDBConnection` | `SlowQueryThreshold`| Define o limite (ms) para que uma consulta seja considerada lenta. |

| `IDBBulkLoader` | `Prepare` | Prepara o comando SQL de inserção em massa baseado nos parâmetros definidos. |
| `IDBBulkLoader` | `SetValue` | Atribui o valor de um campo para um registro específico dentro do lote atual. |
| `IDBBulkLoader` | `Execute` | Executa a carga do lote de registros no banco de dados. |
| `IDBQuery` | `ExecuteQuery` | Executa o comando SQL e retorna um `IDBDataSet`. |
| `IDBQuery` | `FetchOptions` | Configura o modo de carregamento (fmAll, fmManual, fmOnDemand). |
| `IDBQuery` | `ParamByName` | Fornece acesso aos parâmetros da consulta SQL. |
| `IDBDataSet` | `OpenAsync` | Executa a abertura do dataset de forma assíncrona (PPL). |
| `IDBDataSet` | `First/Next/Prior/Last` | Métodos de navegação padrão. |
| `IDBDataSet` | `FieldByName` | Retorna o objeto `TField` de um campo do resultado. |
| `IDBDataSet` | `Eof/Bof` | Verifica o estado do cursor no DataSet. |
| `IDBCacheProvider` | `InvalidateByTable` | Invalida todas as entradas de cache vinculadas a uma tabela (Distribuído no Redis). |
| `TRedisCacheProvider`| `Create` | Inicializa o provedor de cache apontando para um servidor Redis. |


## Regras e Contratos

- **Gerenciamento de Memória:** O DataEngine utiliza `ARC` (Automatic Reference Counting) via interfaces (`IInterface`), o que desobriga o desenvolvedor de chamar `.Free` manualmente nas interfaces (exceto em casos especiais de componentes nativos gerenciados).
- **Independência de Engine:** Todas as consultas devem ser escritas de forma a rodar em qualquer driver suportado. Casos de SQL específico devem ser tratados na camada superior.
- **Log Automático:** Operações de escrita (`INSERT/UPDATE/DELETE`) geram log interno automático no Core se configurado.
