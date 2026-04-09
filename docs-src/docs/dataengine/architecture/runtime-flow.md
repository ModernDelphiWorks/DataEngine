---
displayed_sidebar: dataengineSidebar
title: Runtime Flow
---

## Fluxo Principal de Execução

1. **Criação da Factory:** O desenvolvedor instancia uma factory específica (ex: `TFactoryFireDAC.Create`) passando o componente nativo de conexão.
2. **Obtenção da Interface:** A factory retorna uma instância de `IDBConnection` (normalmente um `TDriverConnection`).
3. **Execução de Comando:** Ao chamar `ExecuteQuery` ou `ExecuteDirect`:
    - **Health Check (Se via Guard/Pool):** O sistema verifica se a conexão está ativa (`IsAlive`). Se não, tenta reconectar.
    - **Check Cache:** Se `CacheConfig.Enabled` for true, o Core consulta o `IDBCacheProvider` (Local ou Redis).
    - **Cache Hit:** Retorna o snapshot armazenado sem executar o SQL no banco.
    - **Cache Miss / DML:**
        - O Core registra o log de execução.
        - Em caso de DML (`INSERT/UPDATE/DELETE`), o sistema chama `InvalidateByTable`.
        - No caso do Redis, isso remove as chaves globalmente para todas as instâncias.
        - **Métricas e Observabilidade:** O framework inicia o `TStopwatch` para cronometrar a execução.
        - O Driver específico executa o comando no banco de dados.
        - **Cálculo de Métricas:** Após a execução e o fetch, o sistema calcula `ExecutionTime` e `FetchTime`.
        - **Notificações:** Se houver observadores registrados, os eventos `etMetric` e (se aplicável) `etSlowQuery` são disparados.
        - **Retry Logic:** Se ocorrer um erro de rede, o `TGuardConnection` intercepta, avalia a `ResiliencePolicy` e repete a operação se aplicável. Notifica `etError` aos observadores.
        - Se for um `SELECT` com cache habilitado, o resultado é persistido no provedor de cache antes do retorno.
4. **Retorno:** O resultado é devolvido para a camada superior de forma agnóstica.



## Pontos de Erro

- **Falha de Conexão:** Gerenciada pelo `IDBConnection.Connect` com propagação de exceções nativas. Se orquestrada pelo Guard, ativa o fluxo de **Resiliência**.
- **Falha de Sintaxe SQL:** O driver específico captura o erro e o DataEngine repassa via exceção Delphi comum (`Exception`).
- **Cache Miss:** Caso o metadado não esteja em cache, o DataEngine carrega automaticamente na primeira consulta.
