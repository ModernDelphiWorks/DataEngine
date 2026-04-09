---
displayed_sidebar: dataengineSidebar
title: Execução Assíncrona e Stream
---

# Execução Assíncrona e Stream

O DataEngine v0.14.0 introduz suporte nativo para operações assíncronas, permitindo que consultas pesadas sejam executadas sem bloquear a thread principal (UI) da sua aplicação Delphi.

## O Motor `OpenAsync`

Diferente do `Open` tradicional, o `OpenAsync` utiliza a **PPL (Parallel Programming Library)** interna para despachar a tarefa de busca de dados para uma thread de background.

### Exemplo de Uso Básico

```delphi
var
  LQuery: IDBQuery;
begin
  LQuery := FConnection.CreateQuery;
  LQuery.CommandText := 'SELECT * FROM Vendas_Pesadas';
  
  LQuery.ExecuteQuery.OpenAsync(
    procedure // OnComplete (Executa na Main Thread)
    begin
      ShowMessage('Dados carregados com sucesso!');
    end,
    procedure(E: Exception) // OnError (Executa na Main Thread)
    begin
      ShowMessage('Erro no carregamento: ' + E.Message);
    end
  );
end;
```

## Modos de Fetch (Streaming)

Através da propriedade `FetchOptions`, você pode controlar como os dados são trazidos do banco:

| Modo | Descrição |
|------|-----------|
| `fmAll` | (Padrão) Traz todos os registros de uma vez. |
| `fmManual` | O desenvolvedor controla o lote de registros através do `Next`. |
| `fmOnDemand` | Traz registros em lotes automaticamente conforme a navegação (Ideal para Grids). |

### Configurando o Fetch

```delphi
LQuery.FetchOptions := TFetchOptions.Create(fmOnDemand, 50); // Lotes de 50
```

## Arquitetura de Sincronização

O DataEngine utiliza internamente a classe `TDataEngineAsync` para orquestrar:
1. **Background Execution**: Uso de `TTask.Run`.
2. **UI Notification**: Uso de `TThread.Queue` para garantir que o acesso a componentes visuais nos callbacks seja seguro contra Access Violations.

:::info
Mesmo que o driver nativo ainda não tenha sido otimizado para streaming real em Slices futuros, o fallback do Core garante que a UI permaneça responsiva enquanto a thread de background aguarda a resposta do banco.
:::
