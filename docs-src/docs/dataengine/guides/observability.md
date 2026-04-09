---
displayed_sidebar: dataengineSidebar
title: Observabilidade Avançada
---

# Observabilidade Avançada

O DataEngine inclui um sistema robusto de observabilidade para monitorar o desempenho, coletar métricas de precisão e identificar gargalos em tempo real.

## Conceitos Chave

- **Observador (Observer):** Interface `IDBObserver` que recebe notificações sobre o ciclo de vida da query.
- **Métricas de Precisão:** Coleta automática de `ExecutionTime` e `FetchTime` usando `TStopwatch` (alta resolução).
- **Slow Query Tracking:** Identificação automática de consultas que excedem um limite predefinido.
- **Telemetria Estruturada:** Eventos categorizados (Start, End, Error, Metric) com dados detalhados.

## Configurando o Limite de Slow Query

Por padrão, consultas que levam mais de **1000ms** são marcadas como *Slow Queries*. Você pode ajustar esse valor na conexão:

```pascal
LConnection.SlowQueryThreshold := 500; // Define o limite para 500ms
```

> **Novidade em v0.17.0:** O `SlowQueryThreshold` configurado na conexão é agora propagado automaticamente para todas as queries criadas via `CreateQuery`. Não é mais necessário configurar o threshold individualmente em cada query.

## Registrando um Observador

Para monitorar as operações, implemente a interface `IDBObserver` (ou herde de `TMetricCollector`) e registre-a na conexão.

```pascal
type
  TMyMonitor = class(TInterfacedObject, IDBObserver)
    procedure OnEvent(const AParam: TMonitorParam);
  end;

procedure TMyMonitor.OnEvent(const AParam: TMonitorParam);
begin
  case AParam.EventType of
    etMetric:
      WriteLn(Format('Query %s: %dms', [AParam.SQL, AParam.ExecutionTime]));
    etSlowQuery:
      WriteLn(Format('ALERTA: Query lenta detectada: %s', [AParam.SQL]));
    etError:
      WriteLn(Format('Erro na query: %s - Mensagem: %s', [AParam.SQL, AParam.ErrorMessage]));
  end;
end;

// Uso
var
  LMonitor: IDBObserver;
begin
  LMonitor := TMyMonitor.Create;
  LConnection.AddObserver(LMonitor);
  // ...
  LConnection.RemoveObserver(LMonitor); // Remover quando nao for mais necessario
end;
```

## Metricas Coletadas

| Metrica | Descricao |
|---------|-----------|
| `ExecutionTime` | Tempo total de execucao no banco de dados isolado da extração de dados (ms). |
| `FetchTime` | Tempo gasto recuperando os registros resultante através da progressão do loop nativo (ms). Isolado desde v0.17.1 para maior precisão em modo assíncrono. |
| `RowsAffected` | Quantidade de registros afetados para comandos DML. |
| `TotalTime` | Soma de ExecutionTime + FetchTime. |

## Gerenciamento de Observadores

O `TDriverConnection` usa o padrão arquitetural **Snapshot (Lock-on-Clone)** nativamente a partir da v0.17.1, garantindo travamento mínimo via `TCriticalSection` apenas durante a cópia atômica (`ToArray`) da lista de referência. Múltiplos observadores podem ser registrados, removidos e ativados em paralelo por tasks assíncronas (PPL) em altíssima concorrência sem Deadlocks ou Access Violations (AVs).

| Metodo | Descricao |
|--------|-----------|
| `AddObserver(AObserver)` | Registra um novo observador na conexao. |
| `RemoveObserver(AObserver)` | Remove um observador previamente registrado. |

## Compatibilidade

O sistema de observabilidade mantém compatibilidade total com o `MonitorCallback` legado, garantindo que logs existentes continuem funcionando enquanto você migra para observadores estruturados.
