---
displayed_sidebar: dataengineSidebar
title: Quickstart
---

## Pré-requisitos

- Delphi XE ou superior.
- [Boss](https://github.com/HashLoad/boss) (opcional, recomendado para gerenciamento de dependências).
- Um engine de conexão instalado (FireDAC, UniDAC, Zeos, etc.) ou o **JSON Store Driver** nativo.
- Servidor **Redis** (opcional, para uso de cache distribuído).


## Instalação

```sh
boss install ModernDelphiWorks/DataEngine
```

## Exemplo Mínimo

```delphi
uses
  DataEngine.FactoryFireDac,
  DataEngine.FactoryInterfaces;

var
  FConn: IDBConnection;
  FQuery: IDBQuery;
  LValue: string;
begin
  // Inicialização (FireDAC com SQLite)
  FConn := TFactoryFireDAC.Create(FDConnection1, dnSQLite);
  FConn.Connect;
  
  try
    FQuery := FConn.CreateQuery;
    FQuery.CommandText := 'SELECT NOME FROM CLIENTE WHERE ID = :ID';
    FQuery.ParamByName('ID').AsInteger := 1;
    
    LValue := FQuery.ExecuteQuery.FieldByName('NOME').AsString;
    ShowMessage(LValue);
  finally
    FConn.Disconnect;
  end;
end;
```

## Configuração de Cache Distribuído (Redis)

O DataEngine permite compartilhar o cache de consultas entre múltiplas instâncias usando Redis.

```delphi
uses
  DataEngine.CacheManager,
  DataEngine.RedisCacheProvider,
  DataEngine.CacheTypes;

begin
  // Configura o provedor global de cache como Redis
  TCacheManager.Instance.CacheProvider := TRedisCacheProvider.Create('localhost', 6379);
  
  // Ativa o cache na query
  FQuery := FConn.CreateQuery;
  FQuery.CacheConfig.Enabled := True;
  FQuery.CacheConfig.ExpirationMinutes := 10;
  FQuery.CommandText := 'SELECT * FROM PRODUTOS';
  FQuery.ExecuteQuery; // Se existir no Redis, não toca no banco.
end;
```


## Próximos Passos

- [Arquitetura](../architecture/overview.md)
- [API Reference](../reference/api.md)
