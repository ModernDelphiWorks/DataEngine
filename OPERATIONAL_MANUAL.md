# DataEngine Operational Manual v1.0.0

## 1. Introduction
DataEngine is a modular and extensible database engine framework for Delphi. It provides a clean abstraction over various database engines, allowing you to develop database-agnostic applications. By using DataEngine, your application code interacts with interfaces (`IDBConnection`, `IDBQuery`, `IDBDataSet`), making it independent of the underlying database access components (Engines).

## 2. Driver Configuration
DataEngine supports multiple engines through its Factory pattern. To use a specific engine, you need to instantiate its corresponding Factory.

### Supported Engines
- **FireDAC**: `TFactoryFireDAC`
- **DBExpress**: `TFactoryDBExpress`
- **UniDAC**: `TFactoryUniDAC`
- **ZeosLib**: `TFactoryZeos`
- **ADO**: `TFactoryADO`
- **SQLite3 Native**: `TFactorySQLite3`

### Example
```delphi
const
  DATABASE_PATH = '.\database.db3';
var
  LFDConn: TFDConnection;
begin
  LFDConn := TFDConnection.Create(nil);
  LFDConn.Params.DriverID := 'SQLite';
  LFDConn.Params.Database := DATABASE_PATH;
  LFDConn.LoginPrompt := False;

  // Injection
  FDBConnection := TFactoryFireDAC.Create(LFDConn, dnSQLite);
  FDBConnection.Connect;
end;
```

## 3. Multi-tenant Connection Pooling
The `PoolManager` allows you to manage multiple connection pools isolated by a `TenantID`. This is ideal for SaaS applications where each tenant has its own database or specific connection settings.

### Basic Usage
```delphi
var
  LConn: IDBConnection;
begin
  // Acquire connection for a specific tenant
  LConn := PoolManager.AcquireConnection(
    'Tenant_ABC',
    function: IDBConnection
    begin
      // Logic to create a new connection instance for this tenant
      Result := TFactoryFireDAC.Create(CreateTenantConnection('Tenant_ABC'), dnPostgreSQL);
    end,
    20,  // Max Connections for this tenant pool
    900  // Connection Lifetime in seconds
  );
  
  try
    // Perform database operations
    LConn.ExecuteDirect('SELECT 1');
  finally
    // Always release the connection back to the manager
    PoolManager.ReleaseConnection('Tenant_ABC', LConn);
  end;
end;
```

## 4. Observability and Monitoring
DataEngine features a built-in observability system. You can catch query execution metrics, slow queries, and errors by registering an observer.

### Implementing an Observer
```delphi
type
  TLoggerObserver = class(TInterfacedObject, IDBObserver)
  public
    procedure OnNotify(const AParam: TMonitorParam);
  end;

procedure TLoggerObserver.OnNotify(const AParam: TMonitorParam);
begin
  if AParam.EventType = teQueryEnd then
    WriteLn(Format('SQL: %s | Time: %d ms', [AParam.Command, AParam.ExecutionTime]));
end;

// Registration
FDBConnection.AddObserver(TLoggerObserver.Create);
```

## 5. Persistence and Cache
DataEngine provides an agnostic caching layer. You can use SQLite for local caching or Redis for distributed caching.

### SQLite Local Cache
```delphi
uses DataEngine.SQLiteCacheProvider;

FDBConnection.SetCacheProvider(TSQLiteCacheProvider.Create('cache.db'));
```

### Automatic Invalidation
The cache manager can automatically invalidate entries based on DML operations detected in the SQL strings.

## 6. Core Agnostic Design
The Core modules (`Source/Core`) are designed to be 100% independent of specific engines like FireDAC or UniDAC. All in-memory data operations for snapshots and cache use a generic `TInMemoryDataFactory`, allowing full portability across different Delphi editions and potentially other compilers (like FPC).

### Snapshots
Snapshots are immutable views of data in memory.
```delphi
LSnapshot := TCacheManager.CreateSnapshot(FDBQuery.ExecuteQuery);
// Create many views/clones from the same snapshot without re-querying the DB
LView1 := LSnapshot.CreateView;
LView2 := LSnapshot.CreateView;
```

## 7. Best Practices
1. **Always Use Interfaces**: Avoid casting `IDBConnection` back to concrete classes.
2. **Contextual Pooling**: Use `TenantID` to keep your pools clean and manageable.
3. **Transaction Management**: Use the built-in `IDBTransaction` methods for safe data mutations.
4. **Clean up**: Ensure connections are always released or disconnected in `finally` blocks.
