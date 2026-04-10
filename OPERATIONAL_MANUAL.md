# DataEngine Operational Manual v1.0.0

## 1. Introduction
**DataEngine** is a high-performance, modular, and extensible database engine framework for Delphi. It provides a robust abstraction layer, allowing developers to build database-agnostic applications with ease. By utilizing strong interface-driven design (`IDBConnection`, `IDBQuery`, `IDBDataSet`), DataEngine ensures your business logic remains isolated from the underlying database access components.

## 2. Architecture Overview
The framework is built on a "Factory-Driver" pattern:
- **Core Interfaces**: Defined in `DataEngine.FactoryInterfaces.pas`, these contracts govern all interactions.
- **Factory Layer**: Responsible for injecting specific engine implementations (FireDAC, UniDAC, etc.) into the core.
- **Driver Layer**: Translates generic calls from the core into engine-specific operations.
- **Agnostic Core**: The core modules (Cache, Snapshot, Pool) are strictly independent of any third-party data access components.

## 3. Supported Drivers
DataEngine supports an extensive range of database engines:
- **FireDAC** (Native Delphi)
- **UniDAC** (Devart)
- **ZeosLib** (Open Source)
- **DBExpress** (Legacy)
- **ADO** (Windows Native)
- **SQLite3 Native** (Engine-independent)
- **Mock Driver** (For Unit Testing)

### Initialization Example
```delphi
var
  LConn: IDBConnection;
begin
  // Using FireDAC for PostgreSQL
  LConn := TFactoryFireDAC.Create(MyFDConnection, dnPostgreSQL);
  LConn.Connect;
  
  // Now execute queries agnostically
  LConn.ExecuteDirect('UPDATE users SET active = 1');
end;
```

## 4. Multi-tenant Connection Pooling
Manage isolated connection pools for complex SaaS or Microservices environments using the `PoolManager`.

### Features
- **Tenant Isolation**: Each tenant gets its own dedicated pool.
- **Resource Limits**: Configurable max connections per pool.
- **Lifecycle Management**: Automatic connection lifetime control.

### Implementation
```delphi
LConn := PoolManager.AcquireConnection(
  'Tenant_001',
  function: IDBConnection
  begin
    Result := TFactoryFireDAC.Create(CreateNativeConn, dnMySQL);
  end
);
try
  // Use connection...
finally
  PoolManager.ReleaseConnection('Tenant_001', LConn);
end;
```

## 5. Advanced Observability
Monitor your database performance in real-time with the built-in Observer system. High-precision timing and event reporting give you full visibility into your data pipeline.

### Events Monitored
- `teQueryStart` / `teQueryEnd`: Execution time and SQL statement.
- `teError`: Detailed exception reporting.
- `teSlowQuery`: Automatic detection based on configurable thresholds.

### Usage
```delphi
FDBConnection.AddObserver(TMyCustomLogger.Create);
```

## 6. Caching and Performance
DataEngine features an intelligent, agnostic caching system designed to minimize database roundtrips.

- **Local Persistence**: Uses a native SQLite driver for disk-based caching.
- **Auto-Invalidation**: Detects DML operations and invalidates relevant cache entries.
- **DataSet Snapshots**: Created using `TInMemoryDataFactory`, allowing immutable memory views of any dataset.

## 7. Compliance and Standards
- **Requirements**: Delphi Seattle or superior.
- **Dependencies**: Zero mandatory external dependencies for the Core.
- **Stability**: v1.0.0 represents the stabilization of the engine-agnostic transition.

---
*Copyright © 2025-2026 Isaque Pinheiro. Licensed under Apache-2.0.*
