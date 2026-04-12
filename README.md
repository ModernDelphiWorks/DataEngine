# DBEBr / DataEngine Framework for Delphi

**DataEngine** is a high-performance, modular, and extensible database engine framework for Delphi. It provides a robust abstraction layer, allowing developers to build database-agnostic applications with ease.

<p align="center">
  <a href="https://www.isaquepinheiro.com.br">
    <img src="https://github.com/HashLoad/DBEBr/blob/master/Images/dbebr_framework.png" width="200" height="200">
  </a>
</p>

## 🏛 Supported Platforms
*   **Delphi XE or superior**
*   **Lazarus / FreePascal** (Compatible Core)

## ⚙️ Installation
Installation using [`boss install`]:
```sh
boss install "https://github.com/HashLoad/dbebr"
```

---

## 🚀 Key Features

### 1. Database Independence
Build your application once and run it on any engine. DataEngine abstracts engines like FireDAC, DBExpress, UniDac, Zeos, and more.
- **Factory Pattern**: Inject specific engines (FireDAC, UniDAC, etc.) into a single `IDBConnection` interface.
- **Agnostic Core**: Core features (Cache, Snapshot, Pool) are strictly independent of third-party components.

### 2. Multi-tenant Connection Pooling
Efficiently manage connection pools for SaaS and Microservices.
- **Tenant Isolation**: Dedicated pools per tenant.
- **Resource Management**: Configure max connections and reuse policies.

### 3. Advanced Observability
Real-time database monitoring with a built-in observer system.
- **Execution Metrics**: Track high-precision execution times.
- **Slow Query Tracking**: Automatically detect and log queries exceeding thresholds.
- **Structured Events**: Support for `Start`, `End`, `Error`, and `Metric` events.

### 4. Intelligent Caching
Minimize database roundtrips with an engine-agnostic caching system.
- **Local Persistence**: Integrated SQLite native driver for disk-based snapshots.
- **Auto-Invalidation**: Heuristic detection of DML operations to clear relevant cache entries.

---

## ⚡️ Quick Start

### Basic Connection (FireDAC Instance)
```delphi
procedure TMyDataModule.Setup;
begin
  // Standardize your connection to use DataEngine interfaces
  FDBConnection := TFactoryFireDAC.Create(MyFDConnection, dnSQLite);
  FDBConnection.Connect;
end;
```

### Simple Query Execution
```delphi
procedure TMyDataModule.UpdateUser;
var
  LQuery: IDBQuery;
begin
  LQuery := FDBConnection.CreateQuery;
  try
    LQuery.CommandText := 'UPDATE users SET active = 1 WHERE id = :id';
    LQuery.ParamByName('id').AsInteger := 123;
    LQuery.ExecuteDirect;
  finally
    // Interfaces handle cleanup automatically (ARC enabled)
  end;
end;
```

### Transaction Management
```delphi
FDBConnection.StartTransaction;
try
  // Your database operations here
  FDBConnection.Commit;
except
  FDBConnection.Rollback;
  raise;
end;
```

### Using Connection Pool (Multi-tenant)
```delphi
LConn := PoolManager.AcquireConnection(
  'Tenant_A',
  function: IDBConnection
  begin
    Result := TFactoryFireDAC.Create(CreateNativeConn, dnPostgreSQL);
  end
);
try
  // Use connection...
finally
  PoolManager.ReleaseConnection('Tenant_A', LConn);
end;
```

---

## ⛏️ Contributing
We love contributions! Feel free to open issues or submit pull requests.

1.  Fork the project.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

## 📬 Contact & Support
- **Telegram**: [HashLoad Channel](https://t.me/hashload)
- **Website**: [isaquepinheiro.com.br](https://www.isaquepinheiro.com.br)

## 💲 Donation
[![Doação](https://img.shields.io/badge/PagSeguro-contribua-green)](https://pag.ae/bglQrWD)
---
*Copyright © 2025-2026 Isaque Pinheiro. Licensed under Apache-2.0.*
