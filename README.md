# DBEBr / DataEngine Framework for Delphi & Lazarus

[![Delphi XE+](https://img.shields.io/badge/Delphi-XE%20or%20superior-blue.svg)]()
[![Lazarus Compatible](https://img.shields.io/badge/Lazarus-Compatible-orange.svg)]()
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

*   [🇬🇧 English](#-english)
*   [🇧🇷 Português](#-português)

---

## 🇬🇧 English

**DataEngine** is a high-performance, modular, and extensible database engine abstraction framework for Delphi and Lazarus. It provides a robust and uniform interface layer, allowing developers to build completely database-agnostic applications with ease.

<p align="center">
  <a href="https://www.isaquepinheiro.com.br">
    <img src="https://github.com/HashLoad/DBEBr/blob/master/Images/dbebr_framework.png" width="200" height="200" alt="DataEngine Logo">
  </a>
</p>

### 🏛 Supported Platforms
*   **Delphi XE or superior** (VCL, FMX, Console)
*   **Lazarus / FreePascal** (Compatible Core)

### ⚙️ Installation
To install using [`boss`]:
```sh
boss install "https://github.com/HashLoad/dbebr"
```

---

### 🚀 Key Features

#### 1. Database Independence
Build your application once and run it on any engine. DataEngine abstracts engines like FireDAC, DBExpress, UniDac, Zeos, and more.
*   **Factory Pattern:** Inject specific engines (FireDAC, UniDAC, etc.) into a single unified `IDBConnection` interface.
*   **Agnostic Core:** Core features (Cache, Snapshot, Pool) are strictly independent of third-party components.

#### 2. Multi-tenant Connection Pooling
Efficiently manage connection pools for SaaS and Microservices.
*   **Tenant Isolation:** Dedicated, separated pools per tenant (Multi-tenant).
*   **Resource Management:** Configurable max connections and reuse/eviction policies.

#### 3. Advanced Observability & Monitoring
Real-time database query monitoring with a built-in high-precision observer system.
*   **Execution Metrics:** Track high-precision query execution times.
*   **Slow Query Tracking:** Automatically detect and log queries exceeding configured thresholds.
*   **Structured Events:** Standardized hook events for `Start`, `End`, `Error`, and `Metric` execution.

#### 4. Intelligent Caching & Snapshots
Minimize database roundtrips with an engine-agnostic local caching system.
*   **Local Persistence:** Integrated SQLite native driver for local disk-based snapshots.
*   **Auto-Invalidation:** Heuristic detection of DML operations to automatically clear relevant cache entries.

---

### ⚡️ Quick Start

#### Basic Connection (FireDAC Instance)
```delphi
procedure TMyDataModule.Setup;
begin
  // Standardize your connection to use DataEngine interfaces
  FDBConnection := TFactoryFireDAC.Create(MyFDConnection, dnSQLite);
  FDBConnection.Connect;
end;
```

#### Simple Query Execution
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

#### Transaction Management
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

#### Using Connection Pool (Multi-tenant)
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

### ⛏️ Contributing
We love contributions! Feel free to open issues or submit pull requests.

1.  Fork the project.
2.  Create your feature branch (`git checkout -b feature/AmazingFeature`).
3.  Commit your changes (`git commit -m 'Add some AmazingFeature'`).
4.  Push to the branch (`git push origin feature/AmazingFeature`).
5.  Open a Pull Request.

### 📬 Contact & Support
*   **Telegram**: [HashLoad Channel](https://t.me/hashload)
*   **Website**: [isaquepinheiro.com.br](https://www.isaquepinheiro.com.br)

### 💲 Donation
[![Doação](https://img.shields.io/badge/PagSeguro-contribua-green)](https://pag.ae/bglQrWD)

---

## 🇧🇷 Português

**DataEngine** é um framework de alta performance, modular e extensível para abstração de motores de banco de dados em Delphi e Lazarus. Ele fornece uma camada de interface robusta e uniforme, permitindo que desenvolvedores criem aplicações totalmente agnósticas a banco de dados com extrema facilidade.

### 🏛 Plataformas Suportadas
*   **Delphi XE ou superior** (VCL, FMX, Console)
*   **Lazarus / FreePascal** (Core Compatível)

### ⚙️ Instalação
Para instalar usando o [`boss`]:
```sh
boss install "https://github.com/HashLoad/dbebr"
```

---

### 🚀 Recursos Principais

#### 1. Independência de Banco de Dados
Construa sua aplicação uma vez e execute-a em qualquer SGBD. O DataEngine abstrai drivers como FireDAC, DBExpress, UniDac, Zeos, entre outros.
*   **Factory Pattern:** Injete motores específicos (FireDAC, UniDAC, etc.) sob uma única interface unificada `IDBConnection`.
*   **Core Agnóstico:** Recursos principais (Cache, Snapshot, Pool) são estritamente independentes de componentes de terceiros.

#### 2. Pooling de Conexões Multi-tenant
Gerenciamento altamente eficiente de pools de conexão voltado a arquiteturas SaaS e Microsserviços.
*   **Isolamento de Tenants:** Pools dedicados e separados por tenant (Multi-tenant).
*   **Gestão de Recursos:** Políticas configuráveis de máximo de conexões abertas e tempo de vida útil de recursos.

#### 3. Observabilidade Avançada
Monitoramento em tempo real de comandos SQL executados com um sistema de observadores de alta precisão integrado.
*   **Métricas de Execução:** Rastreamento de tempo de execução de queries com alta exatidão.
*   **Slow Queries:** Detecção e gravação automática de logs para consultas que excedem thresholds (limites) de tempo configurados.
*   **Eventos Estruturados:** Hooks padronizados de monitoramento para os eventos `Start`, `End`, `Error` e `Metric`.

#### 4. Cache Inteligente & Snapshots
Minimize requisições de rede ao banco de dados com um sistema agnóstico de cache local.
*   **Persistência Local:** Driver SQLite nativo integrado para gravação de snapshots em disco local.
*   **Invalidação Automática:** Detecção heurística de comandos DML para invalidar e limpar de forma inteligente os caches correspondentes.

---

### ⚡️ Início Rápido

#### Conexão Básica (Instancia FireDAC)
```delphi
procedure TMyDataModule.Setup;
begin
  // Padronize sua conexão para usar as interfaces do DataEngine
  FDBConnection := TFactoryFireDAC.Create(MyFDConnection, dnSQLite);
  FDBConnection.Connect;
end;
```

#### Execução de Consulta Simples
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
    // Interfaces gerenciam a destruição automática do recurso (ARC)
  end;
end;
```

#### Controle de Transações
```delphi
FDBConnection.StartTransaction;
try
  // Suas operações de banco aqui
  FDBConnection.Commit;
except
  FDBConnection.Rollback;
  raise;
end;
```

#### Utilizando o Pool de Conexões (Multi-tenant)
```delphi
LConn := PoolManager.AcquireConnection(
  'Tenant_A',
  function: IDBConnection
  begin
    Result := TFactoryFireDAC.Create(CreateNativeConn, dnPostgreSQL);
  end
);
try
  // Use a conexão adquirida...
finally
  PoolManager.ReleaseConnection('Tenant_A', LConn);
end;
```

---

### ⛏️ Contribuição
Adoramos contribuições! Sinta-se à vontade para abrir issues ou enviar pull requests.

1.  Faça um Fork do projeto.
2.  Crie sua branch de recurso (`git checkout -b feature/MinhaNovaFeature`).
3.  Faça o commit de suas alterações (`git commit -m 'Adiciona MinhaNovaFeature'`).
4.  Faça o push para a branch (`git push origin feature/MinhaNovaFeature`).
5.  Abra um Pull Request.

### 📬 Contato & Suporte
*   **Telegram**: [Canal HashLoad](https://t.me/hashload)
*   **Website**: [isaquepinheiro.com.br](https://www.isaquepinheiro.com.br)

### 💲 Doação
[![Doação](https://img.shields.io/badge/PagSeguro-contribua-green)](https://pag.ae/bglQrWD)

---
*Copyright © 2025-2026 Isaque Pinheiro. Licensed under Apache-2.0.*
