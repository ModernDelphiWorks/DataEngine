# DataEngine Framework for Delphi & Lazarus

[![Delphi XE+](https://img.shields.io/badge/Delphi-XE%20or%20superior-blue.svg)]()
[![Lazarus Compatible](https://img.shields.io/badge/Lazarus-Compatible-orange.svg)]()
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

*   [🇬🇧 English](#-english)
*   [🇧🇷 Português](#-português)

---

## 🇬🇧 English

**DataEngine** is a high-performance, modular, and extensible database engine abstraction framework for Delphi and Lazarus. It provides a robust, uniform interface layer that decouples applications from database-specific drivers, allowing developers to write database-agnostic code effortlessly. With native support for multi-tenant connection pooling, intelligent query monitoring, and advanced client-side caching, DataEngine is built for high-throughput enterprise systems, microservices, and SaaS applications.

### 🚀 Key Features

*   **Database Engine Independence:** Write code once and run it on any RDBMS. Abstract and switch between FireDAC, dbExpress, UniDAC, Zeos, and more.
*   **Factory-Based DI:** Inject specific engine implementations (e.g., FireDAC, UniDAC) into a unified `IDBConnection` interface.
*   **Multi-tenant Connection Pooling:** Efficiently manage high-concurrency connections with tenant isolation, custom eviction policies, and resource capping.
*   **Observability & Query Metrics:** Built-in high-precision execution monitoring to detect slow queries and track performance statistics.
*   **Agnostic Client Caching & Snapshots:** Minimize network latency using local disk-based or in-memory caches, with intelligent heuristics to auto-invalidate cache entries upon DML operations.

### 🏛 Compatibility Matrix

| Environment / IDE | Platform / Compiler | Connection Pooling | Client Caching |
| :--- | :--- | :---: | :---: |
| **Delphi XE or superior** | VCL, FMX, Console (Win/Linux/macOS/iOS/Android) | ✅ Yes | ✅ Yes |
| **Lazarus / FreePascal** | LCL, Console (Cross-platform) | ✅ Yes | ✅ Yes |

### 🐧 Cross-Platform Build — Win32 / Win64 / Linux64 (verified)

> **✅ Verified 2026-06-20** in a real production backend: DataEngine compiles as a dependency on **Win32, Win64 and Linux64** (`dcclinux64`), and the Linux server boots through FireDAC connection setup. macOS/iOS/Android follow from the Delphi RTL but are **not build-verified** here yet.

Windows-only pieces are guarded for non-Windows targets: the `MidasLib` (`TClientDataSet` runtime) is `{$IFDEF MSWINDOWS}`-guarded, and FireDAC is registered cross-platform in `DataEngine.DriverFireDac` — `FireDAC.Stan.Def` + `FireDAC.Phys.FB` + the **headless** `FireDAC.ConsoleUI.Wait` — so console/Linux builds register the driver factory without the VCL wait-UI.

**Building a consumer app for Linux64:**
1. Install the Linux 64-bit platform — RAD Studio GetIt, or `GetItCmd -if=delphi_linux -ae`.
2. Provide a Linux SDK — the RAD Studio SDK Manager (+ PAServer), **or** assemble a sysroot from a WSL/Linux toolchain (`build-essential`) and pass `--syslibroot` / `--libpath` to `dcclinux64`.
3. Compile with `dcclinux64` via a console build harness (generate a response file from the project search path to bypass the Win32 32 KB command-line limit).

### ⚙️ Installation

To install using the package manager [**Boss**](https://github.com/HashLoad/boss):

```sh
boss install DBEngine4D
```

> [!NOTE]
> For historical registry reasons on Boss, the package name is declared as **DBEngine4D** in its manifest, but the official framework name is **DataEngine**.

---

### ⚡️ Quick Start

#### 1. Basic Connection Setup (FireDAC Factory)
```delphi
uses
  DataEngine.Factory.FireDAC,
  DataEngine.Interfaces;

var
  FDBConnection: IDBConnection;
begin
  // Establish an engine-agnostic connection using the FireDAC factory
  FDBConnection := TFactoryFireDAC.Create(MyFDConnection, dnSQLite);
  FDBConnection.Connect;
end;
```

#### 2. Fluent Query Execution
```delphi
var
  LQuery: IDBQuery;
begin
  LQuery := FDBConnection.CreateQuery;
  try
    LQuery.CommandText := 'UPDATE users SET active = 1 WHERE id = :id';
    LQuery.ParamByName('id').AsInteger := 123;
    LQuery.ExecuteDirect;
  finally
    // Interfaces manage cleanup automatically via ARC
  end;
end;
```

#### 3. Transaction Management
```delphi
FDBConnection.StartTransaction;
try
  // Database operations
  FDBConnection.Commit;
except
  FDBConnection.Rollback;
  raise;
end;
```

#### 4. Connection Pooling (Multi-tenant)
```delphi
var
  LConn: IDBConnection;
begin
  LConn := PoolManager.AcquireConnection(
    'Tenant_A',
    function: IDBConnection
    begin
      Result := TFactoryFireDAC.Create(CreateNativeConn, dnPostgreSQL);
    end
  );
  try
    // Use your connection here
  finally
    PoolManager.ReleaseConnection('Tenant_A', LConn);
  end;
end;
```

---

## 🇧🇷 Português

**DataEngine** é um framework de alta performance, modular e extensível para abstração de motores de banco de dados em Delphi e Lazarus. Ele fornece uma camada de interface uniforme e robusta que desacopla a aplicação dos drivers de banco de dados específicos, permitindo que desenvolvedores criem softwares totalmente agnósticos a banco de dados de maneira simplificada. Projetado para sistemas corporativos, microsserviços e aplicações SaaS com alta carga, o DataEngine oferece pooling de conexões multi-tenant, monitoramento inteligente de consultas e cache avançado local.

### 🚀 Recursos Principais

*   **Independência de Bancos de Dados:** Escreva seu código uma vez e rode em qualquer SGBD. Abstraia drivers como FireDAC, dbExpress, UniDAC, Zeos e outros.
*   **Injeção via Factory Pattern:** Injete implementações específicas (como FireDAC ou UniDAC) sob uma única interface unificada `IDBConnection`.
*   **Pooling de Conexões Multi-tenant:** Gerencie eficientemente centenas de conexões simultâneas com isolamento estrito de tenants e limpeza ativa de recursos ociosos.
*   **Observabilidade e Métricas de Consulta:** Rastreamento estruturado de tempo de execução com hooks automáticos para registrar slow queries e coletar estatísticas de execução.
*   **Cache e Snapshots Agnósticos:** Reduza a latência de rede salvando dados localmente em disco ou memória, com invalidação heurística automática durante operações DML.

### 🏛 Matriz de Compatibilidade

| Ambiente / IDE | Plataforma / Compilador | Pooling de Conexões | Cache Local |
| :--- | :--- | :---: | :---: |
| **Delphi XE ou superior** | VCL, FMX, Console (Win/Linux/macOS/iOS/Android) | ✅ Sim | ✅ Sim |
| **Lazarus / FreePascal** | LCL, Console (Multiplataforma) | ✅ Sim | ✅ Sim |

### 🐧 Build Multiplataforma — Win32 / Win64 / Linux64 (verificado)

> **✅ Verificado em 2026-06-20** num backend real em produção: o DataEngine compila como dependência em **Win32, Win64 e Linux64** (`dcclinux64`), e o servidor Linux sobe até a configuração de conexão FireDAC. macOS/iOS/Android seguem da RTL Delphi, mas **ainda não foram verificados** em build aqui.

As partes Windows-only ficam guardadas para alvos não-Windows: o `MidasLib` (runtime do `TClientDataSet`) está sob `{$IFDEF MSWINDOWS}`, e o FireDAC é registrado de forma multiplataforma em `DataEngine.DriverFireDac` — `FireDAC.Stan.Def` + `FireDAC.Phys.FB` + o `FireDAC.ConsoleUI.Wait` **headless** — para builds console/Linux registrarem o factory do driver sem o wait-UI da VCL.

**Para buildar um app consumidor no Linux64:**
1. Instale a plataforma Linux 64-bit — RAD Studio GetIt, ou `GetItCmd -if=delphi_linux -ae`.
2. Forneça um SDK Linux — o SDK Manager do RAD Studio (+ PAServer), **ou** monte um sysroot a partir de um toolchain WSL/Linux (`build-essential`) e passe `--syslibroot` / `--libpath` ao `dcclinux64`.
3. Compile com `dcclinux64` via um harness de console (gere um response file a partir do search path do projeto para contornar o limite de 32 KB de linha de comando do Win32).

### ⚙️ Instalação

Para instalar usando o gerenciador de pacotes [**Boss**](https://github.com/HashLoad/boss):

```sh
boss install DBEngine4D
```

> [!NOTE]
> Por motivos históricos de registro no Boss, o pacote é declarado como **DBEngine4D** no manifesto, embora o nome oficial do projeto seja **DataEngine**.

---

### ⚡️ Início Rápido

#### 1. Conexão Básica (Factory do FireDAC)
```delphi
uses
  DataEngine.Factory.FireDAC,
  DataEngine.Interfaces;

var
  FDBConnection: IDBConnection;
begin
  // Estabelece uma conexão agnóstica a banco usando o factory do FireDAC
  FDBConnection := TFactoryFireDAC.Create(MyFDConnection, dnSQLite);
  FDBConnection.Connect;
end;
```

#### 2. Execução Direta de Consulta
```delphi
var
  LQuery: IDBQuery;
begin
  LQuery := FDBConnection.CreateQuery;
  try
    LQuery.CommandText := 'UPDATE users SET active = 1 WHERE id = :id';
    LQuery.ParamByName('id').AsInteger := 123;
    LQuery.ExecuteDirect;
  finally
    // As interfaces gerenciam a destruição automática do recurso via ARC
  end;
end;
```

#### 3. Controle de Transações
```delphi
FDBConnection.StartTransaction;
try
  // Suas operações de banco de dados
  FDBConnection.Commit;
except
  FDBConnection.Rollback;
  raise;
end;
```

#### 4. Pool de Conexões Multi-tenant
```delphi
var
  LConn: IDBConnection;
begin
  LConn := PoolManager.AcquireConnection(
    'Tenant_A',
    function: IDBConnection
    begin
      Result := TFactoryFireDAC.Create(CreateNativeConn, dnPostgreSQL);
    end
  );
  try
    // Use a conexão adquirida do pool
  finally
    PoolManager.ReleaseConnection('Tenant_A', LConn);
  end;
end;
```

---
*Copyright © 2025-2026 Isaque Pinheiro. Licensed under MIT License.*
