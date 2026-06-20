{
  ------------------------------------------------------------------------------
  DataEngine
  High-performance database engine abstraction framework for Delphi and Lazarus.

  SPDX-License-Identifier: MIT
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the MIT License.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.FactoryNexusDB;

interface

uses
  DB,
  Classes,
  SysUtils,
  nxdb,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryNexusDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TnxDatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TnxDatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TnxDatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverNexusDB,
  DataEngine.DriverNexusDBTransaction;

{ TFactoryNexusDB }

constructor TFactoryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverNexusDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverNexusDB.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

procedure TFactoryNexusDB.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // NexusDB typically manages transactions via the TnxDatabase component itself.
  // If a separate transaction component mechanism exists or is used, validation
  // should be added here similar to other drivers.
  inherited AddTransaction(AKey, ATransaction);
end;

destructor TFactoryNexusDB.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.





