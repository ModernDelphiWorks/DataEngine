{
  ------------------------------------------------------------------------------
  DataEngine
  Modular and extensible database engine framework for Delphi.

  SPDX-License-Identifier: Apache-2.0
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the Apache License, Version 2.0.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit FactoryNexusDB;

interface

uses
  DB,
  Classes,
  SysUtils,
  nxdb,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryNexusDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TnxDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TnxDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TnxDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverNexusDB,
  DriverNexusDBTransaction;

{ TFactoryNexusDB }

constructor TFactoryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverNexusDBTransaction.Create(AConnection);
  FDriverConnection  := TDriverNexusDB.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
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
