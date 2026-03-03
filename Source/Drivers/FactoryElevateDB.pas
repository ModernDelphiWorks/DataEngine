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

unit FactoryElevateDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  edbcomps,
  FactoryConnection,
  FactoryInterfaces,
  DriverConnection;

type
  TFactoryElevateDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverElevateDB,
  DriverElevateDBTransaction;

{ TFactoryElevateDB }

constructor TFactoryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverElevateDBTransaction.Create(AConnection);
  FDriverConnection  := TDriverElevateDB.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                nil);
  FAutoTransaction := False;
end;

constructor TFactoryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverElevateDBTransaction.Create(AConnection);
  FDriverConnection  := TDriverElevateDB.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryElevateDB.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryElevateDB.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TEDBDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TEDBDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.
