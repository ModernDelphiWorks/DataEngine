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

unit FactorySQLDirect;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SDEngine,
  FactoryConnection,
  FactoryInterfaces,
  DriverConnection;

type
  TFactorySQLDirect = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TSDDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TSDDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TSDDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverSQLDirect,
  DriverSQLDirectTransaction;

{ TFactorySQLDirect }

constructor TFactorySQLDirect.Create(const AConnection: TSDDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverSQLDirectTransaction.Create(AConnection);
  FDriverConnection  := TDriverSQLDirect.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                nil);
  FAutoTransaction := False;
end;

constructor TFactorySQLDirect.Create(const AConnection: TSDDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactorySQLDirect.Create(const AConnection: TSDDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverSQLDirectTransaction.Create(AConnection);
  FDriverConnection  := TDriverSQLDirect.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactorySQLDirect.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactorySQLDirect.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TSDDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TSDDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.
