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

unit FactoryADO;

interface

uses
  DB,
  Classes,
  SysUtils,
  ADODB,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryADO = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TADOConnection;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TADOConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TADOConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverADO,
  DriverADOTransaction;

{ TFactoryADO }

constructor TFactoryADO.Create(const AConnection: TADOConnection;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverADOTransaction.Create(AConnection);
  FDriverConnection  := TDriverADO.Create(AConnection,
                                          FDriverTransaction,
                                          ADriverName,
                                          FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryADO.Create(const AConnection: TADOConnection;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryADO.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // ADO Transaction is usually the Connection itself
  if not (ATransaction is TADOConnection) then
    raise Exception.Create('Invalid transaction type. Expected TADOConnection.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryADO.Create(const AConnection: TADOConnection;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryADO.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
