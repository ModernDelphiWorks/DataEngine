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

unit FactoryIBExpress;

interface

uses
  DB,
  Classes,
  SysUtils,
  IBDatabase,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryIBExpress = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TIBDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TIBDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TIBDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverIBExpress,
  DriverIBExpressTransaction;

{ TFactoryIBExpress }

constructor TFactoryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverIBExpressTransaction.Create(AConnection);
  FDriverConnection  := TDriverIBExpress.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryIBExpress.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // IBExpress uses TIBTransaction
  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryIBExpress.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
