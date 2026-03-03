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

unit FactoryDBExpress;

interface

uses
  DB,
  Classes,
  SysUtils,
  SqlExpr,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryDBExpress = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TSQLConnection;
      const ADriver: TDBEngineDriver); overload;
    constructor Create(const AConnection: TSQLConnection; const ADriver: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TSQLConnection; const ADriver: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverDBExpress,
  DriverDBExpressTransaction;

{ TFactoryDBExpress }

constructor TFactoryDBExpress.Create(const AConnection: TSQLConnection;
  const ADriver: TDBEngineDriver);
begin
  FDriverTransaction := TDriverDBExpressTransaction.Create(AConnection);
  FDriverConnection  := TDriverDBExpress.Create(AConnection,
                                                FDriverTransaction,
                                                ADriver,
                                                FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryDBExpress.Create(const AConnection: TSQLConnection;
  const ADriver: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriver);
end;

destructor TFactoryDBExpress.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

procedure TFactoryDBExpress.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // DBExpress transactions are created on demand, so strict type checking might not be applicable
  // unless we enforce TDBXTransactionWrapper, but that's internal.
  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryDBExpress.Create(const AConnection: TSQLConnection;
  const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriver);
end;

end.
