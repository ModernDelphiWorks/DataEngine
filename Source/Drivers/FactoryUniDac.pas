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

unit FactoryUniDac;

interface

uses
  DB,
  Classes,
  SysUtils,
  Uni,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryUniDAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TUniConnection;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TUniConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TUniConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverUniDac,
  DriverUniDacTransaction;

{ TFactoryUniDAC }

constructor TFactoryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverUniDACTransaction.Create(AConnection);
  FDriverConnection  := TDriverUniDAC.Create(AConnection,
                                             FDriverTransaction,
                                             ADriverName,
                                             FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryUniDAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // UniDAC uses TUniTransaction
  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryUniDAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
