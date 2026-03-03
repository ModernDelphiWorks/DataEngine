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

unit FactoryODAC;

interface

uses
  DB,
  Classes,
  SysUtils,
  Ora,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryODAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TOraSession;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TOraSession;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TOraSession;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverODAC,
  DriverODACTransaction;

{ TFactoryODAC }

constructor TFactoryODAC.Create(const AConnection: TOraSession;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverODACTransaction.Create(AConnection);
  FDriverConnection  := TDriverODAC.Create(AConnection,
                                           FDriverTransaction,
                                           ADriverName,
                                           FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryODAC.Create(const AConnection: TOraSession;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryODAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TOraSession) then
    raise Exception.Create('Invalid transaction type. Expected TOraSession.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryODAC.Create(const AConnection: TOraSession;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryODAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
