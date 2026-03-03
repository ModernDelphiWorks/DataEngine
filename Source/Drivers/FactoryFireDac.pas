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

unit FactoryFireDac;

interface

uses
  DB,
  Classes,
  SysUtils,
  FireDAC.Comp.Client,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryFireDAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TFDConnection;
      const ADriver: TDBEngineDriver); overload;
    constructor Create(const AConnection: TFDConnection; const ADriver: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TFDConnection; const ADriver: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverFireDac,
  DriverFireDacTransaction;

{ TFactoryFireDAC }

constructor TFactoryFireDAC.Create(const AConnection: TFDConnection;
  const ADriver: TDBEngineDriver);
begin
  FDriverTransaction := TDriverFireDACTransaction.Create(AConnection);
  FDriverConnection  := TDriverFireDAC.Create(AConnection,
                                              FDriverTransaction,
                                              ADriver,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryFireDAC.Create(const AConnection: TFDConnection;
  const ADriver: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriver);
end;

destructor TFactoryFireDAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

procedure TFactoryFireDAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TFDTransaction) then
    raise Exception.Create('Invalid transaction type. Expected TFDTransaction.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryFireDAC.Create(const AConnection: TFDConnection;
  const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriver);
end;

end.



