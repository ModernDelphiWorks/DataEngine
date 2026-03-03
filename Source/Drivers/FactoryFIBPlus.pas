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

unit FactoryFIBPlus;

interface

uses
  DB,
  Classes,
  SysUtils,
  FIBDatabase,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryFIBPlus = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverFIBPlus,
  DriverFIBPlusTransaction;

{ TFactoryFIBPlus }

constructor TFactoryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverFIBPlusTransaction.Create(AConnection);
  FDriverConnection  := TDriverFIBPlus.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryFIBPlus.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TFIBDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TFIBDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryFIBPlus.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
