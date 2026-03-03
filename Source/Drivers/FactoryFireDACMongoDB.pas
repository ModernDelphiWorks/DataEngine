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

unit FactoryFireDACMongoDB;

interface

uses
  DB,
  Classes,
  SysUtils,
  FireDAC.Comp.Client,
  FactoryConnection,
  FactoryInterfaces;

type
  TFactoryMongoFireDAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TFDConnection;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TFDConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TFDConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverFireDacMongoDB,
  DriverFireDacMongoDBTransaction;

{ TFactoryMongoFireDAC }

constructor TFactoryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverMongoFireDACTransaction.Create(AConnection);
  FDriverConnection  := TDriverMongoFireDAC.Create(AConnection,
                                                   FDriverTransaction,
                                                   ADriverName,
                                                   FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryMongoFireDAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TFDConnection) then
    raise Exception.Create('Invalid transaction type. Expected TFDConnection.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryMongoFireDAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
