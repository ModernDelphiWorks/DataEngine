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

unit FactoryIBObjects;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  IB_Components,
  FactoryConnection,
  FactoryInterfaces,
  DriverConnection;

type
  TFactoryIBObjects = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TIBODatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TIBODatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TIBODatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverIBObjects,
  DriverIBObjectsTransaction;

{ TFactoryIBObjects }

constructor TFactoryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverIBObjectsTransaction.Create(AConnection);
  FDriverConnection  := TDriverIBObjects.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                nil);
  FAutoTransaction := False;
end;

constructor TFactoryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverIBObjectsTransaction.Create(AConnection);
  FDriverConnection  := TDriverIBObjects.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryIBObjects.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryIBObjects.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TIBODatabase) then
    raise Exception.Create('Invalid transaction type. Expected TIBODatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.
