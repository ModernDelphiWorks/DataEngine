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

unit FactoryAbsoluteDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  ABSMain,
  FactoryConnection,
  FactoryInterfaces,
  DriverConnection;

type
  TFactoryAbsoluteDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TABSDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TABSDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TABSDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverAbsoluteDB,
  DriverAbsoluteDBTransaction;

{ TFactoryAbsoluteDB }

constructor TFactoryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverAbsoluteDBTransaction.Create(AConnection);
  FDriverConnection  := TDriverAbsoluteDB.Create(AConnection,
                                                 FDriverTransaction,
                                                 ADriverName,
                                                 nil);
  FAutoTransaction := False;
end;

constructor TFactoryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverAbsoluteDBTransaction.Create(AConnection);
  FDriverConnection  := TDriverAbsoluteDB.Create(AConnection,
                                                 FDriverTransaction,
                                                 ADriverName,
                                                 AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryAbsoluteDB.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryAbsoluteDB.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TABSDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TABSDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.
