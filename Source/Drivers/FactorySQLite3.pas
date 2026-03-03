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

unit FactorySQLite3;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SQLiteTable3,
  FactoryConnection,
  FactoryInterfaces,
  DriverConnection;

type
  TFactorySQLite3 = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverSQLite3,
  DriverSQLite3Transaction;

{ TFactorySQLite3 }

constructor TFactorySQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverSQLite3Transaction.Create(AConnection);
  FDriverConnection  := TDriverSQLite3.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              nil);
  FAutoTransaction := False;
end;

constructor TFactorySQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactorySQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverSQLite3Transaction.Create(AConnection);
  FDriverConnection  := TDriverSQLite3.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactorySQLite3.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactorySQLite3.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TSQLiteDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TSQLiteDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.
