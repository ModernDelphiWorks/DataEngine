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

unit FactorySQLDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SQLDB,
  FactoryConnection,
  FactoryInterfaces,
  DriverConnection;

type
  TFactorySQLdb = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TSQLConnection;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TSQLConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TSQLConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverSQLDB,
  DriverSQLDBTransaction;

{ TFactorySQLdb }

constructor TFactorySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverSQLdbTransaction.Create(AConnection);
  FDriverConnection  := TDriverSQLdb.Create(AConnection,
                                            FDriverTransaction,
                                            ADriverName,
                                            nil);
  FAutoTransaction := False;
end;

constructor TFactorySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactorySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverSQLdbTransaction.Create(AConnection);
  FDriverConnection  := TDriverSQLdb.Create(AConnection,
                                            FDriverTransaction,
                                            ADriverName,
                                            AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactorySQLdb.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactorySQLdb.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TSQLTransaction) then
    raise Exception.Create('Invalid transaction type. Expected TSQLTransaction.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.
