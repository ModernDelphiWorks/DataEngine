{
  ------------------------------------------------------------------------------
  DataEngine
  High-performance database engine abstraction framework for Delphi and Lazarus.

  SPDX-License-Identifier: MIT
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the MIT License.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.FactorySQLDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SQLDB,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TFactorySQLdb = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TSQLConnection;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TSQLConnection;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TSQLConnection;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverSQLDB,
  DataEngine.DriverSQLDBTransaction;

{ TFactorySQLdb }

constructor TFactorySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverSQLdbTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverSQLdb.Create(AConnection,
                                            FDriverTransaction,
                                            ADriverName,
                                            nil);
  FAutoTransaction := False;
end;

constructor TFactorySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactorySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverSQLdbTransaction.Create(AConnection, FMonitorCallback);
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





