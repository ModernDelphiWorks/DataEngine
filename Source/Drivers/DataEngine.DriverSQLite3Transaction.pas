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

unit DataEngine.DriverSQLite3Transaction;

{$IFDEF DATAENGINE_DRIVER_SQLITE3}

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SQLiteTable3,
  DataEngine.DriverConnection, DataEngine.FactoryInterfaces;

type
  TDriverSQLite3Transaction = class(TDriverTransaction)
  protected
    FConnection: TSQLiteDatabase;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverSQLite3Transaction }

constructor TDriverSQLite3Transaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection);
  if not (AConnection is TSQLiteDatabase) then
    raise Exception.Create('Invalid connection type. Expected TSQLiteDatabase.');
    
  FConnection := TSQLiteDatabase(AConnection);
  // SQLite3 (via SQLiteTable3) manages transactions on the database connection object
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverSQLite3Transaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverSQLite3Transaction._InTransaction: Boolean;
begin
  Result := FConnection.IsTransactionOpen;
end;

procedure TDriverSQLite3Transaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not FConnection.IsTransactionOpen then
    FConnection.BeginTransaction;
end;

procedure TDriverSQLite3Transaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.IsTransactionOpen then
    FConnection.Commit;
end;

procedure TDriverSQLite3Transaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.IsTransactionOpen then
    FConnection.Rollback;
end;

{$ELSE}

interface

implementation

{$ENDIF}

end.



