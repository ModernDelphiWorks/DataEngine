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

unit DriverSQLite3Transaction;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SQLiteTable3,
  DriverConnection;

type
  TDriverSQLite3Transaction = class(TDriverTransaction)
  protected
    FConnection: TSQLiteDatabase;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverSQLite3Transaction }

constructor TDriverSQLite3Transaction.Create(const AConnection: TComponent);
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

function TDriverSQLite3Transaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.IsTransactionOpen;
end;

procedure TDriverSQLite3Transaction.StartTransaction;
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

end.
