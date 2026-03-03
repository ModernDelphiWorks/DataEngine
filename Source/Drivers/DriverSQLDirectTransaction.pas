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

unit DriverSQLDirectTransaction;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SDEngine,
  DriverConnection;

type
  TDriverSQLDirectTransaction = class(TDriverTransaction)
  protected
    FConnection: TSDDatabase;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverSQLDirectTransaction }

constructor TDriverSQLDirectTransaction.Create(const AConnection: TComponent);
begin
  inherited Create(AConnection);
  if not (AConnection is TSDDatabase) then
    raise Exception.Create('Invalid connection type. Expected TSDDatabase.');
    
  FConnection := TSDDatabase(AConnection);
  // SQLDirect manages transactions on the database component itself
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverSQLDirectTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverSQLDirectTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.InTransaction;
end;

procedure TDriverSQLDirectTransaction.StartTransaction;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not FConnection.InTransaction then
    FConnection.StartTransaction;
end;

procedure TDriverSQLDirectTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.InTransaction then
    FConnection.Commit;
end;

procedure TDriverSQLDirectTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.InTransaction then
    FConnection.Rollback;
end;

end.
