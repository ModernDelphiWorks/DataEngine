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

unit DriverSQLDBTransaction;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Generics.Collections,
  Data.DB,
  SQLDB,
  DriverConnection;

type
  TDriverSQLdbTransaction = class(TDriverTransaction)
  private
    FConnection: TSQLConnection;
    FTransaction: TSQLTransaction;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverSQLdbTransaction }

constructor TDriverSQLdbTransaction.Create(const AConnection: TComponent);
begin
  inherited Create(AConnection);
  if not (AConnection is TSQLConnection) then
    raise Exception.Create('Invalid connection type. Expected TSQLConnection.');

  FConnection := TSQLConnection(AConnection);
  
  if FConnection.Transaction = nil then
  begin
    FTransaction := TSQLTransaction.Create(nil);
    FTransaction.Database := FConnection;
    FConnection.Transaction := FTransaction;
  end;
  
  if FConnection.Transaction.Name = '' then
    FConnection.Transaction.Name := 'DEFAULT';
    
  FTransactionList.Add('DEFAULT', FConnection.Transaction);
  FTransactionActive := FConnection.Transaction;
end;

destructor TDriverSQLdbTransaction.Destroy;
begin
  if Assigned(FTransaction) then
  begin
    FConnection.Transaction := nil;
    FTransaction.Database := nil;
    FreeAndNil(FTransaction);
  end;
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverSQLdbTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := (FTransactionActive as TSQLTransaction).Active;
end;

procedure TDriverSQLdbTransaction.StartTransaction;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not (FTransactionActive as TSQLTransaction).Active then
    (FTransactionActive as TSQLTransaction).StartTransaction;
end;

procedure TDriverSQLdbTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if (FTransactionActive as TSQLTransaction).Active then
    (FTransactionActive as TSQLTransaction).Commit;
end;

procedure TDriverSQLdbTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if (FTransactionActive as TSQLTransaction).Active then
    (FTransactionActive as TSQLTransaction).Rollback;
end;

end.
