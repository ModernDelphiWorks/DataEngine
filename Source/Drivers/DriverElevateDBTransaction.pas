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

unit DriverElevateDBTransaction;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  edbcomps,
  DriverConnection;

type
  TDriverElevateDBTransaction = class(TDriverTransaction)
  protected
    FConnection: TEDBDatabase;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverElevateDBTransaction }

constructor TDriverElevateDBTransaction.Create(const AConnection: TComponent);
begin
  inherited Create(AConnection);
  if not (AConnection is TEDBDatabase) then
    raise Exception.Create('Invalid connection type. Expected TEDBDatabase.');
    
  FConnection := TEDBDatabase(AConnection);
  // ElevateDB manages transactions on the database component itself
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverElevateDBTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverElevateDBTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.InTransaction;
end;

procedure TDriverElevateDBTransaction.StartTransaction;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not FConnection.InTransaction then
    FConnection.StartTransaction;
end;

procedure TDriverElevateDBTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.InTransaction then
    FConnection.Commit;
end;

procedure TDriverElevateDBTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.InTransaction then
    FConnection.Rollback;
end;

end.
