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

unit DriverIBObjectsTransaction;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  IB_Components,
  DriverConnection;

type
  TDriverIBObjectsTransaction = class(TDriverTransaction)
  protected
    FConnection: TIBODatabase;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverIBObjectsTransaction }

constructor TDriverIBObjectsTransaction.Create(const AConnection: TComponent);
begin
  inherited Create(AConnection);
  if not (AConnection is TIBODatabase) then
    raise Exception.Create('Invalid connection type. Expected TIBODatabase.');
    
  FConnection := TIBODatabase(AConnection);
  // IBObjects usually manages transactions via DefaultTransaction or separate components.
  // Here we assume the TIBODatabase itself (or its default transaction) is the context.
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverIBObjectsTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverIBObjectsTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.DefaultTransaction.InTransaction;
end;

procedure TDriverIBObjectsTransaction.StartTransaction;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not FConnection.Connected then
    FConnection.Connected := True;

  if not FConnection.DefaultTransaction.InTransaction then
    FConnection.DefaultTransaction.StartTransaction;
end;

procedure TDriverIBObjectsTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.DefaultTransaction.InTransaction then
    FConnection.DefaultTransaction.Commit;
end;

procedure TDriverIBObjectsTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.DefaultTransaction.InTransaction then
    FConnection.DefaultTransaction.Rollback;
end;

end.
