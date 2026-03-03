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

unit DriverIBExpressTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  IBDatabase,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverIBExpressTransaction = class(TDriverTransaction)
  protected
    FConnection: TIBDatabase;
    FInternalTransaction: TIBTransaction; // Only used if we create it
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverIBExpressTransaction }

constructor TDriverIBExpressTransaction.Create(const AConnection: TComponent);
var
  LTransaction: TIBTransaction;
begin
  inherited;
  FConnection := AConnection as TIBDatabase;
  FInternalTransaction := nil;
  
  if FConnection.DefaultTransaction <> nil then
  begin
     LTransaction := FConnection.DefaultTransaction;
  end
  else
  begin
    FInternalTransaction := TIBTransaction.Create(nil);
    FInternalTransaction.DefaultDatabase := FConnection;
    FInternalTransaction.AutoStopAction := saCommit;
    FConnection.DefaultTransaction := FInternalTransaction;
    LTransaction := FInternalTransaction;
  end;
  
  if LTransaction.Name = EmptyStr then
    LTransaction.Name := 'DEFAULT';

  FTransactionList.Add(UpperCase(LTransaction.Name), LTransaction);
  FTransactionActive := LTransaction;
end;

destructor TDriverIBExpressTransaction.Destroy;
begin
  if Assigned(FInternalTransaction) then
  begin
    if FConnection.DefaultTransaction = FInternalTransaction then
      FConnection.DefaultTransaction := nil;
    FInternalTransaction.Free;
  end;
  FInternalTransaction := nil;
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

procedure TDriverIBExpressTransaction.StartTransaction;
begin
  FConnection.Connected := True;
  (FTransactionActive as TIBTransaction).StartTransaction;
end;

procedure TDriverIBExpressTransaction.Commit;
begin
  (FTransactionActive as TIBTransaction).Commit;
end;

procedure TDriverIBExpressTransaction.Rollback;
begin
  (FTransactionActive as TIBTransaction).Rollback;
end;

function TDriverIBExpressTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := (FTransactionActive as TIBTransaction).InTransaction;
end;

end.
