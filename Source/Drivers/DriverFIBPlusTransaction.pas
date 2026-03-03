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

unit DriverFIBPlusTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  FIBDatabase,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverFIBPlusTransaction = class(TDriverTransaction)
  protected
    FConnection: TFIBDatabase;
    FInternalTransaction: TFIBTransaction;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverFIBPlusTransaction }

constructor TDriverFIBPlusTransaction.Create(const AConnection: TComponent);
var
  LTransaction: TFIBTransaction;
begin
  inherited;
  FConnection := AConnection as TFIBDatabase;
  FInternalTransaction := nil;

  // Use existing DefaultTransaction if available, otherwise create one
  if FConnection.DefaultTransaction <> nil then
  begin
    LTransaction := FConnection.DefaultTransaction;
  end
  else
  begin
    FInternalTransaction := TFIBTransaction.Create(nil);
    FInternalTransaction.DefaultDatabase := FConnection;
    FInternalTransaction.TimeoutAction := TACommit; 
    FConnection.DefaultTransaction := FInternalTransaction;
    LTransaction := FInternalTransaction;
  end;

  if LTransaction.Name = EmptyStr then
    LTransaction.Name := 'DEFAULT';

  FTransactionList.Add(UpperCase(LTransaction.Name), LTransaction);
  FTransactionActive := LTransaction;
end;

destructor TDriverFIBPlusTransaction.Destroy;
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

function TDriverFIBPlusTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := (FTransactionActive as TFIBTransaction).InTransaction;
end;

procedure TDriverFIBPlusTransaction.StartTransaction;
begin
  FConnection.Connected := True;
  if not (FTransactionActive as TFIBTransaction).InTransaction then
    (FTransactionActive as TFIBTransaction).StartTransaction;
end;

procedure TDriverFIBPlusTransaction.Commit;
begin
  (FTransactionActive as TFIBTransaction).Commit;
end;

procedure TDriverFIBPlusTransaction.Rollback;
begin
  (FTransactionActive as TFIBTransaction).Rollback;
end;

end.
