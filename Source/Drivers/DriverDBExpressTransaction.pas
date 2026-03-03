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

unit DriverDBExpressTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  SqlExpr,
  DBXCommon,
  DriverConnection,
  FactoryInterfaces;

type
  TDBXTransactionWrapper = class(TComponent)
  private
    FTransaction: TDBXTransaction;
  public
    constructor Create(AOwner: TComponent; ATransaction: TDBXTransaction); reintroduce;
    destructor Destroy; override;
    property Transaction: TDBXTransaction read FTransaction;
  end;

  TDriverDBExpressTransaction = class(TDriverTransaction)
  private
    FConnection: TSQLConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDBXTransactionWrapper }

constructor TDBXTransactionWrapper.Create(AOwner: TComponent;
  ATransaction: TDBXTransaction);
begin
  inherited Create(AOwner);
  FTransaction := ATransaction;
end;

destructor TDBXTransactionWrapper.Destroy;
begin
  if Assigned(FTransaction) then
    FTransaction.Free;
  inherited;
end;

{ TDriverDBExpressTransaction }

constructor TDriverDBExpressTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TSQLConnection;
end;

destructor TDriverDBExpressTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

procedure TDriverDBExpressTransaction.StartTransaction;
var
  LTrans: TDBXTransaction;
  LWrapper: TDBXTransactionWrapper;
begin
  if Assigned(FTransactionActive) then
    raise Exception.Create('Transaction already active');

  LTrans := FConnection.BeginTransaction;
  LWrapper := TDBXTransactionWrapper.Create(nil, LTrans);
  LWrapper.Name := 'DEFAULT'; 
  
  if FTransactionList.ContainsKey('DEFAULT') then
  begin
    FTransactionList['DEFAULT'].Free;
    FTransactionList.Remove('DEFAULT');
  end;

  FTransactionList.Add('DEFAULT', LWrapper);
  FTransactionActive := LWrapper;
end;

procedure TDriverDBExpressTransaction.Commit;
var
  LWrapper: TDBXTransactionWrapper;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('No active transaction to commit.');

  LWrapper := FTransactionActive as TDBXTransactionWrapper;
  try
    FConnection.CommitFreeAndNil(LWrapper.FTransaction);
  finally
    if FTransactionList.ContainsKey(LWrapper.Name) then
      FTransactionList.Remove(LWrapper.Name);
    
    FTransactionActive := nil;
    LWrapper.Free;
  end;
end;

procedure TDriverDBExpressTransaction.Rollback;
var
  LWrapper: TDBXTransactionWrapper;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('No active transaction to rollback.');

  LWrapper := FTransactionActive as TDBXTransactionWrapper;
  try
    FConnection.RollbackFreeAndNil(LWrapper.FTransaction);
  finally
    if FTransactionList.ContainsKey(LWrapper.Name) then
      FTransactionList.Remove(LWrapper.Name);
      
    FTransactionActive := nil;
    LWrapper.Free;
  end;
end;

function TDriverDBExpressTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
     
  Result := FConnection.InTransaction;
end;

end.
