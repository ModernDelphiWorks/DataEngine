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

unit DriverADOTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  ADODB,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverADOTransaction = class(TDriverTransaction)
  protected
    FConnection: TADOConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverADOTransaction }

constructor TDriverADOTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TADOConnection;
  
  // ADO manages transactions on the connection itself. 
  // We treat the Connection as the "Transaction Object" for consistency in storage.
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverADOTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverADOTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.InTransaction;
end;

procedure TDriverADOTransaction.StartTransaction;
begin
  FConnection.BeginTrans;
end;

procedure TDriverADOTransaction.Commit;
begin
  FConnection.CommitTrans;
end;

procedure TDriverADOTransaction.Rollback;
begin
  FConnection.RollbackTrans;
end;

end.
