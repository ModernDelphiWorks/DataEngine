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

unit DriverFireDacTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  FireDAC.Comp.Client,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverFireDACTransaction = class(TDriverTransaction)
  private
    FConnection: TFDConnection;
    FTransaction: TFDTransaction;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverFireDACTransaction }

constructor TDriverFireDACTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TFDConnection;
  if FConnection.Transaction = nil then
  begin
    FTransaction := TFDTransaction.Create(nil);
    FTransaction.Connection := FConnection;
    FConnection.Transaction := FTransaction;
  end;
  FConnection.Transaction.Name := 'DEFAULT';
  FTransactionList.Add('DEFAULT', FConnection.Transaction);
  FTransactionActive := FConnection.Transaction;
end;

destructor TDriverFireDACTransaction.Destroy;
begin
  inherited;
  if Assigned(FTransaction) then
  begin
    FTransaction.Connection := nil;
    FTransaction.Free;
  end;
  FConnection := nil;
end;

procedure TDriverFireDACTransaction.StartTransaction;
begin
  (FTransactionActive as TFDTransaction).StartTransaction;
end;

procedure TDriverFireDACTransaction.Commit;
begin
  (FTransactionActive as TFDTransaction).Commit;
end;

procedure TDriverFireDACTransaction.Rollback;
begin
  (FTransactionActive as TFDTransaction).Rollback;
end;

function TDriverFireDACTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
  Result := (FTransactionActive as TFDTransaction).Active;
end;

end.

