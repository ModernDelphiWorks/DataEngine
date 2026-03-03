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

unit DriverZeosTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  ZAbstractConnection,
  ZConnection,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverZeosTransaction = class(TDriverTransaction)
  private
    FConnection: TZConnection;
    {$IFDEF ZEOS80UP}
    FTransaction: TZTransaction;
    {$ENDIF}
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverZeosTransaction }

constructor TDriverZeosTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TZConnection;
  {$IFDEF ZEOS80UP}
  if FConnection.Transaction = nil then
  begin
    FTransaction := TZTransaction.Create(nil);
    FTransaction.Connection := FConnection;
    FConnection.Transaction := FTransaction;
  end;
  FConnection.Transaction.Name := 'DEFAULT';
  FTransactionList.Add('DEFAULT', FConnection.Transaction);
  FTransactionActive := FConnection.Transaction;
  {$ENDIF}
end;

destructor TDriverZeosTransaction.Destroy;
begin
  {$IFDEF ZEOS80UP}
  if Assigned(FTransaction) then
  begin
    FConnection.Transaction := nil;
    FTransaction.Connection := nil;
    FTransaction.Free;
  end;
  FTransactionActive := nil;
  {$ENDIF}
  FConnection := nil;
  inherited;
end;

procedure TDriverZeosTransaction.StartTransaction;
begin
  {$IFDEF ZEOS80UP}
  (FTransactionActive as TZTransaction).StartTransaction;
  {$ELSE}
  FConnection.StartTransaction;
  {$ENDIF}
end;

procedure TDriverZeosTransaction.Commit;
begin
  {$IFDEF ZEOS80UP}
  (FTransactionActive as TZTransaction).Commit;
  {$ELSE}
  FConnection.Commit;
  {$ENDIF}
end;

procedure TDriverZeosTransaction.Rollback;
begin
  {$IFDEF ZEOS80UP}
  (FTransactionActive as TZTransaction).Rollback;
  {$ELSE}
  FConnection.Rollback;
  {$ENDIF}
end;

function TDriverZeosTransaction.InTransaction: Boolean;
begin
  {$IFDEF ZEOS80UP}
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
  Result := (FTransactionActive as TZTransaction).InTransaction;
  {$ELSE}
  Result := FConnection.InTransaction;
  {$ENDIF}
end;

end.
