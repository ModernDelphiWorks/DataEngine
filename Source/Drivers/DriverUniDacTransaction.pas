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

unit DriverUniDacTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  Uni,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverUniDACTransaction = class(TDriverTransaction)
  private
    FConnection: TUniConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverUniDACTransaction }

constructor TDriverUniDACTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TUniConnection;
  
  // Ensure we have a default transaction if UniDAC didn't create one (unlikely but safe)
  if FConnection.DefaultTransaction = nil then
    raise Exception.Create('Connection DefaultTransaction is nil');

  FConnection.DefaultTransaction.Name := 'DEFAULT';
  FTransactionList.Add('DEFAULT', FConnection.DefaultTransaction);
  FTransactionActive := FConnection.DefaultTransaction;
end;

destructor TDriverUniDACTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

procedure TDriverUniDACTransaction.StartTransaction;
begin
  (FTransactionActive as TUniTransaction).StartTransaction;
end;

procedure TDriverUniDACTransaction.Commit;
begin
  (FTransactionActive as TUniTransaction).Commit;
end;

procedure TDriverUniDACTransaction.Rollback;
begin
  (FTransactionActive as TUniTransaction).Rollback;
end;

function TDriverUniDACTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
  Result := (FTransactionActive as TUniTransaction).Active;
end;

end.
