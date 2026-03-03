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

unit DriverODACTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  Ora,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverODACTransaction = class(TDriverTransaction)
  protected
    FConnection: TOraSession;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverODACTransaction }

constructor TDriverODACTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TOraSession;
  // ODAC manages transactions on the session level.
  // We track the session as the transaction object.
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverODACTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverODACTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.InTransaction;
end;

procedure TDriverODACTransaction.StartTransaction;
begin
  FConnection.StartTransaction;
end;

procedure TDriverODACTransaction.Commit;
begin
  FConnection.Commit;
end;

procedure TDriverODACTransaction.Rollback;
begin
  FConnection.Rollback;
end;

end.
