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

unit DriverNexusDBTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  nxdb,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverNexusDBTransaction = class(TDriverTransaction)
  protected
    FConnection: TnxDatabase;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverNexusDBTransaction }

constructor TDriverNexusDBTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TnxDatabase;
  // NexusDB manages transactions on the database component itself
  // We register the connection as the default transaction object
  FTransactionList.Add('DEFAULT', FConnection); 
  FTransactionActive := FConnection;
end;

destructor TDriverNexusDBTransaction.Destroy;
begin
  FConnection := nil;
  inherited;
end;

function TDriverNexusDBTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  Result := FConnection.InTransaction;
end;

procedure TDriverNexusDBTransaction.StartTransaction;
begin
  if not FConnection.InTransaction then
    FConnection.StartTransaction;
end;

procedure TDriverNexusDBTransaction.Commit;
begin
  FConnection.Commit;
end;

procedure TDriverNexusDBTransaction.Rollback;
begin
  FConnection.Rollback;
end;

end.
