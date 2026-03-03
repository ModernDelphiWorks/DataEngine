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

unit DriverMemoryTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  DriverConnection,
  DriverMemory,
  FactoryInterfaces;

type
  TDriverMemoryTransaction = class(TDriverTransaction)
  private
    FConnection: TMemoryConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverMemoryTransaction }

constructor TDriverMemoryTransaction.Create(const AConnection: TComponent);
begin
  inherited Create(AConnection);
  if AConnection is TMemoryConnection then
    FConnection := TMemoryConnection(AConnection)
  else
    raise Exception.Create('Invalid connection type. Expected TMemoryConnection.');

  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverMemoryTransaction.Destroy;
begin
  FConnection := nil;
  inherited;
end;

procedure TDriverMemoryTransaction.StartTransaction;
begin
  FConnection.StartTransaction;
end;

procedure TDriverMemoryTransaction.Commit;
begin
  FConnection.Commit;
end;

procedure TDriverMemoryTransaction.Rollback;
begin
  FConnection.Rollback;
end;

function TDriverMemoryTransaction.InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

end.
