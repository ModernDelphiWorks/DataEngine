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

unit DataEngine.DriverMemoryTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  DataEngine.DriverConnection,
  DataEngine.DriverMemory,
  DataEngine.FactoryInterfaces;

type
  TDriverMemoryTransaction = class(TDriverTransaction)
  private
    FConnection: TMemoryConnection;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverMemoryTransaction }

constructor TDriverMemoryTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
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

procedure TDriverMemoryTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
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




