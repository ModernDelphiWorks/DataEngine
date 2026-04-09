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

unit DataEngine.DriverODACTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  Ora,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverODACTransaction = class(TDriverTransaction)
  protected
    FConnection: TOraSession;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverODACTransaction }

constructor TDriverODACTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
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

function TDriverODACTransaction._InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

procedure TDriverODACTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
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



