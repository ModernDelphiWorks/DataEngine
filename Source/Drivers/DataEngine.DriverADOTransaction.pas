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

unit DataEngine.DriverADOTransaction;

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  ADODB,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverADOTransaction = class(TDriverTransaction)
  protected
    FConnection: TADOConnection;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverADOTransaction }

constructor TDriverADOTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
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

function TDriverADOTransaction._InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

procedure TDriverADOTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
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



