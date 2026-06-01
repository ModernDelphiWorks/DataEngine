{
  ------------------------------------------------------------------------------
  DataEngine
  High-performance database engine abstraction framework for Delphi and Lazarus.

  SPDX-License-Identifier: MIT
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the MIT License.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.DriverSQLDirectTransaction;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  SDEngine,
  DataEngine.DriverConnection, DataEngine.FactoryInterfaces;

type
  TDriverSQLDirectTransaction = class(TDriverTransaction)
  protected
    FConnection: TSDDatabase;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverSQLDirectTransaction }

constructor TDriverSQLDirectTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection);
  if not (AConnection is TSDDatabase) then
    raise Exception.Create('Invalid connection type. Expected TSDDatabase.');
    
  FConnection := TSDDatabase(AConnection);
  // SQLDirect manages transactions on the database component itself
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverSQLDirectTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverSQLDirectTransaction._InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

procedure TDriverSQLDirectTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not FConnection.InTransaction then
    FConnection.StartTransaction;
end;

procedure TDriverSQLDirectTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.InTransaction then
    FConnection.Commit;
end;

procedure TDriverSQLDirectTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if FConnection.InTransaction then
    FConnection.Rollback;
end;

end.



