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

unit DataEngine.DriverSQLDBTransaction;

{$IFDEF DATAENGINE_DRIVER_SQLDB}

interface

uses
  System.Classes,
  System.SysUtils,
  System.Generics.Collections,
  Data.DB,
  SQLDB,
  DataEngine.DriverConnection, DataEngine.FactoryInterfaces;

type
  TDriverSQLdbTransaction = class(TDriverTransaction)
  private
    FConnection: TSQLConnection;
    FTransaction: TSQLTransaction;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverSQLdbTransaction }

constructor TDriverSQLdbTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection);
  if not (AConnection is TSQLConnection) then
    raise Exception.Create('Invalid connection type. Expected TSQLConnection.');

  FConnection := TSQLConnection(AConnection);
  
  if FConnection.Transaction = nil then
  begin
    FTransaction := TSQLTransaction.Create(nil);
    FTransaction.Database := FConnection;
    FConnection.Transaction := FTransaction;
  end;
  
  if FConnection.Transaction.Name = '' then
    FConnection.Transaction.Name := 'DEFAULT';
    
  FTransactionList.Add('DEFAULT', FConnection.Transaction);
  FTransactionActive := FConnection.Transaction;
end;

destructor TDriverSQLdbTransaction.Destroy;
begin
  if Assigned(FTransaction) then
  begin
    FConnection.Transaction := nil;
    FTransaction.Database := nil;
    FreeAndNil(FTransaction);
  end;
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverSQLdbTransaction._InTransaction: Boolean;
begin
  Result := (FTransactionActive as TSQLTransaction).Active;
end;

procedure TDriverSQLdbTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if not (FTransactionActive as TSQLTransaction).Active then
    (FTransactionActive as TSQLTransaction).StartTransaction;
end;

procedure TDriverSQLdbTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if (FTransactionActive as TSQLTransaction).Active then
    (FTransactionActive as TSQLTransaction).Commit;
end;

procedure TDriverSQLdbTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  if (FTransactionActive as TSQLTransaction).Active then
    (FTransactionActive as TSQLTransaction).Rollback;
end;


{$ELSE}

interface

implementation

{$ENDIF}

end.
