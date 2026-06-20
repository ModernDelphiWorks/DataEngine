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

unit DataEngine.DriverFIBPlusTransaction;

{$IFDEF DATAENGINE_DRIVER_FIBPLUS}

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  FIBDatabase,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverFIBPlusTransaction = class(TDriverTransaction)
  protected
    FConnection: TFIBDatabase;
    FInternalTransaction: TFIBTransaction;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverFIBPlusTransaction }

constructor TDriverFIBPlusTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
var
  LTransaction: TFIBTransaction;
begin
  inherited;
  FConnection := AConnection as TFIBDatabase;
  FInternalTransaction := nil;

  // Use existing DefaultTransaction if available, otherwise create one
  if FConnection.DefaultTransaction <> nil then
  begin
    LTransaction := FConnection.DefaultTransaction;
  end
  else
  begin
    FInternalTransaction := TFIBTransaction.Create(nil);
    FInternalTransaction.DefaultDatabase := FConnection;
    FInternalTransaction.TimeoutAction := TACommit; 
    FConnection.DefaultTransaction := FInternalTransaction;
    LTransaction := FInternalTransaction;
  end;

  if LTransaction.Name = EmptyStr then
    LTransaction.Name := 'DEFAULT';

  FTransactionList.Add(UpperCase(LTransaction.Name), LTransaction);
  FTransactionActive := LTransaction;
end;

destructor TDriverFIBPlusTransaction.Destroy;
begin
  if Assigned(FInternalTransaction) then
  begin
    if FConnection.DefaultTransaction = FInternalTransaction then
      FConnection.DefaultTransaction := nil;
    FInternalTransaction.Free;
  end;
  FInternalTransaction := nil;
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverFIBPlusTransaction._InTransaction: Boolean;
begin
  Result := (FTransactionActive as TFIBTransaction).InTransaction;
end;

procedure TDriverFIBPlusTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  FConnection.Connected := True;
  if not (FTransactionActive as TFIBTransaction).InTransaction then
    (FTransactionActive as TFIBTransaction).StartTransaction;
end;

procedure TDriverFIBPlusTransaction.Commit;
begin
  (FTransactionActive as TFIBTransaction).Commit;
end;

procedure TDriverFIBPlusTransaction.Rollback;
begin
  (FTransactionActive as TFIBTransaction).Rollback;
end;


{$ELSE}

interface

implementation

{$ENDIF}

end.
