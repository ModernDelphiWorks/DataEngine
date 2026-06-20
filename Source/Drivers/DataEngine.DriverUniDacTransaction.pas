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

unit DataEngine.DriverUniDacTransaction;

{$IFDEF DATAENGINE_DRIVER_UNIDAC}

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  Uni,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverUniDACTransaction = class(TDriverTransaction)
  private
    FConnection: TUniConnection;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
  protected
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverUniDACTransaction }

constructor TDriverUniDACTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection, AMonitorCallback);
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

procedure TDriverUniDACTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
var
  LUniTransaction: TUniTransaction;
  LNativeLevel: TIsolationLevel;
begin
  LUniTransaction := FTransactionActive as TUniTransaction;
  if ALevel <> ilDefault then
  begin
    case ALevel of
      ilReadUncommitted: LNativeLevel := Uni.ilReadUncommitted;
      ilReadCommitted: LNativeLevel := Uni.ilReadCommitted;
      ilRepeatableRead: LNativeLevel := Uni.ilRepeatableRead;
      ilSerializable: LNativeLevel := Uni.ilSerializable;
      ilSnapshot: LNativeLevel := Uni.ilSnapshot;
    else
      LNativeLevel := Uni.ilReadCommitted;
      if Assigned(FMonitorCallback) then
        FMonitorCallback(TMonitorParam.Create('[WARNING] UniDAC: Isolation level not supported. Falling back to ReadCommitted.', nil));
    end;
    LUniTransaction.IsolationLevel := LNativeLevel;
  end;
  LUniTransaction.StartTransaction;
end;

procedure TDriverUniDACTransaction.Commit;
begin
  (FTransactionActive as TUniTransaction).Commit;
end;

procedure TDriverUniDACTransaction.Rollback;
begin
  (FTransactionActive as TUniTransaction).Rollback;
end;

function TDriverUniDACTransaction._InTransaction: Boolean;
begin
  Result := (FTransactionActive as TUniTransaction).Active;
end;

{$ELSE}

interface

implementation

{$ENDIF}

end.


