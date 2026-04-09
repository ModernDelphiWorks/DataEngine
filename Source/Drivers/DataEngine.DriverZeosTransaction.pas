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

unit DataEngine.DriverZeosTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  ZAbstractConnection,
  ZConnection,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverZeosTransaction = class(TDriverTransaction)
  private
    FConnection: TZConnection;
    {$IFDEF ZEOS80UP}
    FTransaction: TZTransaction;
    {$ENDIF}
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

function LevelToNativeZeos(const ALevel: TDBIsolationLevel; const AMonitorCallback: TMonitorProc): TZIsolationLevel;
begin
  case ALevel of
    ilReadUncommitted: Result := tiReadUncommitted;
    ilReadCommitted: Result := tiReadCommitted;
    ilRepeatableRead: Result := tiRepeatableRead;
    ilSerializable: Result := tiSerializable;
    ilSnapshot:
      begin
        Result := tiSerializable;
        if Assigned(AMonitorCallback) then
          AMonitorCallback(TMonitorParam.Create('[WARNING] Zeos: Snapshot isolation level not supported. Falling back to Serializable.', nil));
      end;
  else
    Result := tiReadCommitted;
    if Assigned(AMonitorCallback) then
      AMonitorCallback(TMonitorParam.Create('[WARNING] Zeos: Isolation level not supported. Falling back to ReadCommitted.', nil));
  end;
end;

{ TDriverZeosTransaction }

constructor TDriverZeosTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection, AMonitorCallback);
  FConnection := AConnection as TZConnection;
  {$IFDEF ZEOS80UP}
  if FConnection.Transaction = nil then
  begin
    FTransaction := TZTransaction.Create(nil);
    FTransaction.Connection := FConnection;
    FConnection.Transaction := FTransaction;
  end;
  FConnection.Transaction.Name := 'DEFAULT';
  FTransactionList.Add('DEFAULT', FConnection.Transaction);
  FTransactionActive := FConnection.Transaction;
  {$ENDIF}
end;

destructor TDriverZeosTransaction.Destroy;
begin
  {$IFDEF ZEOS80UP}
  if Assigned(FTransaction) then
  begin
    FConnection.Transaction := nil;
    FTransaction.Connection := nil;
    FTransaction.Free;
  end;
  FTransactionActive := nil;
  {$ENDIF}
  FConnection := nil;
  inherited;
end;

procedure TDriverZeosTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  if ALevel <> ilDefault then
    FConnection.TransactIsolationLevel := LevelToNativeZeos(ALevel, FMonitorCallback);

  {$IFDEF ZEOS80UP}
  (FTransactionActive as TZTransaction).StartTransaction;
  {$ELSE}
  FConnection.StartTransaction;
  {$ENDIF}
end;

procedure TDriverZeosTransaction.Commit;
begin
  {$IFDEF ZEOS80UP}
  (FTransactionActive as TZTransaction).Commit;
  {$ELSE}
  FConnection.Commit;
  {$ENDIF}
end;

procedure TDriverZeosTransaction.Rollback;
begin
  {$IFDEF ZEOS80UP}
  (FTransactionActive as TZTransaction).Rollback;
  {$ELSE}
  FConnection.Rollback;
  {$ENDIF}
end;

function TDriverZeosTransaction.InTransaction: Boolean;
begin
  {$IFDEF ZEOS80UP}
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
  Result := (FTransactionActive as TZTransaction).InTransaction;
  {$ELSE}
  Result := FConnection.InTransaction;
  {$ENDIF}
end;

end.


