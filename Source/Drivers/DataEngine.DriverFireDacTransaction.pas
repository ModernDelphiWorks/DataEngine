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

unit DataEngine.DriverFireDacTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  FireDAC.Comp.Client,
  FireDAC.Stan.Intf,
  FireDAC.Phys.Intf,
  FireDAC.Stan.Option,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverFireDACTransaction = class(TDriverTransaction)
  private
    FConnection: TFDConnection;
    FTransaction: TFDTransaction;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

function LevelToNativeFD(const ALevel: TDBIsolationLevel; const AMonitorCallback: TMonitorProc): TFDTxIsolation;
begin
  case ALevel of
    ilReadUncommitted: Result := xiDirtyRead;
    ilReadCommitted: Result := xiReadCommitted;
    ilRepeatableRead: Result := xiRepeatableRead;
    ilSerializable: Result := xiReadCommitted; 
    ilSnapshot: Result := xiReadCommitted;
  else
    Result := xiReadCommitted;
  end;
end;

{ TDriverFireDACTransaction }

constructor TDriverFireDACTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection, AMonitorCallback);
  FConnection := AConnection as TFDConnection;
  if FConnection.Transaction = nil then
  begin
    FTransaction := TFDTransaction.Create(nil);
    FTransaction.Connection := FConnection;
    FConnection.Transaction := FTransaction;
  end;
  FConnection.Transaction.Name := 'DEFAULT';
  FTransactionList.Add('DEFAULT', FConnection.Transaction);
  FTransactionActive := FConnection.Transaction;
end;

destructor TDriverFireDACTransaction.Destroy;
begin
  inherited;
  if Assigned(FTransaction) then
  begin
    FTransaction.Connection := nil;
    FTransaction.Free;
  end;
  FConnection := nil;
end;

procedure TDriverFireDACTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
var
  LFDTransaction: TFDTransaction;
begin
  LFDTransaction := FTransactionActive as TFDTransaction;
  if ALevel <> ilDefault then
    LFDTransaction.Options.Isolation := LevelToNativeFD(ALevel, FMonitorCallback);
  LFDTransaction.StartTransaction;
end;

procedure TDriverFireDACTransaction.Commit;
begin
  (FTransactionActive as TFDTransaction).Commit;
end;

procedure TDriverFireDACTransaction.Rollback;
begin
  (FTransactionActive as TFDTransaction).Rollback;
end;

function TDriverFireDACTransaction.InTransaction: Boolean;
begin
  Result := Assigned(FTransactionActive) and (FTransactionActive as TFDTransaction).Active;
end;

end.



