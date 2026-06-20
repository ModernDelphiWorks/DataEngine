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

unit DataEngine.DriverDBExpressTransaction;

{$IFDEF DATAENGINE_DRIVER_DBEXPRESS}

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  SqlExpr,
  DBXCommon,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDBXTransactionWrapper = class(TComponent)
  private
    FTransaction: TDBXTransaction;
  public
    constructor Create(AOwner: TComponent; ATransaction: TDBXTransaction); reintroduce;
    destructor Destroy; override;
    property Transaction: TDBXTransaction read FTransaction;
  end;

  TDriverDBExpressTransaction = class(TDriverTransaction)
  private
    FConnection: TSQLConnection;
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

{ TDBXTransactionWrapper }

constructor TDBXTransactionWrapper.Create(AOwner: TComponent;
  ATransaction: TDBXTransaction);
begin
  inherited Create(AOwner);
  FTransaction := ATransaction;
end;

destructor TDBXTransactionWrapper.Destroy;
begin
  if Assigned(FTransaction) then
    FTransaction.Free;
  inherited;
end;

{ TDriverDBExpressTransaction }

constructor TDriverDBExpressTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TSQLConnection;
end;

destructor TDriverDBExpressTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

procedure TDriverDBExpressTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
var
  LTrans: TDBXTransaction;
  LWrapper: TDBXTransactionWrapper;
begin
  if Assigned(FTransactionActive) then
    raise Exception.Create('Transaction already active');

  LTrans := FConnection.BeginTransaction;
  LWrapper := TDBXTransactionWrapper.Create(nil, LTrans);
  LWrapper.Name := 'DEFAULT'; 
  
  if FTransactionList.ContainsKey('DEFAULT') then
  begin
    FTransactionList['DEFAULT'].Free;
    FTransactionList.Remove('DEFAULT');
  end;

  FTransactionList.Add('DEFAULT', LWrapper);
  FTransactionActive := LWrapper;
end;

procedure TDriverDBExpressTransaction.Commit;
var
  LWrapper: TDBXTransactionWrapper;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('No active transaction to commit.');

  LWrapper := FTransactionActive as TDBXTransactionWrapper;
  try
    FConnection.CommitFreeAndNil(LWrapper.FTransaction);
  finally
    if FTransactionList.ContainsKey(LWrapper.Name) then
      FTransactionList.Remove(LWrapper.Name);
    
    FTransactionActive := nil;
    LWrapper.Free;
  end;
end;

procedure TDriverDBExpressTransaction.Rollback;
var
  LWrapper: TDBXTransactionWrapper;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('No active transaction to rollback.');

  LWrapper := FTransactionActive as TDBXTransactionWrapper;
  try
    FConnection.RollbackFreeAndNil(LWrapper.FTransaction);
  finally
    if FTransactionList.ContainsKey(LWrapper.Name) then
      FTransactionList.Remove(LWrapper.Name);
      
    FTransactionActive := nil;
    LWrapper.Free;
  end;
end;

function TDriverDBExpressTransaction._InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;


{$ELSE}

interface

implementation

{$ENDIF}

end.
