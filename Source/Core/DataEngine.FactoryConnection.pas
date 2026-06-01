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

unit DataEngine.FactoryConnection;

interface

uses
  DB,
  Classes,
  SysUtils,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TFactoryConnection = class abstract(TInterfacedObject, IDBConnection, IDBTransaction)
  protected
    FOptions: IOptions;
    FAutoTransaction: Boolean;
    FDriverConnection: TDriverConnection;
    FDriverTransaction: TDriverTransaction;
    FCommandMonitor: ICommandMonitor;
    FMonitorCallback: TMonitorProc;
    function _GetTransaction(const AKey: String): TComponent; virtual;
  public
    procedure Connect; virtual;
    procedure Disconnect; virtual;
    procedure ExecuteDirect(const ASQL: String); overload; virtual;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); overload; virtual;
    procedure ExecuteScript(const AScript: String); virtual;
    procedure AddScript(const AScript: String); virtual;
    procedure ExecuteScripts; virtual;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); virtual;
    procedure SetCommandMonitor(AMonitor: ICommandMonitor); virtual;
      deprecated 'use Create(AConnection, ADriverName, AMonitor)';
    function IsConnected: Boolean; virtual;
    function CreateQuery: IDBQuery; virtual;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; virtual;
    function BulkLoader: IDBBulkLoader; virtual;
    function GetSQLScripts: String; virtual;
    function RowsAffected: UInt32; virtual;
    function GetDriver: TDBEngineDriver; virtual;
    function CommandMonitor: ICommandMonitor; virtual;
    function MonitorCallback: TMonitorProc; virtual;
    function Options: IOptions; virtual;
    // Transactions
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); virtual;
    procedure Commit; virtual;
    procedure Rollback; virtual;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); virtual;
    procedure UseTransaction(const AKey: String); virtual;
    function TransactionActive: TComponent; virtual;
    function InTransaction: Boolean; virtual;
    function Cache: IDBCacheProvider; virtual;
    function MetadataCache: IDBMetadataCache; virtual;
    procedure SetCacheProvider(ACache: IDBCacheProvider); virtual;
    procedure SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache); virtual;
    procedure RefreshMetadata(const ATableName: string); virtual;
    function IsAlive: Boolean; virtual;
    function ResiliencePolicy: IDBResiliencePolicy; virtual;
    procedure SetResiliencePolicy(APolicy: IDBResiliencePolicy); virtual;
    procedure AddObserver(const AObserver: IDBObserver); virtual;
    procedure RemoveObserver(const AObserver: IDBObserver); virtual;
    function SlowQueryThreshold: Integer; virtual;
    procedure SetSlowQueryThreshold(const AValue: Integer); virtual;
  end;

  TDriverTransactionHacker = class(TDriverTransaction)
  end;

implementation

{ TFactoryConnection }

procedure TFactoryConnection.AddScript(const AScript: String);
begin
  FDriverConnection.AddScript(AScript);
end;

procedure TFactoryConnection.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  FDriverTransaction.AddTransaction(AKey, ATransaction);
end;

procedure TFactoryConnection.ApplyUpdates(const ADataSets: array of IDBDataSet);
begin
  FDriverConnection.ApplyUpdates(ADataSets);
end;

function TFactoryConnection.CommandMonitor: ICommandMonitor;
begin
  Result := FCommandMonitor;
end;

procedure TFactoryConnection.Commit;
begin
  FDriverTransaction.Commit;
  if FAutoTransaction then
    Disconnect;
end;

procedure TFactoryConnection.Connect;
begin
  if not IsConnected then
    FDriverConnection.Connect;
end;

function TFactoryConnection.CreateQuery: IDBQuery;
begin
  Result := FDriverConnection.CreateQuery;
  if Assigned(Result) then
    Result.SlowQueryThreshold := FDriverConnection.SlowQueryThreshold;
end;

function TFactoryConnection.CreateDataSet(const ASQL: String): IDBDataSet;
begin
  Result := FDriverConnection.CreateDataSet(ASQL);
end;

function TFactoryConnection.BulkLoader: IDBBulkLoader;
begin
  Result := FDriverConnection.BulkLoader;
end;

procedure TFactoryConnection.Disconnect;
begin
  if IsConnected then
    FDriverConnection.Disconnect;
end;

function TFactoryConnection.Options: IOptions;
begin
  if not Assigned(FOptions) then
    FOptions := TOptions.Create;
  Result := FOptions;
end;

procedure TFactoryConnection.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LInTransaction: Boolean;
  LIsConnected: Boolean;
begin
  LInTransaction := InTransaction;
  LIsConnected := IsConnected;
  if not LIsConnected then
    Connect;
  try
    try
      if not LInTransaction then
        StartTransaction;
      FDriverConnection.ExecuteDirect(ASQL, AParams);
      if not LInTransaction then
        Commit;
    except
      on E: Exception do
      begin
        if not LInTransaction then
          Rollback;
        raise Exception.Create(E.Message);
      end;
    end;
  finally
    if not LIsConnected then
      Disconnect;
  end;
end;

procedure TFactoryConnection.ExecuteDirect(const ASQL: String);
var
  LInTransaction: Boolean;
  LIsConnected: Boolean;
begin
  LInTransaction := InTransaction;
  LIsConnected := IsConnected;
  if not LIsConnected then
    Connect;
  try
    if not LInTransaction then
      StartTransaction;
    try
      FDriverConnection.ExecuteDirect(ASQL);
      if not LInTransaction then
        Commit;
    except
      on E: Exception do
      begin
        if not LInTransaction then
          Rollback;
        raise Exception.Create(E.Message);
      end;
    end;
  finally
    if not LIsConnected then
      Disconnect;
  end;
end;

procedure TFactoryConnection.ExecuteScript(const AScript: String);
var
  LInTransaction: Boolean;
  LIsConnected: Boolean;
begin
  LInTransaction := InTransaction;
  LIsConnected := IsConnected;
  if not LIsConnected then
    Connect;
  try
    if not LInTransaction then
      StartTransaction;
    try
      FDriverConnection.ExecuteScript(AScript);
      if not LInTransaction then
        Commit;
    except
      on E: Exception do
      begin
        if not LInTransaction then
          Rollback;
        raise Exception.Create(E.Message);
      end;
    end;
  finally
    if not LIsConnected then
      Disconnect;
  end;
end;

procedure TFactoryConnection.ExecuteScripts;
var
  LInTransaction: Boolean;
  LIsConnected: Boolean;
begin
  LInTransaction := InTransaction;
  LIsConnected := IsConnected;
  if not LIsConnected then
    Connect;
  try
    if not LInTransaction then
      StartTransaction;
    try
      FDriverConnection.ExecuteScripts;
      if not LInTransaction then
        Commit;
    except
      on E: Exception do
      begin
        if not LInTransaction then
          Rollback;
        raise Exception.Create(E.Message);
      end;
    end;
  finally
    if not LIsConnected then
      Disconnect;
  end;
end;

function TFactoryConnection.GetDriver: TDBEngineDriver;
begin
  Result := FDriverConnection.GetDriver;
end;

function TFactoryConnection.GetSQLScripts: String;
begin
  Result := FDriverConnection.GetSQLScripts;
end;

function TFactoryConnection.InTransaction: Boolean;
begin
  Result := False;
  if not IsConnected then
    Exit;
  Result := FDriverTransaction.InTransaction;
end;

function TFactoryConnection.IsConnected: Boolean;
begin
  Result := FDriverConnection.IsConnected;
end;

function TFactoryConnection.MonitorCallback: TMonitorProc;
begin
  Result := FMonitorCallback;
end;

procedure TFactoryConnection.Rollback;
begin
  FDriverTransaction.Rollback;
  if FAutoTransaction then
    Disconnect;
end;

function TFactoryConnection.RowsAffected: UInt32;
begin
  Result := FDriverConnection.RowsAffected;
end;

procedure TFactoryConnection.SetCommandMonitor(AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
end;

procedure TFactoryConnection.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  if FDriverTransaction.TransactionActive = nil then
  begin
    try
      UseTransaction('DEFAULT');
    except
      // Silence error if 'DEFAULT' is not yet registered.
      // Some drivers (like DBExpress) initialize 'DEFAULT' lazily inside StartTransaction.
    end;
  end;
  if not IsConnected then
  begin
    Connect;
    FAutoTransaction := True;
  end;
  FDriverTransaction.StartTransaction(ALevel);
end;

function TFactoryConnection.TransactionActive: TComponent;
begin
  Result := FDriverTransaction.TransactionActive;
end;

procedure TFactoryConnection.UseTransaction(const AKey: String);
begin
  FDriverTransaction.UseTransaction(AKey);
end;

function TFactoryConnection._GetTransaction(const AKey: String): TComponent;
begin
  Result := TDriverTransactionHacker(FDriverTransaction)._GetTransaction(AKey);
end;

function TFactoryConnection.Cache: IDBCacheProvider;
begin
  Result := FDriverConnection.Cache;
end;

procedure TFactoryConnection.SetCacheProvider(ACache: IDBCacheProvider);
begin
  FDriverConnection.SetCacheProvider(ACache);
end;

function TFactoryConnection.MetadataCache: IDBMetadataCache;
begin
  Result := FDriverConnection.MetadataCache;
end;

procedure TFactoryConnection.SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache);
begin
  FDriverConnection.SetMetadataCacheProvider(AMetadataCache);
end;

procedure TFactoryConnection.RefreshMetadata(const ATableName: string);
begin
  FDriverConnection.RefreshMetadata(ATableName);
end;

function TFactoryConnection.IsAlive: Boolean;
begin
  Result := FDriverConnection.IsAlive;
end;

function TFactoryConnection.ResiliencePolicy: IDBResiliencePolicy;
begin
  Result := FDriverConnection.ResiliencePolicy;
end;

procedure TFactoryConnection.SetResiliencePolicy(APolicy: IDBResiliencePolicy);
begin
  FDriverConnection.SetResiliencePolicy(APolicy);
end;

procedure TFactoryConnection.AddObserver(const AObserver: IDBObserver);
begin
  FDriverConnection.AddObserver(AObserver);
end;

procedure TFactoryConnection.RemoveObserver(const AObserver: IDBObserver);
begin
  FDriverConnection.RemoveObserver(AObserver);
end;

function TFactoryConnection.SlowQueryThreshold: Integer;
begin
  Result := FDriverConnection.SlowQueryThreshold;
end;

procedure TFactoryConnection.SetSlowQueryThreshold(const AValue: Integer);
begin
  FDriverConnection.SetSlowQueryThreshold(AValue);
end;

end.



