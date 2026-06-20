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

unit DataEngine.DriverUniDac;

{$IFDEF DATAENGINE_DRIVER_UNIDAC}

interface

uses
  Classes,
  SysUtils,
  StrUtils,
  Variants,
  DB,
  // UniDAC
  Uni,
  DBAccess,
  UniProvider,
  UniScript,
  // DBE
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.Metadata.Manager;

type
  // Classe de conex�o concreta com UniDAC
  TDriverUniDAC = class(TDriverConnection)
  private
    function _GetTransactionActive: TUniTransaction;
  protected
    FConnection: TUniConnection;
    FSQLScript : TUniScript;
    function _IsAlive: Boolean; override;
  public
    constructor Create(const AConnection: TComponent;
      const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc;
      const ACache: IDBCacheProvider = nil;
      const AMetadataCache: IDBMetadataCache = nil); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure _InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil); override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryUniDAC = class(TDriverQuery)
  private
    FSQLQuery: TUniSQL;
    function _GetTransactionActive: TUniTransaction;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TUniConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc;
      const ADriver: TDriverName;
      const ACache: IDBCacheProvider = nil;
      const AMetadataCache: IDBMetadataCache = nil);
    destructor Destroy; override;
    procedure _InternalExecuteDirect; override;
    function _InternalExecuteQuery: IDBDataSet; override;
    function _GetParams: TParams; override;
    procedure Prepare; override;
    procedure Unprepare; override;
    function ParamByName(const AValue: string): TParam; override;
  end;

  TDriverDataSetUniDAC = class(TDriverDataSet<TUniQuery>)
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
    procedure _SetFetchOptions(const Value: TFetchOptions); override;
  public
    constructor Create(const ADataSet: TUniQuery; const AMonitorCallback: TMonitorProc); reintroduce;
    procedure Open; override;
    procedure ApplyUpdates; override;
    procedure CancelUpdates; override;
    function RowsAffected: UInt32; override;
    function IsUniDirectional: Boolean; override;
    function IsReadOnly: Boolean; override;
    function IsCachedUpdates: Boolean; override;
  end;

implementation
 
uses
  DataEngine.CacheManager,
  DataEngine.DriverUniDacBulkLoader;

{ TDriverUniDAC }

constructor TDriverUniDAC.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction;
  const ADriverName: TDriverName;
  const AMonitorCallback: TMonitorProc;
  const ACache: IDBCacheProvider;
  const AMetadataCache: IDBMetadataCache);
begin
  inherited Create(AConnection, ADriverTransaction, ADriverName, AMonitorCallback, ACache, AMetadataCache);
  FConnection := AConnection as TUniConnection;
  FSQLScript  := TUniScript.Create(nil);
  try
    FSQLScript.Connection := FConnection;
    FSQLScript.SQL.Clear;
  except
    FreeAndNil(FSQLScript);
    raise;
  end;
end;

destructor TDriverUniDAC.Destroy;
begin
  FDriverTransaction := nil;
  FConnection := nil;
  FreeAndNil(FSQLScript);
  inherited;
end;

procedure TDriverUniDAC.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverUniDAC._InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil);
var
  LExeSQL: TUniSQL;
begin
  LExeSQL := TUniSQL.Create(nil);
  try
    LExeSQL.Connection := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := ASQL;

    if Assigned(AParams) then
      _UpdateNativeParams(LExeSQL.Params, AParams);

    LExeSQL.Prepare;
    LExeSQL.Execute;
    FRowsAffected := LExeSQL.RowsAffected;
  finally
    FreeAndNil(LExeSQL);
  end;
end;

procedure TDriverUniDAC.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverUniDAC.ExecuteScripts;
begin
  if FSQLScript.SQL.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  try
    FSQLScript.Transaction := _GetTransactionActive;
    try
      FSQLScript.Execute;
      FRowsAffected := FSQLScript.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog('Error during script execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(FSQLScript.SQL.Text, FSQLScript.Transaction.Name, nil);
    FSQLScript.SQL.Clear;
  end;
end;

function TDriverUniDAC.GetSQLScripts: String;
begin
  Result := 'Transaction: ' + FSQLScript.Transaction.Name + ' ' +  FSQLScript.SQL.Text;
end;

procedure TDriverUniDAC.AddScript(const AScript: String);
begin
  if Self.GetDriver in [dnInterbase, dnFirebird, dnFirebird3] then
    if FSQLScript.SQL.Count = 0 then
      FSQLScript.SQL.Add('SET AUTOCOMMIT OFF');
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverUniDAC.ApplyUpdates(const ADataSets: array of IDBDataSet);
var
  LDataSet: IDBDataSet;
begin
  for LDataset in AdataSets do
    LDataset.ApplyUpdates;
end;

procedure TDriverUniDAC.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverUniDAC.IsConnected: Boolean;
begin
  Result := FConnection.Connected = True;
end;

function TDriverUniDAC._GetTransactionActive: TUniTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TUniTransaction;
end;

function TDriverUniDAC._IsAlive: Boolean;
begin
  try
    Result := FConnection.Connected and (FConnection.Ping);
  except
    Result := False;
  end;
end;

function TDriverUniDAC.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryUniDAC.Create(FConnection,
                                       FDriverTransaction,
                                       FMonitorCallback,
                                       FDriver,
                                       FCacheProvider,
                                       FMetadataCache);
end;

function TDriverUniDAC.BulkLoader: IDBBulkLoader;
begin
  Result := TDriverUniDACBulkLoader.Create(FConnection);
end;

function TDriverUniDAC.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryUniDAC.Create(FConnection,
                                         FDriverTransaction,
                                         FMonitorCallback,
                                         FDriver,
                                         FCacheProvider,
                                         FMetadataCache);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryUniDAC }

constructor TDriverQueryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc; const ADriver: TDriverName;
  const ACache: IDBCacheProvider; const AMetadataCache: IDBMetadataCache);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  inherited Create(ADriverTransaction, AMonitorCallback, ADriver, ACache, AMetadataCache);
  FSQLQuery := TUniSQL.Create(nil);
  try
    FSQLQuery.Connection := AConnection;
  except
    FreeAndNil(FSQLQuery);
    raise;
  end;
end;

destructor TDriverQueryUniDAC.Destroy;
begin
  FreeAndNil(FSQLQuery);
  inherited;
end;

function TDriverQueryUniDAC._InternalExecuteQuery: IDBDataSet;
var
  LResultSet: TUniQuery;
  LFor : Int16;
  LHasMetadataCache: Boolean;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LResultSet := TUniQuery.Create(nil);
  try
    LResultSet.Connection := FSQLQuery.Connection;
    LResultSet.Transaction := _GetTransactionActive;
    LResultSet.SQL.Text := FSQLQuery.SQL.Text;

    LHasMetadataCache := _TryApplyMetadataCache(LResultSet.SQL.Text, LResultSet.FieldDefs);
    
    LResultSet.FetchRows := FFetchOptions.BatchSize;
    if FFetchOptions.Mode in [fmOnDemand, fmManual] then
      LResultSet.SpecificOptions.Values['FetchAll'] := 'False';
    
    if LResultSet.SQL.Text = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    try
      for LFor := 0 to FSQLQuery.Params.Count - 1 do
      begin
        LResultSet.Params[LFor].DataType := FSQLQuery.Params[LFor].DataType;
        LResultSet.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
      end;
      
      if not LResultSet.Prepared then
        LResultSet.Prepare;
      LResultSet.Open;

      if not LHasMetadataCache then
        _TrySaveMetadataCache(LResultSet.SQL.Text, LResultSet.FieldDefs);
      
      Result := TDriverDataSetUniDAC.Create(LResultSet, FMonitorCallback);
      Result.FetchingAll := (FFetchOptions.Mode = fmAll);
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.SQL.Text, E.Message, LResultSet.Params);
        raise;
      end;
    end;
  finally
    if (Result = nil) and Assigned(LResultSet) then
      FreeAndNil(LResultSet);
      
    if Assigned(LResultSet) and (LResultSet.SQL.Text <> EmptyStr) then
      _SetMonitorLog(LResultSet.SQL.Text, LResultSet.Transaction.Name, LResultSet.Params);
  end;
end;

function TDriverQueryUniDAC.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryUniDAC._GetParams: TParams;
begin
  Result := FSQLQuery.Params;
end;

procedure TDriverQueryUniDAC.Prepare;
begin
  FSQLQuery.Prepare;
end;

procedure TDriverQueryUniDAC.Unprepare;
begin
  FSQLQuery.Unprepare;
end;

function TDriverQueryUniDAC.ParamByName(const AValue: string): TParam;
begin
  Result := FSQLQuery.ParamByName(AValue);
end;

function TDriverQueryUniDAC._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

function TDriverQueryUniDAC._GetTransactionActive: TUniTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TUniTransaction;
end;

procedure TDriverQueryUniDAC._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryUniDAC._InternalExecuteDirect;
var
  LExeSQL: TUniSQL;
begin
  LExeSQL := TUniSQL.Create(nil);
  try
    LExeSQL.Connection := FSQLQuery.Connection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := FSQLQuery.SQL.Text;

    if FSQLQuery.Params.Count > 0 then
      _UpdateNativeParams(LExeSQL.Params, FSQLQuery.Params);

    LExeSQL.Prepare;
    LExeSQL.Execute;
    FRowsAffected := LExeSQL.RowsAffected;
  finally
    FreeAndNil(LExeSQL);
  end;
end;

{ TDriverDataSetUniDAC }

constructor TDriverDataSetUniDAC.Create(const ADataSet: TUniQuery;
      const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;


procedure TDriverDataSetUniDAC.ApplyUpdates;
begin
  FDataSet.ApplyUpdates;
end;

procedure TDriverDataSetUniDAC.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

function TDriverDataSetUniDAC.IsCachedUpdates: Boolean;
begin
  Result := FDataSet.CachedUpdates;
end;

function TDriverDataSetUniDAC.IsReadOnly: Boolean;
begin
  Result := FDataSet.ReadOnly;
end;

function TDriverDataSetUniDAC.IsUniDirectional: Boolean;
begin
  Result := FDataSet.UniDirectional;
end;

procedure TDriverDataSetUniDAC.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, FDataSet.Transaction.Name, FDataSet.Params);
  end;
end;

function TDriverDataSetUniDAC.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

function TDriverDataSetUniDAC._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetUniDAC._SetCachedUpdates(const Value: Boolean);
begin
  FDataSet.CachedUpdates := Value;
end;

procedure TDriverDataSetUniDAC._SetCommandText(const ACommandText: String);
begin
  if FDataSet.Active then
    Exit;
  FDataSet.SQL.Text := ACommandText;
end;

procedure TDriverDataSetUniDAC._SetReadOnly(const Value: Boolean);
begin
  FDataSet.ReadOnly := Value;
end;

procedure TDriverDataSetUniDAC._SetUniDirectional(const Value: Boolean);
begin
  FDataSet.UniDirectional := Value;
end;

procedure TDriverDataSetUniDAC._SetFetchOptions(const Value: TFetchOptions);
begin
  inherited _SetFetchOptions(Value);
  FDataSet.FetchRows := Value.BatchSize;
  if Value.Mode in [fmOnDemand, fmManual] then
    FDataSet.SpecificOptions.Values['FetchAll'] := 'False'
  else
    FDataSet.SpecificOptions.Values['FetchAll'] := 'True';
end;


{$ELSE}

interface

implementation

{$ENDIF}

end.
