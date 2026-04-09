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

unit DataEngine.DriverFireDac;

interface

uses
  DB,
  Classes,
  SysUtils,
  StrUtils,
  Variants,
  FireDAC.Comp.Client,
  FireDAC.Comp.Script,
  FireDAC.Comp.ScriptCommands,
  FireDAC.DApt,
  FireDAC.Stan.Param,
  FireDAC.Phys.Intf,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.Metadata.Manager;

type
  TFDQueryHelper = class Helper for TFDQuery
  public
    function AsParams: TParams;
  end;

  TDriverFireDAC = class(TDriverConnection)
  private
    function _GetTransactionActive: TFDTransaction;
    procedure _DoMonitorLog(ASender, AInitiator: TObject; var AException: Exception);
  protected
    FConnection: TFDConnection;
    procedure _InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil); override;
    function _IsAlive: Boolean; override;
  public
    constructor Create(const AConnection: TComponent; const ADriverTransaction: TDriverTransaction;
      const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc;
      const ACache: IDBCacheProvider = nil; const AMetadataCache: IDBMetadataCache = nil); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryFireDAC = class(TDriverQuery)
  private
    FFDQuery: TFDQuery;
    function _GetTransactionActive: TFDTransaction;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TFDConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc; const ADriver: TDBEngineDriver;
      const ACache: IDBCacheProvider = nil; const AMetadataCache: IDBMetadataCache = nil);
    destructor Destroy; override;
    procedure _InternalExecuteDirect; override;
    function _InternalExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
    function _GetParams: TParams; override;
    procedure Prepare; override;
    procedure Unprepare; override;
    function ParamByName(const AValue: string): TParam; override;
  end;

  TDriverDataSetFireDAC = class(TDriverDataSet<TFDQuery>)
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
    procedure _SetFetchOptions(const Value: TFetchOptions); override;
  public
    constructor Create(const ADataSet: TFDQuery; const AMonitorCallback: TMonitorProc); reintroduce;
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
  DataEngine.DriverFireDacBulkLoader;

{ TFDQueryHelper }

function TFDQueryHelper.AsParams: TParams;
var
  LFor: Int16;
begin
  Result := TParams.Create;
  for LFor := 0 to Self.Params.Count - 1 do
  begin
    Result.Add;
    Result[LFor].DataType := Self.Params[LFor].DataType;
    Result[LFor].Value := Self.Params[LFor].Value;
  end;
end;

{ TDriverFireDAC }

procedure TDriverFireDAC._DoMonitorLog(ASender, AInitiator: TObject;
  var AException: Exception);
begin
  _SetMonitorLog(AException.Message, FSQLScript.CurrentCommand.EngineIntf.CommandIntf.SQLText, nil);
end;

constructor TDriverFireDAC.Create(const AConnection: TComponent; const ADriverTransaction: TDriverTransaction;
  const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc;
  const ACache: IDBCacheProvider; const AMetadataCache: IDBMetadataCache);
begin
  inherited Create(AConnection, ADriverTransaction, ADriver, AMonitorCallback, ACache, AMetadataCache);
  FConnection := AConnection as TFDConnection;
  FSQLScript := TFDScript.Create(nil);
  try
    FSQLScript.Connection := FConnection;
    FSQLScript.SQLScripts.Add;
    FSQLScript.ScriptOptions.Reset;
    FSQLScript.ScriptOptions.BreakOnError := True;
    FSQLScript.ScriptOptions.RaisePLSQLErrors := True;
    FSQLScript.ScriptOptions.EchoCommands := ecNone;
    FSQLScript.ScriptOptions.CommandSeparator := ';';
    FSQLScript.ScriptOptions.CommitEachNCommands := 1000;
    FSQLScript.ScriptOptions.DropNonexistObj := True;
  except
    FreeAndNil(FSQLScript);
    raise;
  end;
end;

destructor TDriverFireDAC.Destroy;
begin
  FConnection := nil;
  FDriverTransaction := nil;
  FreeAndNil(FSQLScript);
  inherited;
end;

procedure TDriverFireDAC.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverFireDAC._InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil);
var
  LExeSQL: TFDQuery;
begin
  LExeSQL := TFDQuery.Create(nil);
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

procedure TDriverFireDAC.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverFireDAC.ExecuteScripts;
var
  LErrorLogged: Boolean;
  LCommand: IFDPhysCommand;
begin
  if FSQLScript.SQLScripts.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  FRowsAffected := 0;
  FSQLScript.OnError := _DoMonitorLog;
  try
    if _GetTransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    FSQLScript.Transaction := _GetTransactionActive;
    if not FSQLScript.ValidateAll then
      raise Exception.Create('One or more SQL scripts are invalid. Execution stopped.');
    try
      while FSQLScript.ExecuteStep do
      begin
        if Assigned(FSQLScript.CurrentCommand) and
           Supports(FSQLScript.CurrentCommand.EngineIntf.CommandIntf, IFDPhysCommand) then
        begin
          LCommand := FSQLScript.CurrentCommand.EngineIntf.CommandIntf;
          if LCommand.RowsAffected >= 0 then
            FRowsAffected := FRowsAffected + UInt32(LCommand.RowsAffected);
        end;
      end;
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog('Database error during script execution', E.Message, nil);
        raise;
      end;
      on E: Exception do
      begin
        _SetMonitorLog('General error during script execution', E.Message, nil);
        raise;
      end;
    end;  
  finally
    if FSQLScript.TotalErrors = 0 then
    begin
      _SetMonitorLog(FSQLScript.SQLScripts.Items[0].SQL.Text, FSQLScript.Transaction.Name, nil);
      _SetMonitorLog(Format('Script completed: %.1f%% done', [FSQLScript.TotalPct10Done / 10.0]),
                     FSQLScript.Transaction.Name, nil);
    end
    else
      _SetMonitorLog('Script execution completed with errors.', '', nil);

    if FSQLScript.SQLScripts.Count > 0 then
      FSQLScript.SQLScripts[0].SQL.Clear;

    FSQLScript.OnError := nil;
  end;
end;

function TDriverFireDAC.GetSQLScripts: String;
begin
  Result := 'Transaction: ' + FSQLScript.Transaction.Name + ' ' + FSQLScript.SQLScripts.Items[0].SQL.Text;
end;

procedure TDriverFireDAC.AddScript(const AScript: String);
var
  LSQLScript: TFDSQLScript;
begin
  if Self.GetDriver in [TDBEngineDriver.dnInterbase,
                        TDBEngineDriver.dnFirebird,
                        TDBEngineDriver.dnFirebird3] then
  begin
    LSQLScript := FSQLScript.SQLScripts.Items[0];
    if LSQLScript.SQL.Count = 0 then
      LSQLScript.SQL.Add('SET AUTOCOMMIT OFF');
  end;
  FSQLScript.SQLScripts[0].SQL.Add(AScript);
end;

procedure TDriverFireDAC.ApplyUpdates(const ADataSets: array of IDBDataSet);
var
  LDataSet: IDBDataSet;
begin
  inherited;
  for LDataSet in ADataSets do
    LDataSet.ApplyUpdates;
end;

procedure TDriverFireDAC.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverFireDAC.IsConnected: Boolean;
begin
  Result := FConnection.Connected = True;
end;

function TDriverFireDAC._GetTransactionActive: TFDTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TFDTransaction;
end;

function TDriverFireDAC.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryFireDAC.Create(FConnection,
                                       FDriverTransaction,
                                       FMonitorCallback,
                                       FDriver,
                                       FCacheProvider,
                                       FMetadataCache);
end;

function TDriverFireDAC.BulkLoader: IDBBulkLoader;
begin
  Result := TDriverFireDACBulkLoader.Create(FConnection, _GetTransactionActive);
end;

function TDriverFireDAC.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryFireDAC.Create(FConnection,
                                         FDriverTransaction,
                                         FMonitorCallback,
                                         FDriver,
                                         FCacheProvider,
                                         FMetadataCache);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

function TDriverFireDAC._IsAlive: Boolean;
begin
  try
    Result := FConnection.Connected and (FConnection.Ping);
  except
    Result := False;
  end;
end;

{ TDriverQueryFireDAC }

constructor TDriverQueryFireDAC.Create(const AConnection: TFDConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc; const ADriver: TDBEngineDriver;
  const ACache: IDBCacheProvider; const AMetadataCache: IDBMetadataCache);
begin
  if AConnection = nil then
    raise EArgumentNilException.Create('AConnection cannot be nil');

  inherited Create(ADriverTransaction, AMonitorCallback, ADriver, ACache, AMetadataCache);

  FFDQuery := TFDQuery.Create(nil);
  try
    FFDQuery.Connection := AConnection;
  except
    if Assigned(FFDQuery) then
      FreeAndNil(FFDQuery);
    raise;
  end;
end;

destructor TDriverQueryFireDAC.Destroy;
begin
  FreeAndNil(FFDQuery);
  inherited;
end;

function TDriverQueryFireDAC._InternalExecuteQuery: IDBDataSet;
var
  LDataSet: TFDQuery;
  LParams: TParams;
  LFor: Int16;
  LHasMetadataCache: Boolean;
begin
  LDataSet := TFDQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FFDQuery.Connection) then
      raise Exception.Create('Connection not assigned.');
    if _GetTransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LDataSet.Connection := FFDQuery.Connection;
    LDataSet.Transaction := _GetTransactionActive;
    LDataSet.SQL.Text := FFDQuery.SQL.Text;

    LHasMetadataCache := False;
    LHasMetadataCache := _TryApplyMetadataCache(LDataSet.SQL.Text, LDataSet.FieldDefs);
    if LHasMetadataCache then
      LDataSet.FetchOptions.AutoGetFieldDefs := False;

    LDataSet.FetchOptions.Mode := TFDFetchMode(FFetchOptions.Mode);
    LDataSet.FetchOptions.RowsetSize := FFetchOptions.BatchSize;
    if FFetchOptions.Mode = fmOnDemand then
      LDataSet.FetchOptions.Unidirectional := True;

    try
      if FFDQuery.Params.Count > 0 then
      begin
        for LFor := 0 to FFDQuery.Params.Count - 1 do
        begin
          if not Assigned(FFDQuery.Params[LFor]) then
            raise Exception.Create('Invalid or unassigned parameter.');

          LDataSet.Params[LFor].DataType := FFDQuery.Params[LFor].DataType;
          LDataSet.Params[LFor].Value := FFDQuery.Params[LFor].Value;
        end;
      end;
      if LDataSet.SQL.Text = '' then
        raise Exception.Create('SQL statement is empty. Cannot execute the query.');

      try
        if not LDataSet.Prepared then
          LDataSet.Prepare;
      except
        on E: Exception do
        begin
          _SetMonitorLog('Error preparing query', E.Message, LParams);
          raise;
        end;
      end;

      LDataSet.Open;

      if not LHasMetadataCache then
        _TrySaveMetadataCache(LDataSet.SQL.Text, LDataSet.FieldDefs);
      Result := TDriverDataSetFireDAC.Create(LDataSet, FMonitorCallback);
      Result.FetchingAll := (FFetchOptions.Mode = fmAll);
      LParams := LDataSet.AsParams;
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog(LDataSet.SQL.Text, E.Message, LParams);
        FreeAndNil(LDataSet);
        raise;
      end;
      on E: Exception do
      begin
        _SetMonitorLog('General error', E.Message, nil);
        FreeAndNil(LDataSet);
        raise;
      end;
    end;
  finally
    if Assigned(LDataSet) then
    begin
      if (not LDataSet.Active) and (LDataSet.SQL.Text <> '') then
        _SetMonitorLog(LDataSet.SQL.Text, LDataSet.Transaction.Name, LParams);
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      FreeAndNil(LParams);
    end;
  end;
end;

function TDriverQueryFireDAC.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryFireDAC._GetCommandText: String;
begin
  Result := FFDQuery.SQL.Text;
end;

function TDriverQueryFireDAC._GetParams: TParams;
begin
  Result := FFDQuery.Params;
end;

function TDriverQueryFireDAC._GetTransactionActive: TFDTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TFDTransaction;
end;

function TDriverQueryFireDAC.ParamByName(const AValue: string): TParam;
begin
  Result := TParam(FFDQuery.ParamByName(AValue));
end;

procedure TDriverQueryFireDAC.Prepare;
begin
  FFDQuery.Prepare;
end;

procedure TDriverQueryFireDAC.Unprepare;
begin
  FFDQuery.Unprepare;
end;

procedure TDriverQueryFireDAC._SetCommandText(const ACommandText: String);
begin
  FFDQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryFireDAC._InternalExecuteDirect;
var
  LExeSQL: TFDQuery;
begin
  LExeSQL := TFDQuery.Create(nil);
  try
    LExeSQL.Connection := FFDQuery.Connection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := FFDQuery.SQL.Text;

    if FFDQuery.Params.Count > 0 then
      _UpdateNativeParams(LExeSQL.Params, FFDQuery.Params);

    LExeSQL.Prepare;
    LExeSQL.Execute;
    FRowsAffected := LExeSQL.RowsAffected;
  finally
    FreeAndNil(LExeSQL);
  end;
end;

{ TDriverDataSetFireDAC }

procedure TDriverDataSetFireDAC.ApplyUpdates;
begin
  FDataSet.ApplyUpdates;
end;

procedure TDriverDataSetFireDAC.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

constructor TDriverDataSetFireDAC.Create(const ADataSet: TFDQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;


function TDriverDataSetFireDAC.IsCachedUpdates: Boolean;
begin
  Result := FDataSet.CachedUpdates;
end;

function TDriverDataSetFireDAC.IsReadOnly: Boolean;
begin
  Result := FDataSet.ReadOnly;
end;

function TDriverDataSetFireDAC.IsUniDirectional: Boolean;
begin
  Result := FDataSet.IsUniDirectional;
end;

procedure TDriverDataSetFireDAC.Open;
var
  LParams: TParams;
begin
  try
    inherited Open;
    LParams := FDataSet.AsParams;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, FDataSet.Transaction.Name, LParams);
    if Assigned(LParams) then
    begin
      LParams.Clear;
      FreeAndNil(LParams);
    end;
  end;
end;

function TDriverDataSetFireDAC.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

function TDriverDataSetFireDAC._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetFireDAC._SetCachedUpdates(const Value: Boolean);
begin
  FDataSet.CachedUpdates := Value;
end;

procedure TDriverDataSetFireDAC._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

procedure TDriverDataSetFireDAC._SetReadOnly(const Value: Boolean);
begin
  FDataSet.ReadOnly := Value;
end;

procedure TDriverDataSetFireDAC._SetUniDirectional(const Value: Boolean);
begin
  FDataSet.FetchOptions.Unidirectional := Value;
end;

procedure TDriverDataSetFireDAC._SetFetchOptions(const Value: TFetchOptions);
begin
  inherited _SetFetchOptions(Value);
  FDataSet.FetchOptions.Mode := TFDFetchMode(Value.Mode);
  FDataSet.FetchOptions.RowsetSize := Value.BatchSize;
  if Value.Mode = fmOnDemand then
    FDataSet.FetchOptions.Unidirectional := True;
end;

end.


