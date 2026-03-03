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

unit DriverNexusDB;

interface

uses
  Classes,
  DB,
  Variants,
  StrUtils,
  SysUtils,
  nxdb,
  nxllComponent,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverNexusDB = class(TDriverConnection)
  protected
    FConnection: TnxDatabase;
    FSQLScript: TnxQuery;
    function _GetTransactionActive: TComponent;
  public
    constructor Create(const AConnection: TComponent;
      const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure ExecuteDirect(const ASQL: String); override;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryNexusDB = class(TDriverQuery)
  private
    FnxQuery: TnxQuery;
    function _GetTransactionActive: TComponent;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TnxDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetNexusDB = class(TDriverDataSet<TnxQuery>)
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const ADataSet: TnxQuery; const AMonitorCallback: TMonitorProc); reintroduce;
    destructor Destroy; override;
    procedure Open; override;
    procedure ApplyUpdates; override;
    procedure CancelUpdates; override;
    function RowsAffected: UInt32; override;
    function IsUniDirectional: Boolean; override;
    function IsReadOnly: Boolean; override;
    function IsCachedUpdates: Boolean; override;
  end;

implementation

{ TDriverNexusDB }

constructor TDriverNexusDB.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction;
  const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  FConnection := AConnection as TnxDatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript := TnxQuery.Create(nil);
  try
    FSQLScript.Session := FConnection.Session;
    FSQLScript.Database := FConnection;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverNexusDB.Destroy;
begin
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverNexusDB.Disconnect;
begin
  FConnection.Connected := False;
end;

function TDriverNexusDB._GetTransactionActive: TComponent;
begin
  Result := FDriverTransaction.TransactionActive;
end;

procedure TDriverNexusDB.ExecuteDirect(const ASQL: String);
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  try
    FConnection.ExecQuery(ASQL, []);
  except
    on E: Exception do
    begin
      _SetMonitorLog(ASQL, E.Message, nil);
      raise;
    end;
  end;
  _SetMonitorLog(ASQL, 'DEFAULT', nil);
end;

procedure TDriverNexusDB.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TnxQuery;
  LFor: UInt16;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TnxQuery.Create(nil);
  try
    LExeSQL.Session := FConnection.Session;
    LExeSQL.Database := FConnection;
    LExeSQL.SQL.Text   := ASQL;
    for LFor := 0 to AParams.Count - 1 do
    begin
      LExeSQL.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
      LExeSQL.ParamByName(AParams[LFor].Name).Value := AParams[LFor].Value;
    end;
    try
      LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, LExeSQL.Params);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

procedure TDriverNexusDB.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverNexusDB.ExecuteScripts;
begin
  if FSQLScript.SQL.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  FConnection.Connected := True;
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  try
    try
      FSQLScript.ExecSQL;
    except
      on E: Exception do
      begin
        _SetMonitorLog('Error during script execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(FSQLScript.SQL.Text, 'DEFAULT', nil);
    FSQLScript.SQL.Clear;
  end;
end;

procedure TDriverNexusDB.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

function TDriverNexusDB.GetSQLScripts: String;
begin
  Result := 'Transaction: ' + 'DEFAULT' + ' ' +  FSQLScript.SQL.Text;
end;

procedure TDriverNexusDB.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverNexusDB.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverNexusDB.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryNexusDB.Create(FConnection,
                                       FDriverTransaction,
                                       FMonitorCallback);
end;

function TDriverNexusDB.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryNexusDB.Create(FConnection,
                                         FDriverTransaction,
                                         FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result   := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryNexusDB }

constructor TDriverQueryNexusDB.Create(const AConnection: TnxDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FnxQuery := TnxQuery.Create(nil);
  try
    FnxQuery.Session := AConnection.Session;
    FnxQuery.Database := AConnection;
  except
    FnxQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryNexusDB.Destroy;
begin
  FnxQuery.Free;
  inherited;
end;

function TDriverQueryNexusDB._GetTransactionActive: TComponent;
begin
  Result := FDriverTransaction.TransactionActive;
end;

function TDriverQueryNexusDB.ExecuteQuery: IDBResultSet;
var
  LResultSet: TnxQuery;
  LFor: UInt16;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LResultSet := TnxQuery.Create(nil);
  try
    LResultSet.Session := FnxQuery.Session;
    LResultSet.Database := FnxQuery.Database;
    LResultSet.SQL.Text   := FnxQuery.SQL.Text;
    
    if FnxQuery.SQL.Text = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    for LFor := 0 to FnxQuery.Params.Count - 1 do
    begin
      LResultSet.Params[LFor].DataType := FnxQuery.Params[LFor].DataType;
      LResultSet.Params[LFor].Value    := FnxQuery.Params[LFor].Value;
    end;
    
    try
      LResultSet.Open;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.SQL.Text, E.Message, LResultSet.Params);
        LResultSet.Free;
        raise;
      end;
    end;
    
    Result := TDriverDataSetNexusDB.Create(LResultSet, FMonitorCallback);
    if LResultSet.RecordCount = 0 then
       Result.FetchingAll := True;
  except
    on E: Exception do
    begin
       // If exception happened before result creation, we need to ensure LResultSet is freed if not assigned to Result
       // But here we use try..except inside.
       if Assigned(LResultSet) and (Result = nil) then
         LResultSet.Free;
       raise;
    end;
  end;
end;

function TDriverQueryNexusDB._GetCommandText: String;
begin
  Result := FnxQuery.SQL.Text;
end;

procedure TDriverQueryNexusDB._SetCommandText(const ACommandText: String);
begin
  FnxQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryNexusDB.ExecuteDirect;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
    
  if FnxQuery.SQL.Text = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  try
    FnxQuery.ExecSQL;
    FRowsAffected := FnxQuery.RowsAffected;
    _SetMonitorLog(FnxQuery.SQL.Text, 'DEFAULT', FnxQuery.Params);
  except
    on E: Exception do
    begin
      _SetMonitorLog(FnxQuery.SQL.Text, E.Message, FnxQuery.Params);
      raise;
    end;
  end;
end;

function TDriverQueryNexusDB.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetNexusDB }

constructor TDriverDataSetNexusDB.Create(const ADataSet: TnxQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetNexusDB.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetNexusDB.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, 'DEFAULT', FDataSet.Params);
  end;
end;

function TDriverDataSetNexusDB.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

function TDriverDataSetNexusDB._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetNexusDB._SetCachedUpdates(const Value: Boolean);
begin
  FDataSet.CachedUpdates := Value;
end;

procedure TDriverDataSetNexusDB._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

procedure TDriverDataSetNexusDB._SetReadOnly(const Value: Boolean);
begin
  FDataSet.ReadOnly := Value;
end;

procedure TDriverDataSetNexusDB._SetUniDirectional(const Value: Boolean);
begin
  FDataSet.UniDirectional := Value;
end;

procedure TDriverDataSetNexusDB.ApplyUpdates;
begin
  if FDataSet.CachedUpdates then
    FDataSet.ApplyUpdates;
end;

procedure TDriverDataSetNexusDB.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

function TDriverDataSetNexusDB.IsCachedUpdates: Boolean;
begin
  Result := FDataSet.CachedUpdates;
end;

function TDriverDataSetNexusDB.IsReadOnly: Boolean;
begin
  Result := FDataSet.ReadOnly;
end;

function TDriverDataSetNexusDB.IsUniDirectional: Boolean;
begin
  Result := FDataSet.UniDirectional;
end;

end.
