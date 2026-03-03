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

unit DriverIBExpress;

interface

uses
  Classes,
  DB,
  Variants,
  SysUtils,
  IBScript,
  IBCustomDataSet,
  IBQuery,
  IBDatabase,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverIBExpress = class(TDriverConnection)
  protected
    FConnection: TIBDatabase;
    FSQLScript: TIBScript;
    function _GetTransactionActive: TIBTransaction;
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

  TDriverQueryIBExpress = class(TDriverQuery)
  private
    FSQLQuery: TIBQuery;
    function _GetTransactionActive: TIBTransaction;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TIBDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetIBExpress = class(TDriverDataSet<TIBQuery>)
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const ADataSet: TIBQuery; const AMonitorCallback: TMonitorProc); reintroduce;
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

{ TDriverIBExpress }

constructor TDriverIBExpress.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction;
  const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  FConnection := AConnection as TIBDatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript := TIBScript.Create(nil);
  try
    FSQLScript.Database := FConnection;
    // Transaction will be assigned on execution based on active transaction
  except
    on E: Exception do
    begin
      FSQLScript.Free;
      raise Exception.Create(E.Message);
    end;
  end;
end;

destructor TDriverIBExpress.Destroy;
begin
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverIBExpress.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverIBExpress.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TIBQuery;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TIBQuery.Create(nil);
  try
    LExeSQL.Database := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := ASQL;
    
    try
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

procedure TDriverIBExpress.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TIBQuery;
  LFor: UInt16;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TIBQuery.Create(nil);
  try
    LExeSQL.Database := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := ASQL;
    if AParams <> nil then
    begin
      for LFor := 0 to AParams.Count - 1 do
      begin
        LExeSQL.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
        LExeSQL.ParamByName(AParams[LFor].Name).Value    := AParams[LFor].Value;
      end;
    end;
    
    try
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

procedure TDriverIBExpress.ExecuteScript(const AScript: String);
begin
  FSQLScript.Script.Text := AScript;
  ExecuteScripts;
end;

procedure TDriverIBExpress.ExecuteScripts;
begin
  if FSQLScript.Script.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  try
    FSQLScript.Transaction := _GetTransactionActive;
    try
      FSQLScript.ExecuteScript;
    except
      on E: Exception do
      begin
        _SetMonitorLog('Error during script execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(FSQLScript.Script.Text, 'DEFAULT', nil);
    FSQLScript.Script.Clear;
  end;
end;

procedure TDriverIBExpress.AddScript(const AScript: String);
begin
  FSQLScript.Script.Add(AScript);
end;

function TDriverIBExpress.GetSQLScripts: String;
begin
  Result := 'Transaction: ' + 'DEFAULT' + ' ' +  FSQLScript.Script.Text;
end;

procedure TDriverIBExpress.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverIBExpress.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverIBExpress._GetTransactionActive: TIBTransaction;
begin
  if Assigned(FDriverTransaction.TransactionActive) then
    Result := FDriverTransaction.TransactionActive as TIBTransaction
  else
    Result := FConnection.DefaultTransaction;
end;

function TDriverIBExpress.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryIBExpress.Create(FConnection,
                                         FDriverTransaction,
                                         FMonitorCallback);
end;

function TDriverIBExpress.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryIBExpress.Create(FConnection,
                                           FDriverTransaction,
                                           FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryIBExpress }

constructor TDriverQueryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TIBQuery.Create(nil);
  try
    FSQLQuery.Database := AConnection;
    // Transaction will be assigned on execution
    FSQLQuery.UniDirectional := True;
  except
    on E: Exception do
    begin
      FSQLQuery.Free;
      raise Exception.Create(E.Message);
    end;
  end;
end;

destructor TDriverQueryIBExpress.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryIBExpress._GetTransactionActive: TIBTransaction;
begin
  if Assigned(FDriverTransaction.TransactionActive) then
    Result := FDriverTransaction.TransactionActive as TIBTransaction
  else
    Result := FSQLQuery.Database.DefaultTransaction;
end;

function TDriverQueryIBExpress.ExecuteQuery: IDBDataSet;
var
  LResultSet: TIBQuery;
  LFor: UInt16;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LResultSet := TIBQuery.Create(nil);
  try
    LResultSet.Database := FSQLQuery.Database;
    LResultSet.Transaction := _GetTransactionActive;
    LResultSet.UniDirectional := True;
    LResultSet.SQL.Text := FSQLQuery.SQL.Text;
    
    if LResultSet.SQL.Text = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    for LFor := 0 to FSQLQuery.Params.Count - 1 do
    begin
      LResultSet.Params[LFor].DataType := FSQLQuery.Params[LFor].DataType;
      LResultSet.Params[LFor].Value    := FSQLQuery.Params[LFor].Value;
    end;
    
    try
      LResultSet.Open;
      Result := TDriverDataSetIBExpress.Create(LResultSet, FMonitorCallback);
      if LResultSet.RecordCount = 0 then
         Result.FetchingAll := True;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.SQL.Text, E.Message, LResultSet.Params);
        raise;
      end;
    end;
  finally
    if LResultSet.SQL.Text <> EmptyStr then
      _SetMonitorLog(LResultSet.SQL.Text, 'DEFAULT', LResultSet.Params);
  end;
  except
    if Assigned(LResultSet) and (Result = nil) then
      LResultSet.Free;
    raise;
  end;
end;

function TDriverQueryIBExpress._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQueryIBExpress._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryIBExpress.ExecuteDirect;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  if FSQLQuery.SQL.Text = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  FSQLQuery.Transaction := _GetTransactionActive;
  try
    FSQLQuery.ExecSQL;
    FRowsAffected := FSQLQuery.RowsAffected;
    _SetMonitorLog(FSQLQuery.SQL.Text, 'DEFAULT', FSQLQuery.Params);
  except
    on E: Exception do
    begin
      _SetMonitorLog(FSQLQuery.SQL.Text, E.Message, FSQLQuery.Params);
      raise;
    end;
  end;
end;

function TDriverQueryIBExpress.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetIBExpress }

constructor TDriverDataSetIBExpress.Create(const ADataSet: TIBQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetIBExpress.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetIBExpress.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, 'DEFAULT', FDataSet.Params);
  end;
end;

function TDriverDataSetIBExpress.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

function TDriverDataSetIBExpress._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetIBExpress._SetCachedUpdates(const Value: Boolean);
begin
  FDataSet.CachedUpdates := Value;
end;

procedure TDriverDataSetIBExpress._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

procedure TDriverDataSetIBExpress._SetReadOnly(const Value: Boolean);
begin
  // IBQuery does not have ReadOnly property exposed directly in all versions, 
  // but we can assume standard dataset behavior or ignore if not supported
end;

procedure TDriverDataSetIBExpress._SetUniDirectional(const Value: Boolean);
begin
  FDataSet.UniDirectional := Value;
end;

procedure TDriverDataSetIBExpress.ApplyUpdates;
begin
  // IBQuery is usually read-only or direct SQL. For updates, use CachedUpdates + ApplyUpdates 
  // if using IBDataSet/IBTable, but here we have TIBQuery.
  // Assuming standard TDataSet.ApplyUpdates if available or no-op/exception for TIBQuery
  if FDataSet.CachedUpdates then
    FDataSet.ApplyUpdates;
end;

procedure TDriverDataSetIBExpress.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

function TDriverDataSetIBExpress.IsCachedUpdates: Boolean;
begin
  Result := FDataSet.CachedUpdates;
end;

function TDriverDataSetIBExpress.IsReadOnly: Boolean;
begin
  Result := False; // Default assumption
end;

function TDriverDataSetIBExpress.IsUniDirectional: Boolean;
begin
  Result := FDataSet.UniDirectional;
end;

end.
