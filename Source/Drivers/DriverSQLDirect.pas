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

unit DriverSQLDirect;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  SDEngine,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverSQLDirect = class(TDriverConnection)
  protected
    FConnection: TSDDatabase;
    FSQLScript: TSDScript;
  public
    constructor Create(const AConnection: TComponent;
      const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure ExecuteDirect(const ASQL: String); override;
    procedure ExecuteDirect(const ASQL: String;
      const AParams: TParams); override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String): IDBResultSet; override;
  end;

  TDriverQuerySQLDirect = class(TDriverQuery)
  private
    FSQLQuery: TSDQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TSDDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetSQLDirect = class(TDriverDataSet<TSDQuery>)
  public
    constructor Create(const ADataSet: TSDQuery; const AMonitorCallback: TMonitorProc); reintroduce;
  end;

implementation

{ TDriverSQLDirect }

constructor TDriverSQLDirect.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TSDDatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  
  FSQLScript := TSDScript.Create(nil);
  try
    FSQLScript.DatabaseName := FConnection.DatabaseName;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverSQLDirect.Destroy;
begin
  FSQLScript.Free;
  inherited;
end;

procedure TDriverSQLDirect.Connect;
begin
  if not FConnection.Connected then
    FConnection.Connected := True;
end;

procedure TDriverSQLDirect.Disconnect;
begin
  if FConnection.Connected then
    FConnection.Connected := False;
end;

function TDriverSQLDirect.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDriverSQLDirect.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TSDQuery;
begin
  LExeSQL := TSDQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.DatabaseName := FConnection.DatabaseName;
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    try
      LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
      on E: Exception do
      begin
        _SetMonitorLog('General error during direct execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    if Assigned(LExeSQL) then
    begin
      _SetMonitorLog(LExeSQL.SQL.Text, '', nil);
      LExeSQL.Free;
    end;
  end;
end;

procedure TDriverSQLDirect.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LExeSQL: TSDQuery;
  LFor: Integer;
begin
  LExeSQL := TSDQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.DatabaseName := FConnection.DatabaseName;
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    if AParams.Count > 0 then
    begin
      for LFor := 0 to AParams.Count - 1 do
      begin
        if not Assigned(AParams[LFor]) then
          raise Exception.Create(Format('Parameter "%s" is invalid or unassigned.', [AParams[LFor].Name]));
          
        LExeSQL.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
        LExeSQL.ParamByName(AParams[LFor].Name).Value    := AParams[LFor].Value;
      end;
    end;
    
    try
      LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
      on E: Exception do
      begin
        _SetMonitorLog('General error during direct execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    if Assigned(LExeSQL) then
    begin
      _SetMonitorLog(LExeSQL.SQL.Text, '', AParams);
      LExeSQL.Free;
    end;
  end;
end;

procedure TDriverSQLDirect.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverSQLDirect.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverSQLDirect.ExecuteScripts;
begin
  try
    FSQLScript.ExecSQL;
    // SQLDirect script execution doesn't easily return rows affected for the whole batch
    FRowsAffected := 0;
    _SetMonitorLog(FSQLScript.SQL.Text, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog('Error executing script', E.Message, nil);
      raise;
    end;
  end;
  FSQLScript.SQL.Clear;
end;

function TDriverSQLDirect.CreateQuery: IDBQuery;
begin
  Result := TDriverQuerySQLDirect.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverSQLDirect.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQuerySQLDirect.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQuerySQLDirect }

constructor TDriverQuerySQLDirect.Create(const AConnection: TSDDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TSDQuery.Create(nil);
  try
    FSQLQuery.DatabaseName := AConnection.DatabaseName;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQuerySQLDirect.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQuerySQLDirect._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQuerySQLDirect._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQuerySQLDirect.ExecuteDirect;
var
  LExeSQL: TSDQuery;
  LFor: Integer;
begin
  LExeSQL := TSDQuery.Create(nil);
  try
    if FSQLQuery.DatabaseName = '' then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.DatabaseName := FSQLQuery.DatabaseName;
    LExeSQL.SQL.Text := FSQLQuery.SQL.Text;
    
    if FSQLQuery.Params.Count > 0 then
    begin
      for LFor := 0 to FSQLQuery.Params.Count - 1 do
      begin
        LExeSQL.ParamByName(FSQLQuery.Params[LFor].Name).DataType := FSQLQuery.Params[LFor].DataType;
        LExeSQL.ParamByName(FSQLQuery.Params[LFor].Name).Value    := FSQLQuery.Params[LFor].Value;
      end;
    end;
    
    try
      LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog(LExeSQL.SQL.Text, E.Message, nil);
        raise;
      end;
      on E: Exception do
      begin
        _SetMonitorLog('General error', E.Message, nil);
        raise;
      end;
    end;
  finally
    if Assigned(LExeSQL) then
    begin
      _SetMonitorLog(LExeSQL.SQL.Text, '', nil);
      LExeSQL.Free;
    end;
  end;
end;

function TDriverQuerySQLDirect.ExecuteQuery: IDBDataSet;
var
  LDataSet: TSDQuery;
  LFor: Integer;
begin
  LDataSet := TSDQuery.Create(nil);
  try
    if FSQLQuery.DatabaseName = '' then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    LDataSet.DatabaseName := FSQLQuery.DatabaseName;
    LDataSet.SQL.Text := FSQLQuery.SQL.Text;
    
    try
      if FSQLQuery.Params.Count > 0 then
      begin
        for LFor := 0 to FSQLQuery.Params.Count - 1 do
        begin
           LDataSet.ParamByName(FSQLQuery.Params[LFor].Name).DataType := FSQLQuery.Params[LFor].DataType;
           LDataSet.ParamByName(FSQLQuery.Params[LFor].Name).Value    := FSQLQuery.Params[LFor].Value;
        end;
      end;
      
      if LDataSet.SQL.Text = '' then
        raise Exception.Create('SQL statement is empty. Cannot execute the query.');
        
      LDataSet.Prepare;
      LDataSet.Open;
      Result := TDriverDataSetSQLDirect.Create(LDataSet, FMonitorCallback);
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog(LDataSet.SQL.Text, E.Message, nil);
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
    if Assigned(LDataSet) and (not Assigned(Result)) then 
       LDataSet.Free; 
  end;
end;

function TDriverQuerySQLDirect.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetSQLDirect }

constructor TDriverDataSetSQLDirect.Create(const ADataSet: TSDQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

end.
