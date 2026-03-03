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

unit DriverSQLDB;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  SQLDB,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverSQLdb = class(TDriverConnection)
  private
    function _GetTransactionActive: TSQLTransaction;
  protected
    FConnection: TSQLConnection;
    FSQLScript: TSQLScript;
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

  TDriverQuerySQLdb = class(TDriverQuery)
  private
    FSQLQuery: TSQLQuery;
    function _GetTransactionActive: TSQLTransaction;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TSQLConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetSQLdb = class(TDriverDataSet<TSQLQuery>)
  public
    constructor Create(const ADataSet: TSQLQuery; const AMonitorCallback: TMonitorProc); reintroduce;
  end;

implementation

{ TDriverSQLdb }

constructor TDriverSQLdb.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TSQLConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  
  FSQLScript := TSQLScript.Create(nil);
  try
    FSQLScript.Database := FConnection;
    FSQLScript.Script.Clear;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverSQLdb.Destroy;
begin
  FSQLScript.Free;
  inherited;
end;

procedure TDriverSQLdb.Connect;
begin
  if not FConnection.Connected then
    FConnection.Connected := True;
end;

procedure TDriverSQLdb.Disconnect;
begin
  if FConnection.Connected then
    FConnection.Connected := False;
end;

function TDriverSQLdb.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverSQLdb._GetTransactionActive: TSQLTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TSQLTransaction;
end;

procedure TDriverSQLdb.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TSQLQuery;
begin
  LExeSQL := TSQLQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Database := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    try
      if not LExeSQL.Prepared then
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

procedure TDriverSQLdb.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LExeSQL: TSQLQuery;
  LFor: Integer;
begin
  LExeSQL := TSQLQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Database := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    
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
      if not LExeSQL.Prepared then
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

procedure TDriverSQLdb.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverSQLdb.AddScript(const AScript: String);
begin
  // Special handling for Firebird/Interbase auto-commit
  if (FDriver in [dnInterbase, dnFirebird, dnFirebird3]) and (FSQLScript.Script.Count = 0) then
     FSQLScript.Script.Add('SET AUTOCOMMIT OFF');

  FSQLScript.Script.Add(AScript);
end;

procedure TDriverSQLdb.ExecuteScripts;
begin
  if FSQLScript.Script.Count = 0 then
    Exit;
    
  try
    FSQLScript.Transaction := _GetTransactionActive;
    FSQLScript.Execute;
    FRowsAffected := 0; // SQLScript usually doesn't return rows affected easily
    _SetMonitorLog(FSQLScript.Script.Text, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog('Error executing script', E.Message, nil);
      raise;
    end;
  end;
  FSQLScript.Script.Clear;
end;

function TDriverSQLdb.CreateQuery: IDBQuery;
begin
  Result := TDriverQuerySQLdb.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverSQLdb.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQuerySQLdb.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQuerySQLdb }

constructor TDriverQuerySQLdb.Create(const AConnection: TSQLConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TSQLQuery.Create(nil);
  try
    FSQLQuery.Database := AConnection;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQuerySQLdb.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQuerySQLdb._GetTransactionActive: TSQLTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TSQLTransaction;
end;

function TDriverQuerySQLdb._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQuerySQLdb._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQuerySQLdb.ExecuteDirect;
var
  LExeSQL: TSQLQuery;
  LFor: Integer;
begin
  LExeSQL := TSQLQuery.Create(nil);
  try
    if FSQLQuery.Database = nil then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Database := FSQLQuery.Database;
    LExeSQL.Transaction := _GetTransactionActive;
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
      if not LExeSQL.Prepared then
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

function TDriverQuerySQLdb.ExecuteQuery: IDBDataSet;
var
  LDataSet: TSQLQuery;
  LFor: Integer;
begin
  LDataSet := TSQLQuery.Create(nil);
  try
    if FSQLQuery.Database = nil then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    LDataSet.Database := FSQLQuery.Database;
    LDataSet.Transaction := _GetTransactionActive;
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
        
      if not LDataSet.Prepared then
        LDataSet.Prepare;
      LDataSet.Open;
      Result := TDriverDataSetSQLdb.Create(LDataSet, FMonitorCallback);
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

function TDriverQuerySQLdb.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetSQLdb }

constructor TDriverDataSetSQLdb.Create(const ADataSet: TSQLQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

end.
