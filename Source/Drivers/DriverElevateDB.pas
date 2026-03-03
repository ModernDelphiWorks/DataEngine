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

unit DriverElevateDB;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  edbcomps,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverElevateDB = class(TDriverConnection)
  protected
    FConnection: TEDBDatabase;
    FSQLScript: TEDBScript;
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

  TDriverQueryElevateDB = class(TDriverQuery)
  private
    FSQLQuery: TEDBQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetElevateDB = class(TDriverDataSet<TEDBQuery>)
  public
    constructor Create(const ADataSet: TEDBQuery; const AMonitorCallback: TMonitorProc); reintroduce;
  end;

implementation

{ TDriverElevateDB }

constructor TDriverElevateDB.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TEDBDatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  
  FSQLScript := TEDBScript.Create(nil);
  try
    FSQLScript.SessionName := FConnection.SessionName;
    FSQLScript.DatabaseName := FConnection.DatabaseName;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverElevateDB.Destroy;
begin
  FSQLScript.Free;
  inherited;
end;

procedure TDriverElevateDB.Connect;
begin
  if not FConnection.Connected then
    FConnection.Connected := True;
end;

procedure TDriverElevateDB.Disconnect;
begin
  if FConnection.Connected then
    FConnection.Connected := False;
end;

function TDriverElevateDB.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDriverElevateDB.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TEDBQuery;
begin
  LExeSQL := TEDBQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.SessionName := FConnection.SessionName;
    LExeSQL.DatabaseName := FConnection.DatabaseName;
    
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    try
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

procedure TDriverElevateDB.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LExeSQL: TEDBQuery;
  LFor: Integer;
begin
  LExeSQL := TEDBQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.SessionName := FConnection.SessionName;
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

procedure TDriverElevateDB.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverElevateDB.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverElevateDB.ExecuteScripts;
begin
  try
    FSQLScript.ExecScript;
    // ElevateDB scripts might not provide rows affected in the same way, or it depends on the script
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

function TDriverElevateDB.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryElevateDB.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverElevateDB.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryElevateDB.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryElevateDB }

constructor TDriverQueryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TEDBQuery.Create(nil);
  try
    FSQLQuery.SessionName := AConnection.SessionName;
    FSQLQuery.DatabaseName := AConnection.DatabaseName;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryElevateDB.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryElevateDB._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQueryElevateDB._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryElevateDB.ExecuteDirect;
var
  LExeSQL: TEDBQuery;
  LFor: Integer;
begin
  LExeSQL := TEDBQuery.Create(nil);
  try
    if FSQLQuery.DatabaseName = '' then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.SessionName := FSQLQuery.SessionName;
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

function TDriverQueryElevateDB.ExecuteQuery: IDBDataSet;
var
  LDataSet: TEDBQuery;
  LFor: Integer;
begin
  LDataSet := TEDBQuery.Create(nil);
  try
    if FSQLQuery.DatabaseName = '' then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    LDataSet.SessionName := FSQLQuery.SessionName;
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
      Result := TDriverDataSetElevateDB.Create(LDataSet, FMonitorCallback);
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

function TDriverQueryElevateDB.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetElevateDB }

constructor TDriverDataSetElevateDB.Create(const ADataSet: TEDBQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

end.
