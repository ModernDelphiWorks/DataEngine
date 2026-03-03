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

unit DriverAbsoluteDB;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  ABSMain,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverAbsoluteDB = class(TDriverConnection)
  protected
    FConnection: TABSDatabase;
    FSQLScript: TABSQuery;
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

  TDriverQueryAbsoluteDB = class(TDriverQuery)
  private
    FSQLQuery: TABSQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TABSDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetAbsoluteDB = class(TDriverDataSet<TABSQuery>)
  public
    constructor Create(const ADataSet: TABSQuery; const AMonitorCallback: TMonitorProc); reintroduce;
  end;

implementation

{ TDriverAbsoluteDB }

constructor TDriverAbsoluteDB.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TABSDatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  
  if not FileExists(FConnection.DatabaseFileName) then
    FConnection.CreateDatabase;

  FSQLScript := TABSQuery.Create(nil);
  try
    FSQLScript.DatabaseName := FConnection.DatabaseName;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverAbsoluteDB.Destroy;
begin
  FSQLScript.Free;
  inherited;
end;

procedure TDriverAbsoluteDB.Connect;
begin
  if not FConnection.Connected then
    FConnection.Open;
end;

procedure TDriverAbsoluteDB.Disconnect;
begin
  if FConnection.Connected then
    FConnection.Close;
end;

function TDriverAbsoluteDB.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDriverAbsoluteDB.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TABSQuery;
begin
  LExeSQL := TABSQuery.Create(nil);
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

procedure TDriverAbsoluteDB.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LExeSQL: TABSQuery;
  LFor: Integer;
begin
  LExeSQL := TABSQuery.Create(nil);
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

procedure TDriverAbsoluteDB.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverAbsoluteDB.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverAbsoluteDB.ExecuteScripts;
begin
  try
    FSQLScript.ExecSQL;
    FRowsAffected := FSQLScript.RowsAffected;
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

function TDriverAbsoluteDB.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryAbsoluteDB.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverAbsoluteDB.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryAbsoluteDB.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryAbsoluteDB }

constructor TDriverQueryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TABSQuery.Create(nil);
  try
    FSQLQuery.DatabaseName := AConnection.DatabaseName;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryAbsoluteDB.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryAbsoluteDB._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQueryAbsoluteDB._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryAbsoluteDB.ExecuteDirect;
var
  LExeSQL: TABSQuery;
  LFor: Integer;
begin
  LExeSQL := TABSQuery.Create(nil);
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

function TDriverQueryAbsoluteDB.ExecuteQuery: IDBDataSet;
var
  LDataSet: TABSQuery;
  LFor: Integer;
begin
  LDataSet := TABSQuery.Create(nil);
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
        
      LDataSet.Open;
      Result := TDriverDataSetAbsoluteDB.Create(LDataSet, FMonitorCallback);
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

function TDriverQueryAbsoluteDB.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetAbsoluteDB }

constructor TDriverDataSetAbsoluteDB.Create(const ADataSet: TABSQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

end.
