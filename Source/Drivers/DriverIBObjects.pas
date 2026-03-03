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

unit DriverIBObjects;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  IB_Components,
  IBODataset,
  IB_Access,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverIBObjects = class(TDriverConnection)
  protected
    FConnection: TIBODatabase;
    FSQLScript: TIBOQuery;
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

  TDriverQueryIBObjects = class(TDriverQuery)
  private
    FSQLQuery: TIBOQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TIBODatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetIBObjects = class(TDriverDataSet<TIBOQuery>)
  public
    constructor Create(const ADataSet: TIBOQuery; const AMonitorCallback: TMonitorProc); reintroduce;
  end;

implementation

{ TDriverIBObjects }

constructor TDriverIBObjects.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TIBODatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  
  FSQLScript := TIBOQuery.Create(nil);
  try
    FSQLScript.IB_Connection := FConnection;
    FSQLScript.IB_Transaction := FConnection.DefaultTransaction;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverIBObjects.Destroy;
begin
  FSQLScript.Free;
  inherited;
end;

procedure TDriverIBObjects.Connect;
begin
  if not FConnection.Connected then
    FConnection.Connected := True;
end;

procedure TDriverIBObjects.Disconnect;
begin
  if FConnection.Connected then
    FConnection.Connected := False;
end;

function TDriverIBObjects.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDriverIBObjects.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TIBOQuery;
begin
  LExeSQL := TIBOQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.IB_Connection := FConnection;
    LExeSQL.IB_Transaction := FConnection.DefaultTransaction;
    
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

procedure TDriverIBObjects.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LExeSQL: TIBOQuery;
  LFor: Integer;
begin
  LExeSQL := TIBOQuery.Create(nil);
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.IB_Connection := FConnection;
    LExeSQL.IB_Transaction := FConnection.DefaultTransaction;
    
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

procedure TDriverIBObjects.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverIBObjects.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverIBObjects.ExecuteScripts;
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

function TDriverIBObjects.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryIBObjects.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverIBObjects.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryIBObjects.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryIBObjects }

constructor TDriverQueryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TIBOQuery.Create(nil);
  try
    FSQLQuery.IB_Connection := AConnection;
    FSQLQuery.IB_Transaction := AConnection.DefaultTransaction;
    FSQLQuery.UniDirectional := True;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryIBObjects.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryIBObjects._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQueryIBObjects._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryIBObjects.ExecuteDirect;
var
  LExeSQL: TIBOQuery;
  LFor: Integer;
begin
  LExeSQL := TIBOQuery.Create(nil);
  try
    if FSQLQuery.IB_Connection = nil then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.IB_Connection := FSQLQuery.IB_Connection;
    LExeSQL.IB_Transaction := FSQLQuery.IB_Transaction;
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

function TDriverQueryIBObjects.ExecuteQuery: IDBDataSet;
var
  LDataSet: TIBOQuery;
  LFor: Integer;
begin
  LDataSet := TIBOQuery.Create(nil);
  try
    if FSQLQuery.IB_Connection = nil then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    LDataSet.IB_Connection := FSQLQuery.IB_Connection;
    LDataSet.IB_Transaction := FSQLQuery.IB_Transaction;
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
      Result := TDriverDataSetIBObjects.Create(LDataSet, FMonitorCallback);
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

function TDriverQueryIBObjects.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverDataSetIBObjects }

constructor TDriverDataSetIBObjects.Create(const ADataSet: TIBOQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

end.
