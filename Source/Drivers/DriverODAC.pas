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

unit DriverODAC;

interface

uses
  Classes,
  SysUtils,
  DB,
  Variants,
  Ora,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverODAC = class(TDriverConnection)
  protected
    FConnection: TOraSession;
    FSQLScript: TOraQuery;
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

  TDriverQueryODAC = class(TDriverQuery)
  private
    FSQLQuery: TOraQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TOraSession;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetODAC = class(TDriverDataSet<TOraQuery>)
  public
    constructor Create(const ADataSet: TOraQuery; const AMonitorCallback: TMonitorProc); reintroduce;
    destructor Destroy; override;
    procedure Open; override;
    function RowsAffected: UInt32; override;
  end;

implementation

{ TDriverODAC }

constructor TDriverODAC.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TOraSession;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript := TOraQuery.Create(nil);
  try
    FSQLScript.Session := FConnection;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverODAC.Destroy;
begin
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverODAC.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverODAC.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TOraQuery;
  LParams: TParams;
begin
  LExeSQL := TOraQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Session := FConnection;
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
      _SetMonitorLog(LExeSQL.SQL.Text, '', LParams);
      LExeSQL.Free;
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

procedure TDriverODAC.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TOraQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LExeSQL := TOraQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Session := FConnection;
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
      _SetMonitorLog(LExeSQL.SQL.Text, '', LParams);
      LExeSQL.Free;
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

procedure TDriverODAC.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverODAC.ExecuteScripts;
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

procedure TDriverODAC.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverODAC.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverODAC.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverODAC.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryODAC.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverODAC.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryODAC.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryODAC }

constructor TDriverQueryODAC.Create(const AConnection: TOraSession;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise EArgumentNilException.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TOraQuery.Create(nil);
  try
    FSQLQuery.Session := AConnection;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryODAC.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryODAC.ExecuteQuery: IDBDataSet;
var
  LDataSet: TOraQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LDataSet := TOraQuery.Create(nil);
  LParams := nil; 
  try
    if not Assigned(FSQLQuery.Session) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    LDataSet.Session := FSQLQuery.Session;
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
      Result := TDriverDataSetODAC.Create(LDataSet, FMonitorCallback);
      if LDataSet.Active then
      begin
         if LDataSet.RecordCount = 0 then
           Result.FetchingAll := True;
      end;
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

function TDriverQueryODAC.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryODAC._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQueryODAC._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryODAC.ExecuteDirect;
var
  LExeSQL: TOraQuery;
  LFor: Int16;
begin
  LExeSQL := TOraQuery.Create(nil);
  try
    if not Assigned(FSQLQuery.Session) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Session := FSQLQuery.Session;
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

{ TDriverDataSetODAC }

constructor TDriverDataSetODAC.Create(const ADataSet: TOraQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetODAC.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetODAC.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, '', nil);
  end;
end;

function TDriverDataSetODAC.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

end.
