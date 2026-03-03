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

unit DriverADO;

interface

uses
  Classes,
  SysUtils,
  DB,
  Variants,
  ADODB,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverADO = class(TDriverConnection)
  protected
    FConnection: TADOConnection;
    FSQLScript: TADOQuery;
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
    function CreateDataSet(const ASQL: String): IDBDataSet; override;
  end;

  TDriverQueryADO = class(TDriverQuery)
  private
    FADOQuery: TADOQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TADOConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetADO = class(TDriverDataSet<TADOQuery>)
  public
    constructor Create(const ADataSet: TADOQuery; const AMonitorCallback: TMonitorProc); reintroduce;
    destructor Destroy; override;
    procedure Open; override;
    function RowsAffected: UInt32; override;
  end;

implementation

{ TDriverADO }

destructor TDriverADO.Destroy;
begin
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverADO.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverADO.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TADOQuery;
  LParams: TParams;
begin
  LExeSQL := TADOQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
      
    LExeSQL.Connection := FConnection;
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

procedure TDriverADO.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TADOQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LExeSQL := TADOQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');

    LExeSQL.Connection := FConnection;
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    if AParams.Count > 0 then
    begin
      for LFor := 0 to AParams.Count - 1 do
      begin
        if not Assigned(AParams[LFor]) then
          raise Exception.Create(Format('Parameter "%s" is invalid or unassigned.', [AParams[LFor].Name]));
          
        LExeSQL.Parameters.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
        LExeSQL.Parameters.ParamByName(AParams[LFor].Name).Value    := AParams[LFor].Value;
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

procedure TDriverADO.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverADO.ExecuteScripts;
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

procedure TDriverADO.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverADO.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverADO.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

constructor TDriverADO.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  FConnection := AConnection as TADOConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript := TADOQuery.Create(nil);
  try
    FSQLScript.Connection := FConnection;
  except
    FSQLScript.Free;
    raise;
  end;
end;

function TDriverADO.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryADO.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverADO.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryADO.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryADO }

constructor TDriverQueryADO.Create(const AConnection: TADOConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise EArgumentNilException.Create('AConnection cannot be nil');
  
  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FADOQuery := TADOQuery.Create(nil);
  try
    FADOQuery.Connection := AConnection;
  except
    FADOQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryADO.Destroy;
begin
  FADOQuery.Free;
  inherited;
end;

function TDriverQueryADO.ExecuteQuery: IDBDataSet;
var
  LDataSet: TADOQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LDataSet := TADOQuery.Create(nil);
  LParams := nil; // TADOQuery doesn't easily convert to TParams without helper, leaving nil for now or implementing conversion if needed
  try
    if not Assigned(FADOQuery.Connection) then
      raise Exception.Create('Connection not assigned.');
      
    LDataSet.Connection := FADOQuery.Connection;
    LDataSet.SQL.Text := FADOQuery.SQL.Text;
    
    try
      if FADOQuery.Parameters.Count > 0 then
      begin
        for LFor := 0 to FADOQuery.Parameters.Count - 1 do
        begin
           LDataSet.Parameters[LFor].DataType := FADOQuery.Parameters[LFor].DataType;
           LDataSet.Parameters[LFor].Value    := FADOQuery.Parameters[LFor].Value;
        end;
      end;
      
      if LDataSet.SQL.Text = '' then
        raise Exception.Create('SQL statement is empty. Cannot execute the query.');
        
      LDataSet.Open;
      Result := TDriverDataSetADO.Create(LDataSet, FMonitorCallback);
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
    if Assigned(LDataSet) and (not Assigned(Result)) then // Only free if we didn't pass ownership to Result
       LDataSet.Free; 
       
    // Note: If Result IS created, it owns LDataSet, so we don't free it here.
    // However, if we want to log, we should do it before.
    // The FireDAC implementation closes LDataSet in finally if it was local, but here LDataSet is passed to TDriverDataSetADO.
    // TDriverDataSetFireDAC takes ownership? TDriverDataSet<T> takes ownership in constructor.
    // So if Result is created, LDataSet is owned by it.
  end;
end;

function TDriverQueryADO.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryADO._GetCommandText: String;
begin
  Result := FADOQuery.SQL.Text;
end;

procedure TDriverQueryADO._SetCommandText(const ACommandText: String);
begin
  FADOQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryADO.ExecuteDirect;
var
  LExeSQL: TADOQuery;
  LFor: Int16;
begin
  LExeSQL := TADOQuery.Create(nil);
  try
    if not Assigned(FADOQuery.Connection) then
      raise Exception.Create('Connection not assigned.');

    LExeSQL.Connection := FADOQuery.Connection;
    LExeSQL.SQL.Text := FADOQuery.SQL.Text;
    
    if FADOQuery.Parameters.Count > 0 then
    begin
      for LFor := 0 to FADOQuery.Parameters.Count - 1 do
      begin
        LExeSQL.Parameters[LFor].DataType := FADOQuery.Parameters[LFor].DataType;
        LExeSQL.Parameters[LFor].Value    := FADOQuery.Parameters[LFor].Value;
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

{ TDriverDataSetADO }

constructor TDriverDataSetADO.Create(const ADataSet: TADOQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetADO.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetADO.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, '', nil);
  end;
end;

function TDriverDataSetADO.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

end.
