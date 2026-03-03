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

unit DriverZeos;

{$ifdef fpc}
  {$mode delphi}{$H+}
{$endif}

interface

uses
  Classes,
  SysUtils,
  DB,
  Variants,
  ZAbstractConnection,
  ZConnection,
  ZAbstractRODataset,
  ZAbstractDataset,
  ZDataset,
  ZSqlProcessor,
  DriverConnection,
  DriverZeosTransaction,
  FactoryInterfaces;

type
  TZQueryHelper = class Helper for TZQuery
  public
    function AsParams: TParams;
  end;

  TDriverZeos = class(TDriverConnection)
  private
    {$IFDEF ZEOS80UP}
    function _GetTransactionActive: TZTransaction;
    {$ENDIF}
  protected
    FConnection: TZConnection;
    FSQLScript: TZSQLProcessor;
  public
    constructor Create(const AConnection: TComponent;
      const ADriverTransaction: TDriverTransaction;
      const ADriver: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure ExecuteDirect(const ASQL: String); override;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryZeos = class(TDriverQuery)
  private
    FSQLQuery: TZQuery;
    {$IFDEF ZEOS80UP}
    function _GetTransactionActive: TZTransaction;
    {$ENDIF}
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TZConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetZeos = class(TDriverDataSet<TZQuery>)
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const ADataSet: TZQuery; const AMonitorCallback: TMonitorProc); reintroduce;
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

{ TZQueryHelper }

function TZQueryHelper.AsParams: TParams;
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

{ TDriverZeos }

constructor TDriverZeos.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction;
  const ADriver: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  FConnection := AConnection as TZConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriver;
  FMonitorCallback := AMonitorCallback;
  FSQLScript := TZSQLProcessor.Create(nil);
  try
    FSQLScript.Connection := FConnection;
    FSQLScript.Script.Clear;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverZeos.Destroy;
begin
  FDriverTransaction := nil;
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverZeos.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverZeos.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TZQuery;
  LParams: TParams;
begin
  LExeSQL := TZQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    {$IFDEF ZEOS80UP}
    if _GetTransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
    {$ENDIF}
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.Connection := FConnection;
    {$IFDEF ZEOS80UP}
    LExeSQL.Transaction := _GetTransactionActive;
    {$ENDIF}
    LExeSQL.SQL.Text := ASQL;
    
    try
      if not LExeSQL.Prepared then
        LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      LParams := LExeSQL.AsParams;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
    end;
  finally
    if Assigned(LExeSQL) then
    begin
      if LExeSQL.Active then
        LExeSQL.Close;
      {$IFDEF ZEOS80UP}
      _SetMonitorLog(LExeSQL.SQL.Text, LExeSQL.Transaction.Name, LParams);
      {$ELSE}
      _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LParams);
      {$ENDIF}
      LExeSQL.Free;
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

procedure TDriverZeos.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TZQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LExeSQL := TZQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    {$IFDEF ZEOS80UP}
    if _GetTransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
    {$ENDIF}
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.Connection := FConnection;
    {$IFDEF ZEOS80UP}
    LExeSQL.Transaction := _GetTransactionActive;
    {$ENDIF}
    LExeSQL.SQL.Text := ASQL;
    if AParams.Count > 0 then
    begin
      for LFor := 0 to AParams.Count - 1 do
      begin
        LExeSQL.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
        LExeSQL.ParamByName(AParams[LFor].Name).Value := AParams[LFor].Value;
      end;
    end;
    
    try
      if not LExeSQL.Prepared then
        LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      LParams := LExeSQL.AsParams;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
    end;
  finally
    if Assigned(LExeSQL) then
    begin
      if LExeSQL.Active then
        LExeSQL.Close;
      {$IFDEF ZEOS80UP}
      _SetMonitorLog(LExeSQL.SQL.Text, LExeSQL.Transaction.Name, LParams);
      {$ELSE}
      _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LParams);
      {$ENDIF}
      LExeSQL.Free;
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

procedure TDriverZeos.ExecuteScript(const AScript: String);
begin
  FSQLScript.Script.Text := AScript;
  ExecuteScripts;
end;

procedure TDriverZeos.ExecuteScripts;
begin
  if FSQLScript.Script.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  {$IFDEF ZEOS80UP}
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  {$ENDIF}

  try
    {$IFDEF ZEOS80UP}
    FSQLScript.Transaction := _GetTransactionActive;
    {$ENDIF}
    
    try
      FSQLScript.Execute;
    except
      on E: Exception do
      begin
        _SetMonitorLog('Error during script execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    {$IFDEF ZEOS80UP}
    _SetMonitorLog(FSQLScript.Script.Text, FSQLScript.Transaction.Name, nil);
    {$ELSE}
    _SetMonitorLog(FSQLScript.Script.Text, 'DEFAULT', nil);
    {$ENDIF}
    FRowsAffected := 0;
    FSQLScript.Script.Clear;
  end;
end;

function TDriverZeos.GetSQLScripts: String;
begin
  {$IFDEF ZEOS80UP}
  Result := 'Transaction: ' + FSQLScript.Transaction.Name + ' ' +  FSQLScript.Script.Text;
  {$ELSE}
  Result := 'Transaction: ' + 'DEFAULT' + ' ' +  FSQLScript.Script.Text;
  {$ENDIF}
end;

procedure TDriverZeos.AddScript(const AScript: String);
begin
  if Self.GetDriver in [dnInterbase, dnFirebird, dnFirebird3] then
    if FSQLScript.Script.Count = 0 then
      FSQLScript.Script.Add('SET AUTOCOMMIT OFF');
  FSQLScript.Script.Add(AScript);
end;

procedure TDriverZeos.ApplyUpdates(const ADataSets: array of IDBDataSet);
var
  LDataSet: IDBDataSet;
begin
  for LDataset in AdataSets do
    LDataset.ApplyUpdates;
end;

procedure TDriverZeos.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverZeos.IsConnected: Boolean;
begin
  Result := FConnection.Connected = True;
end;

{$IFDEF ZEOS80UP}
function TDriverZeos._GetTransactionActive: TZTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TZTransaction;
end;
{$ENDIF}

function TDriverZeos.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryZeos.Create(FConnection,
                                    FDriverTransaction,
                                    FMonitorCallback);
end;

function TDriverZeos.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryZeos.Create(FConnection,
                                      FDriverTransaction,
                                      FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryZeos }

constructor TDriverQueryZeos.Create(const AConnection: TZConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TZQuery.Create(nil);
  try
    FSQLQuery.Connection := AConnection;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryZeos.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryZeos.ExecuteQuery: IDBDataSet;
var
  LResultSet: TZQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LResultSet := TZQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FSQLQuery.Connection) then
      raise Exception.Create('Connection not assigned.');
    {$IFDEF ZEOS80UP}
    if _GetTransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
    {$ENDIF}

    LResultSet.Connection := FSQLQuery.Connection;
    {$IFDEF ZEOS80UP}
    LResultSet.Transaction := _GetTransactionActive;
    {$ENDIF}
    LResultSet.SQL.Text := FSQLQuery.SQL.Text;
    
    if LResultSet.SQL.Text = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    try
      if FSQLQuery.Params.Count > 0 then
      begin
        for LFor := 0 to FSQLQuery.Params.Count - 1 do
        begin
          LResultSet.ParamByName(FSQLQuery.Params[LFor].Name).DataType := FSQLQuery.Params[LFor].DataType;
          LResultSet.ParamByName(FSQLQuery.Params[LFor].Name).Value := FSQLQuery.Params[LFor].Value;
        end;
      end;
      
      if not LResultSet.Prepared then
        LResultSet.Prepare;
      LResultSet.Open;
      
      Result := TDriverDataSetZeos.Create(LResultSet, FMonitorCallback);
      if LResultSet.Active then
      begin
        if LResultSet.RecordCount = 0 then
          Result.FetchingAll := True;
      end;
      LParams := LResultSet.AsParams;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.SQL.Text, E.Message, LParams);
        FreeAndNil(LResultSet);
        raise;
      end;
    end;
  finally
    if Assigned(LResultSet) then
    begin
      if LResultSet.Active then
        LResultSet.Close;
      if LResultSet.SQL.Text <> '' then
        {$IFDEF ZEOS80UP}
        _SetMonitorLog(LResultSet.SQL.Text, LResultSet.Transaction.Name, LParams);
        {$ELSE}
        _SetMonitorLog(LResultSet.SQL.Text, 'DEFAULT', LParams);
        {$ENDIF}
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

function TDriverQueryZeos.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryZeos._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

{$IFDEF ZEOS80UP}
function TDriverQueryZeos._GetTransactionActive: TZTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TZTransaction;
end;
{$ENDIF}

procedure TDriverQueryZeos._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryZeos.ExecuteDirect;
var
  LExeSQL: TZQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LExeSQL := TZQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FSQLQuery.Connection) then
      raise Exception.Create('Connection not assigned.');
    {$IFDEF ZEOS80UP}
    if _GetTransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
    {$ENDIF}
    
    if FSQLQuery.SQL.Text = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.Connection := FSQLQuery.Connection;
    {$IFDEF ZEOS80UP}
    LExeSQL.Transaction := _GetTransactionActive;
    {$ENDIF}
    LExeSQL.SQL.Text := FSQLQuery.SQL.Text;
    
    if FSQLQuery.Params.Count > 0 then
    begin
      for LFor := 0 to FSQLQuery.Params.Count - 1 do
      begin
        LExeSQL.ParamByName(FSQLQuery.Params[LFor].Name).DataType := FSQLQuery.Params[LFor].DataType;
        LExeSQL.ParamByName(FSQLQuery.Params[LFor].Name).Value := FSQLQuery.Params[LFor].Value;
      end;
    end;
    
    try
      if not LExeSQL.Prepared then
        LExeSQL.Prepare;
      LExeSQL.ExecSQL;
      LParams := LExeSQL.AsParams;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LExeSQL.SQL.Text, E.Message, nil);
        raise;
      end;
    end;
  finally
    if Assigned(LExeSQL) then
    begin
      if LExeSQL.Active then
        LExeSQL.Close;
      {$IFDEF ZEOS80UP}
      _SetMonitorLog(LExeSQL.SQL.Text, LExeSQL.Transaction.Name, LParams);
      {$ELSE}
      _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LParams);
      {$ENDIF}
      LExeSQL.Free;
    end;
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

{ TDriverDataSetZeos }

procedure TDriverDataSetZeos.ApplyUpdates;
begin
  FDataSet.ApplyUpdates;
end;

procedure TDriverDataSetZeos.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

constructor TDriverDataSetZeos.Create(const ADataSet: TZQuery;
      const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetZeos.Destroy;
begin
  inherited;
end;

function TDriverDataSetZeos.IsCachedUpdates: Boolean;
begin
  Result := FDataSet.CachedUpdates;
end;

function TDriverDataSetZeos.IsReadOnly: Boolean;
begin
  Result := FDataSet.ReadOnly;
end;

function TDriverDataSetZeos.IsUniDirectional: Boolean;
begin
  Result := FDataSet.IsUniDirectional;
end;

procedure TDriverDataSetZeos.Open;
var
  LParams: TParams;
begin
  LParams := nil;
  try
    inherited Open;
    LParams := FDataSet.AsParams;
  finally
    {$IFDEF ZEOS80UP}
    _SetMonitorLog(FDataSet.SQL.Text, FDataSet.Transaction.Name, LParams);
    {$ELSE}
    _SetMonitorLog(FDataSet.SQL.Text, 'DEFAULT', LParams);
    {$ENDIF}
    if Assigned(LParams) then
    begin
      LParams.Clear;
      LParams.Free;
    end;
  end;
end;

function TDriverDataSetZeos.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

function TDriverDataSetZeos._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetZeos._SetCachedUpdates(const Value: Boolean);
begin
  FDataSet.CachedUpdates := Value;
end;

procedure TDriverDataSetZeos._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

procedure TDriverDataSetZeos._SetReadOnly(const Value: Boolean);
begin
  FDataSet.ReadOnly := Value;
end;

procedure TDriverDataSetZeos._SetUniDirectional(const Value: Boolean);
begin
end;

end.
