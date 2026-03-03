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

unit DriverUniDac;

interface

uses
  Classes,
  SysUtils,
  StrUtils,
  Variants,
  DB,
  // UniDAC
  Uni,
  DBAccess,
  UniProvider,
  UniScript,
  // DBE
  DriverConnection,
  FactoryInterfaces;

type
  // Classe de conex�o concreta com UniDAC
  TDriverUniDAC = class(TDriverConnection)
  private
    function _GetTransactionActive: TUniTransaction;
  protected
    FConnection: TUniConnection;
    FSQLScript : TUniScript;
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
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryUniDAC = class(TDriverQuery)
  private
    FSQLQuery: TUniSQL;
    function _GetTransactionActive: TUniTransaction;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TUniConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetUniDAC = class(TDriverDataSet<TUniQuery>)
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const ADataSet: TUniQuery; const AMonitorCallback: TMonitorProc); reintroduce;
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

{ TDriverUniDAC }

constructor TDriverUniDAC.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction;
  const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  FConnection := AConnection as TUniConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript  := TUniScript.Create(nil);
  try
    FSQLScript.Connection := FConnection;
    FSQLScript.SQL.Clear;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverUniDAC.Destroy;
begin
  FDriverTransaction := nil;
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverUniDAC.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverUniDAC.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TUniSQL;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TUniSQL.Create(nil);
  try
    LExeSQL.Connection := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := ASQL;
    
    try
      if not LExeSQL.Prepared then
        LExeSQL.Prepare;
      LExeSQL.Execute;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, LExeSQL.Transaction.Name, LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

procedure TDriverUniDAC.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TUniSQL;
  LFor: Int16;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TUniSQL.Create(nil);
  try
    LExeSQL.Connection := FConnection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := ASQL;
    for LFor := 0 to AParams.Count - 1 do
    begin
      LExeSQL.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
      LExeSQL.ParamByName(AParams[LFor].Name).Value := AParams[LFor].Value;
    end;
    
    try
      if not LExeSQL.Prepared then
        LExeSQL.Prepare;
      LExeSQL.Execute;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, LExeSQL.Params);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, LExeSQL.Transaction.Name, LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

procedure TDriverUniDAC.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverUniDAC.ExecuteScripts;
begin
  if FSQLScript.SQL.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  try
    FSQLScript.Transaction := _GetTransactionActive;
    try
      FSQLScript.Execute;
      FRowsAffected := FSQLScript.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog('Error during script execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(FSQLScript.SQL.Text, FSQLScript.Transaction.Name, nil);
    FSQLScript.SQL.Clear;
  end;
end;

function TDriverUniDAC.GetSQLScripts: String;
begin
  Result := 'Transaction: ' + FSQLScript.Transaction.Name + ' ' +  FSQLScript.SQL.Text;
end;

procedure TDriverUniDAC.AddScript(const AScript: String);
begin
  if Self.GetDriver in [dnInterbase, dnFirebird, dnFirebird3] then
    if FSQLScript.SQL.Count = 0 then
      FSQLScript.SQL.Add('SET AUTOCOMMIT OFF');
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverUniDAC.ApplyUpdates(const ADataSets: array of IDBDataSet);
var
  LDataSet: IDBDataSet;
begin
  for LDataset in AdataSets do
    LDataset.ApplyUpdates;
end;

procedure TDriverUniDAC.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverUniDAC.IsConnected: Boolean;
begin
  Result := FConnection.Connected = True;
end;

function TDriverUniDAC._GetTransactionActive: TUniTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TUniTransaction;
end;

function TDriverUniDAC.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryUniDAC.Create(FConnection,
                                      FDriverTransaction,
                                      FMonitorCallback);
end;

function TDriverUniDAC.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryUniDAC.Create(FConnection,
                                        FDriverTransaction,
                                        FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryUniDAC }

constructor TDriverQueryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TUniSQL.Create(nil);
  try
    FSQLQuery.Connection := AConnection;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryUniDAC.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryUniDAC.ExecuteQuery: IDBDataSet;
var
  LResultSet: TUniQuery;
  LFor : Int16;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LResultSet := TUniQuery.Create(nil);
  try
    LResultSet.Connection := FSQLQuery.Connection;
    LResultSet.Transaction := _GetTransactionActive;
    LResultSet.SQL.Text := FSQLQuery.SQL.Text;
    
    if LResultSet.SQL.Text = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    try
      for LFor := 0 to FSQLQuery.Params.Count - 1 do
      begin
        LResultSet.Params[LFor].DataType := FSQLQuery.Params[LFor].DataType;
        LResultSet.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
      end;
      
      if not LResultSet.Prepared then
        LResultSet.Prepare;
      LResultSet.Open;
      
      Result := TDriverDataSetUniDAC.Create(LResultSet, FMonitorCallback);
      if LResultSet.Active then
      begin
        if LResultSet.RecordCount = 0 then
          Result.FetchingAll := True;
      end;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.SQL.Text, E.Message, LResultSet.Params);
        raise;
      end;
    end;
  finally
    if LResultSet.SQL.Text <> EmptyStr then
      _SetMonitorLog(LResultSet.SQL.Text, LResultSet.Transaction.Name, LResultSet.Params);
  end;
  except
    if Assigned(LResultSet) and (Result = nil) then
      LResultSet.Free;
    raise;
  end;
end;

function TDriverQueryUniDAC.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryUniDAC._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

function TDriverQueryUniDAC._GetTransactionActive: TUniTransaction;
begin
  Result := FDriverTransaction.TransactionActive as TUniTransaction;
end;

procedure TDriverQueryUniDAC._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryUniDAC.ExecuteDirect;
var
  LExeSQL: TUniSQL;
  LFor: Int16;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  if FSQLQuery.SQL.Text = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TUniSQL.Create(nil);
  try
    LExeSQL.Connection := FSQLQuery.Connection;
    LExeSQL.Transaction := _GetTransactionActive;
    LExeSQL.SQL.Text := FSQLQuery.SQL.Text;
    for LFor := 0 to FSQLQuery.Params.Count - 1 do
    begin
      LExeSQL.Params[LFor].DataType := FSQLQuery.Params[LFor].DataType;
      LExeSQL.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
    end;
    
    try
      if not LExeSQL.Prepared then
        LExeSQL.Prepare;
      LExeSQL.Execute;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LExeSQL.SQL.Text, E.Message, LExeSQL.Params);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, LExeSQL.Transaction.Name, LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

{ TDriverDataSetUniDAC }

constructor TDriverDataSetUniDAC.Create(const ADataSet: TUniQuery;
      const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetUniDAC.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetUniDAC.ApplyUpdates;
begin
  FDataSet.ApplyUpdates;
end;

procedure TDriverDataSetUniDAC.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

function TDriverDataSetUniDAC.IsCachedUpdates: Boolean;
begin
  Result := FDataSet.CachedUpdates;
end;

function TDriverDataSetUniDAC.IsReadOnly: Boolean;
begin
  Result := FDataSet.ReadOnly;
end;

function TDriverDataSetUniDAC.IsUniDirectional: Boolean;
begin
  Result := FDataSet.UniDirectional;
end;

procedure TDriverDataSetUniDAC.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SQL.Text, FDataSet.Transaction.Name, FDataSet.Params);
  end;
end;

function TDriverDataSetUniDAC.RowsAffected: UInt32;
begin
  Result := FDataSet.RowsAffected;
end;

function TDriverDataSetUniDAC._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetUniDAC._SetCachedUpdates(const Value: Boolean);
begin
  FDataSet.CachedUpdates := Value;
end;

procedure TDriverDataSetUniDAC._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

procedure TDriverDataSetUniDAC._SetReadOnly(const Value: Boolean);
begin
  FDataSet.ReadOnly := Value;
end;

procedure TDriverDataSetUniDAC._SetUniDirectional(const Value: Boolean);
begin
  FDataSet.UniDirectional := Value;
end;

end.
