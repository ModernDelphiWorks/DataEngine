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

unit DriverFIBPlus;

interface

uses
  Classes,
  SysUtils,
  DB,
  Variants,
  FIBQuery,
  FIBDataSet,
  FIBDatabase,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverFIBPlus = class(TDriverConnection)
  protected
    FConnection: TFIBDatabase;
    FSQLScript: TFIBQuery;
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

  TDriverQueryFIBPlus = class(TDriverQuery)
  private
    FSQLQuery: TFIBQuery;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetFIBPlus = class(TDriverDataSet<TFIBDataSet>)
  public
    constructor Create(const ADataSet: TFIBDataSet; const AMonitorCallback: TMonitorProc); reintroduce;
    destructor Destroy; override;
    procedure Open; override;
    function RowsAffected: UInt32; override;
  end;

implementation

{ TDriverFIBPlus }

constructor TDriverFIBPlus.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TFIBDatabase;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript := TFIBQuery.Create(nil);
  try
    FSQLScript.Database := FConnection;
    // Transaction will be assigned on execution based on active transaction
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverFIBPlus.Destroy;
begin
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverFIBPlus.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverFIBPlus.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TFIBQuery;
  LParams: TParams;
begin
  LExeSQL := TFIBQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Database := FConnection;
    LExeSQL.Transaction := FDriverTransaction.TransactionActive as TFIBTransaction;
    
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    try
      LExeSQL.ExecQuery;
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

procedure TDriverFIBPlus.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TFIBQuery;
  LParams: TParams;
  LFor: Int16;
begin
  LExeSQL := TFIBQuery.Create(nil);
  LParams := nil;
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Database := FConnection;
    LExeSQL.Transaction := FDriverTransaction.TransactionActive as TFIBTransaction;
    
    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL.SQL.Text := ASQL;
    if AParams.Count > 0 then
    begin
      for LFor := 0 to AParams.Count - 1 do
      begin
        if not Assigned(AParams[LFor]) then
          raise Exception.Create(Format('Parameter "%s" is invalid or unassigned.', [AParams[LFor].Name]));
          
        LExeSQL.Params.ParamByName(AParams[LFor].Name).Value := AParams[LFor].Value;
      end;
    end;
    
    try
      LExeSQL.ExecQuery;
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

procedure TDriverFIBPlus.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverFIBPlus.ExecuteScripts;
begin
  try
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    FSQLScript.Transaction := FDriverTransaction.TransactionActive as TFIBTransaction;
    FSQLScript.ExecQuery;
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

procedure TDriverFIBPlus.AddScript(const AScript: String);
begin
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverFIBPlus.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverFIBPlus.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverFIBPlus.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryFIBPlus.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverFIBPlus.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryFIBPlus.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryFIBPlus }

constructor TDriverQueryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise EArgumentNilException.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TFIBQuery.Create(nil);
  try
    FSQLQuery.Database := AConnection;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryFIBPlus.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryFIBPlus.ExecuteQuery: IDBDataSet;
var
  LDataSet: TFIBDataSet;
  LParams: TParams;
  LFor: Int16;
begin
  LDataSet := TFIBDataSet.Create(nil);
  LParams := nil; 
  try
    if not Assigned(FSQLQuery.Database) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');
      
    LDataSet.Database := FSQLQuery.Database;
    LDataSet.Transaction := FDriverTransaction.TransactionActive as TFIBTransaction;
    LDataSet.SelectSQL.Text := FSQLQuery.SQL.Text;
    
    try
      if FSQLQuery.Params.Count > 0 then
      begin
        for LFor := 0 to FSQLQuery.Params.Count - 1 do
        begin
           LDataSet.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
        end;
      end;
      
      if LDataSet.SelectSQL.Text = '' then
        raise Exception.Create('SQL statement is empty. Cannot execute the query.');
        
      if not LDataSet.Database.Connected then
        LDataSet.Database.Open;
        
      LDataSet.Open;
      Result := TDriverDataSetFIBPlus.Create(LDataSet, FMonitorCallback);
      if LDataSet.Active then
      begin
         if LDataSet.RecordCount = 0 then
           Result.FetchingAll := True;
      end;
    except
      on E: EDatabaseError do
      begin
        _SetMonitorLog(LDataSet.SelectSQL.Text, E.Message, nil);
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

function TDriverQueryFIBPlus.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryFIBPlus._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

procedure TDriverQueryFIBPlus._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryFIBPlus.ExecuteDirect;
var
  LExeSQL: TFIBQuery;
  LFor: Int16;
begin
  LExeSQL := TFIBQuery.Create(nil);
  try
    if not Assigned(FSQLQuery.Database) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LExeSQL.Database := FSQLQuery.Database;
    LExeSQL.Transaction := FDriverTransaction.TransactionActive as TFIBTransaction;
    LExeSQL.SQL.Text := FSQLQuery.SQL.Text;
    
    if FSQLQuery.Params.Count > 0 then
    begin
      for LFor := 0 to FSQLQuery.Params.Count - 1 do
      begin
        LExeSQL.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
      end;
    end;
    
    try
      LExeSQL.ExecQuery;
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

{ TDriverDataSetFIBPlus }

constructor TDriverDataSetFIBPlus.Create(const ADataSet: TFIBDataSet;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetFIBPlus.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetFIBPlus.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.SelectSQL.Text, '', nil);
  end;
end;

function TDriverDataSetFIBPlus.RowsAffected: UInt32;
begin
  Result := 0; // FIBDataSet does not easily expose rows affected for select
end;

end.
