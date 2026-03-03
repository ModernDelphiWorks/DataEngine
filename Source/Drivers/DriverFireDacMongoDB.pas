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

unit DriverFireDacMongoDB;

interface

uses
  Classes,
  SysUtils,
  StrUtils,
  JSON.Types,
  JSON.Readers,
  JSON.BSON,
  JSON.Builders,
  Variants,
  Data.DB,
  // FireDAC
  FireDAC.Stan.Intf, FireDAC.Stan.Option,
  FireDAC.Stan.Error, FireDAC.UI.Intf, FireDAC.Phys.Intf, FireDAC.Stan.Def,
  FireDAC.Stan.Pool, FireDAC.Stan.Async, FireDAC.Phys, FireDAC.Phys.MongoDB,
  FireDAC.Phys.MongoDBDef, FireDAC.Phys.MongoDBWrapper,
  FireDAC.VCLUI.Wait, FireDAC.Stan.Param, FireDAC.DatS, FireDAC.DApt.Intf,
  FireDAC.Phys.MongoDBDataSet, FireDAC.Comp.Client,
  FireDAC.Comp.UI,
  // DBE
  DriverConnection,
  FactoryInterfaces;

type
  TDriverMongoFireDAC = class(TDriverConnection)
  protected
    FConnection: TFDConnection;
    FMongoEnv: TMongoEnv;
    FMongoConnection: TMongoConnection;
    procedure CommandUpdateExecute(const ACommandText: String; const AParams: TParams);
    procedure CommandInsertExecute(const ACommandText: String; const AParams: TParams);
    procedure CommandDeleteExecute(const ACommandText: String; const AParams: TParams);
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
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String): IDBResultSet; override;
  end;

  TDriverQueryMongoFireDAC = class(TDriverQuery)
  private
    FConnection: TFDConnection;
    FFDMongoQuery: TFDMongoQuery;
    FMongoConnection: TMongoConnection;
    FMongoEnv: TMongoEnv;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TFDConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMongoConnection: TMongoConnection;
      const AMongoEnv: TMongoEnv;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
  end;

  TDriverResultSetMongoFireDAC = class(TDriverDataSet<TFDMongoQuery>)
  public
    constructor Create(const ADataSet: TFDMongoQuery; const AMonitorCallback: TMonitorProc); reintroduce;
    destructor Destroy; override;
    function RowsAffected: UInt32; override;
  end;

implementation

uses
  ormbr.utils;

{ TDriverMongoFireDAC }

constructor TDriverMongoFireDAC.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TFDConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  
  if FConnection.CliObj <> nil then
  begin
    FMongoConnection := TMongoConnection(FConnection.CliObj);
    if FMongoConnection <> nil then
      FMongoEnv := FMongoConnection.Env;
  end;
end;

destructor TDriverMongoFireDAC.Destroy;
begin
  FConnection := nil;
  FMongoConnection := nil;
  FMongoEnv := nil;
  inherited;
end;

procedure TDriverMongoFireDAC.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverMongoFireDAC.ExecuteDirect(const ASQL: String);
begin
  ExecuteDirect(ASQL, nil);
end;

procedure TDriverMongoFireDAC.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LCommand: String;
begin
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    // Validar se estamos conectados ao Mongo
    if FMongoConnection = nil then
    begin
       if FConnection.Connected then
         FMongoConnection := TMongoConnection(FConnection.CliObj);
       
       if FMongoConnection = nil then
         raise Exception.Create('MongoDB Connection not initialized.');
       
       FMongoEnv := FMongoConnection.Env;
    end;

    LCommand := TUtilSingleton
                  .GetInstance
                    .ParseCommandNoSQL('command', ASQL);
    
    if LCommand = 'insert' then
      CommandInsertExecute(ASQL, AParams)
    else
    if LCommand = 'update' then
      CommandUpdateExecute(ASQL, AParams)
    else
    if LCommand = 'delete' then
      CommandDeleteExecute(ASQL, AParams)
    else
      raise Exception.Create('Command not supported for MongoDB: ' + LCommand);
      
    _SetMonitorLog(ASQL, '', AParams);
  except
    on E: Exception do
    begin
      _SetMonitorLog('Error executing Mongo command', E.Message, AParams);
      raise;
    end;
  end;
end;

procedure TDriverMongoFireDAC.ExecuteScript(const AScript: String);
begin
  raise Exception.Create('Command [ExecuteScript()] not supported for NoSQL MongoDB database!');
end;

procedure TDriverMongoFireDAC.ExecuteScripts;
begin
  raise Exception.Create('Command [ExecuteScripts()] not supported for NoSQL MongoDB database!');
end;

procedure TDriverMongoFireDAC.AddScript(const AScript: String);
begin
  raise Exception.Create('Command [AddScript()] not supported for NoSQL MongoDB database!');
end;

procedure TDriverMongoFireDAC.CommandDeleteExecute(const ACommandText: String;
  const AParams: TParams);
var
  LMongoSelector: TMongoSelector;
  LUtil: IUtilSingleton;
begin
  LMongoSelector := TMongoSelector.Create(FMongoEnv);
  LUtil := TUtilSingleton.GetInstance;
  try
    LMongoSelector.Match(LUtil.ParseCommandNoSQL('json', ACommandText));
    FMongoConnection[FConnection.Params.Database]
                    [LUtil.ParseCommandNoSQL('collection', ACommandText)]
      .Remove(LMongoSelector);
  finally
    LMongoSelector.Free;
  end;
end;

procedure TDriverMongoFireDAC.CommandInsertExecute(const ACommandText: String;
  const AParams: TParams);
var
  LMongoInsert: TMongoInsert;
  LUtil: IUtilSingleton;
begin
  LMongoInsert := TMongoInsert.Create(FMongoEnv);
  LUtil := TUtilSingleton.GetInstance;
  try
    LMongoInsert
      .Values(LUtil.ParseCommandNoSQL('json', ACommandText));
    FMongoConnection[FConnection.Params.Database]
                    [LUtil.ParseCommandNoSQL('collection', ACommandText)]
      .Insert(LMongoInsert)
  finally
    LMongoInsert.Free;
  end;
end;

procedure TDriverMongoFireDAC.CommandUpdateExecute(const ACommandText: String;
  const AParams: TParams);
var
  LMongoUpdate: TMongoUpdate;
  LUtil: IUtilSingleton;
begin
  LMongoUpdate := TMongoUpdate.Create(FMongoEnv);
  LUtil := TUtilSingleton.GetInstance;
  try
    LMongoUpdate
      .Match(LUtil.ParseCommandNoSQL('filter', ACommandText));
    LMongoUpdate
      .Modify(LUtil.ParseCommandNoSQL('json', ACommandText));
    FMongoConnection[FConnection.Params.Database]
                    [LUtil.ParseCommandNoSQL('collection', ACommandText)]
      .Update(LMongoUpdate);
  finally
    LMongoUpdate.Free;
  end;
end;

procedure TDriverMongoFireDAC.Connect;
begin
  FConnection.Connected := True;
  if FConnection.CliObj <> nil then
  begin
    FMongoConnection := TMongoConnection(FConnection.CliObj);
    FMongoEnv := FMongoConnection.Env;
  end;
end;

function TDriverMongoFireDAC.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverMongoFireDAC.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryMongoFireDAC.Create(FConnection, FDriverTransaction, FMongoConnection, FMongoEnv, FMonitorCallback);
end;

function TDriverMongoFireDAC.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryMongoFireDAC.Create(FConnection, FDriverTransaction, FMongoConnection, FMongoEnv, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryMongoFireDAC }

constructor TDriverQueryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMongoConnection: TMongoConnection;
  const AMongoEnv: TMongoEnv;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise EArgumentNilException.Create('AConnection cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FConnection := AConnection;
  FMongoConnection := AMongoConnection;
  FMongoEnv := AMongoEnv;

  FFDMongoQuery := TFDMongoQuery.Create(nil);
  try
    FFDMongoQuery.Connection := AConnection;
    if AConnection.Params.Database <> '' then
       FFDMongoQuery.DatabaseName := AConnection.Params.Database;
  except
    FFDMongoQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryMongoFireDAC.Destroy;
begin
  FConnection := nil;
  FMongoConnection := nil;
  FMongoEnv := nil;
  FFDMongoQuery.Free;
  inherited;
end;

function TDriverQueryMongoFireDAC.ExecuteQuery: IDBDataSet;
var
  LResultSet: TFDMongoQuery;
  LLimit, LSkip: UInt16;
  LUtil: IUtilSingleton;
begin
  LResultSet := TFDMongoQuery.Create(nil);
  LResultSet.CachedUpdates := True;
  LUtil := TUtilSingleton.GetInstance;
  try
    if not Assigned(FFDMongoQuery.Connection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LResultSet.Connection := FFDMongoQuery.Connection;
    LResultSet.DatabaseName := FFDMongoQuery.Connection.Params.Database;
    
    // Parse QMatch parts
    LResultSet.CollectionName := LUtil.ParseCommandNoSQL('collection', FFDMongoQuery.QMatch);
    LResultSet.QMatch := LUtil.ParseCommandNoSQL('filter', FFDMongoQuery.QMatch);
    LResultSet.QSort := LUtil.ParseCommandNoSQL('sort', FFDMongoQuery.QMatch);
    
    LLimit := StrToIntDef(LUtil.ParseCommandNoSQL('limit', FFDMongoQuery.QMatch), 0);
    LSkip := StrToIntDef(LUtil.ParseCommandNoSQL('skip', FFDMongoQuery.QMatch), 0);
    
    if LLimit > 0 then
      LResultSet.Query.Limit(LLimit);
    if LSkip > 0 then
      LResultSet.Query.Skip(LSkip);
      
    LResultSet.QProject := '{_id:0}';
    
    try
      LResultSet.Open;
      Result := TDriverResultSetMongoFireDAC.Create(LResultSet, FMonitorCallback);
      if LResultSet.Active and (LResultSet.RecordCount = 0) then
        Result.FetchingAll := True;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.QMatch, E.Message, nil);
        FreeAndNil(LResultSet);
        raise;
      end;
    end;
  finally
     if Assigned(LResultSet) and (not Assigned(Result)) then
       LResultSet.Free;
  end;
end;

function TDriverQueryMongoFireDAC._GetCommandText: String;
begin
  Result := FFDMongoQuery.QMatch;
end;

procedure TDriverQueryMongoFireDAC._SetCommandText(const ACommandText: String);
begin
  FFDMongoQuery.QMatch := ACommandText;
end;

procedure TDriverQueryMongoFireDAC.ExecuteDirect;
begin
  raise Exception.Create('Command [ExecuteDirect()] not supported for NoSQL MongoDB database!');
end;

{ TDriverResultSetMongoFireDAC }

constructor TDriverResultSetMongoFireDAC.Create(const ADataSet: TFDMongoQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverResultSetMongoFireDAC.Destroy;
begin
  inherited;
end;

function TDriverResultSetMongoFireDAC.RowsAffected: UInt32;
begin
  Result := 0;
end;

end.
