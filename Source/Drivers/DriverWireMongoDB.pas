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

unit DriverWireMongoDB;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  System.StrUtils,
  System.Math,
  Data.DB,
  Datasnap.DBClient,
  MongoWire,
  bsonTools,
  JsonDoc,
  MongoWireConnection,
  DriverConnection,
  FactoryInterfaces;

type
  TMongoDBQuery = class(TCustomClientDataSet)
  private
    FConnection: TMongoWireConnection;
    FCollection: String;
    procedure SetConnection(AConnection: TMongoWireConnection);
    function GetSequence(AMongoCampo: String): Int64;
  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;
    procedure Find(ACommandText: String);
    property Collection: String read FCollection write FCollection;
  end;

  TDriverWireMongoDB = class(TDriverConnection)
  protected
    FConnection: TMongoWireConnection;
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
    procedure ExecuteDirect(const ASQL: String;
      const AParams: TParams); override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String): IDBResultSet; override;
  end;

  TDriverQueryWireMongoDB = class(TDriverQuery)
  private
    FConnection: TMongoWireConnection;
    FCommandText: String;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TMongoWireConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverResultSetWireMongoDB = class(TDriverDataSet<TMongoDBQuery>)
  public
    constructor Create(const ADataSet: TMongoDBQuery; const AMonitorCallback: TMonitorProc); reintroduce;
  end;

implementation

uses
  ormbr.utils,
  ormbr.bind,
  ormbr.mapping.explorer,
  ormbr.rest.json,
  ormbr.mapping.rttiutils,
  ormbr.objects.helper;

{ TDriverWireMongoDB }

constructor TDriverWireMongoDB.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TMongoWireConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
end;

destructor TDriverWireMongoDB.Destroy;
begin
  FConnection := nil;
  inherited;
end;

procedure TDriverWireMongoDB.Connect;
begin
  FConnection.Connected := True;
end;

procedure TDriverWireMongoDB.Disconnect;
begin
  FConnection.Connected := False;
end;

function TDriverWireMongoDB.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDriverWireMongoDB.ExecuteDirect(const ASQL: String);
begin
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    FConnection.RunCommand(ASQL);
    _SetMonitorLog(ASQL, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog(ASQL, E.Message, nil);
      raise Exception.Create(E.Message);
    end;
  end;
end;

procedure TDriverWireMongoDB.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LCommand: String;
begin
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    LCommand := TUtilSingleton
                  .GetInstance
                    .ParseCommandNoSQL('command', ASQL);
                    
    if LCommand = 'insert' then
      CommandInsertExecute(ASQL, AParams)
    else if LCommand = 'update' then
      CommandUpdateExecute(ASQL, AParams)
    else if LCommand = 'delete' then
      CommandDeleteExecute(ASQL, AParams)
    else
      FConnection.RunCommand(ASQL); // Fallback for other commands
      
    _SetMonitorLog(ASQL, '', AParams);
  except
    on E: Exception do
    begin
      _SetMonitorLog(ASQL, E.Message, nil);
      raise Exception.Create(E.Message);
    end;
  end;
end;

procedure TDriverWireMongoDB.CommandInsertExecute(const ACommandText: String;
  const AParams: TParams);
var
  LDoc: IJSONDocument;
  LQuery: String;
  LCollection: String;
  LUtil: IUtilSingleton;
begin
  LUtil := TUtilSingleton.GetInstance;
  LCollection := LUtil.ParseCommandNoSQL('collection', ACommandText);
  LQuery := LUtil.ParseCommandNoSQL('json', ACommandText);
  LDoc := JSON(LQuery);
  try
    FConnection
      .MongoWire
        .Insert(LCollection, LDoc);
  except
    raise EMongoException.Create('MongoWire: could not insert Document');
  end;
end;

procedure TDriverWireMongoDB.CommandUpdateExecute(const ACommandText: String;
  const AParams: TParams);
var
  LDocQuery: IJSONDocument;
  LDocFilter: IJSONDocument;
  LFilter: String;
  LQuery: String;
  LCollection: String;
  LUtil: IUtilSingleton;
begin
  LUtil := TUtilSingleton.GetInstance;
  LCollection := LUtil.ParseCommandNoSQL('collection', ACommandText);
  LFilter := LUtil.ParseCommandNoSQL('filter', ACommandText);
  LQuery := LUtil.ParseCommandNoSQL('json', ACommandText);
  LDocQuery := JSON(LQuery);
  LDocFilter := JSON(LFilter);
  try
    FConnection
      .MongoWire
        .Update(LCollection, LDocFilter, LDocQuery);
  except
    raise EMongoException.Create('MongoWire: could not update Document');
  end;
end;

procedure TDriverWireMongoDB.CommandDeleteExecute(const ACommandText: String;
  const AParams: TParams);
var
  LDoc: IJSONDocument;
  LQuery: String;
  LCollection: String;
  LUtil: IUtilSingleton;
begin
  LUtil := TUtilSingleton.GetInstance;
  LCollection := LUtil.ParseCommandNoSQL('collection', ACommandText);
  LQuery := LUtil.ParseCommandNoSQL('json', ACommandText);
  LDoc := JSON(LQuery);
  try
    FConnection
      .MongoWire
        .Delete(LCollection, LDoc);
  except
    raise EMongoException.Create('MongoWire: could not delete Document');
  end;
end;

procedure TDriverWireMongoDB.ExecuteScript(const AScript: String);
begin
  try
    FConnection.RunCommand(AScript);
    _SetMonitorLog(AScript, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog(AScript, E.Message, nil);
      raise Exception.Create(E.Message);
    end;
  end;
end;

procedure TDriverWireMongoDB.AddScript(const AScript: String);
begin
  // Not implemented in original, keeping consistent
end;

procedure TDriverWireMongoDB.ExecuteScripts;
begin
  // Not implemented in original, keeping consistent
end;

function TDriverWireMongoDB.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryWireMongoDB.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverWireMongoDB.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryWireMongoDB.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryWireMongoDB }

constructor TDriverQueryWireMongoDB.Create(const AConnection: TMongoWireConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FConnection := AConnection;
  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
end;

destructor TDriverQueryWireMongoDB.Destroy;
begin
  inherited;
end;

function TDriverQueryWireMongoDB._GetCommandText: String;
begin
  Result := FCommandText;
end;

procedure TDriverQueryWireMongoDB._SetCommandText(const ACommandText: String);
begin
  FCommandText := ACommandText;
end;

procedure TDriverQueryWireMongoDB.ExecuteDirect;
begin
  try
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    FConnection.RunCommand(FCommandText);
    _SetMonitorLog(FCommandText, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog(FCommandText, E.Message, nil);
      raise Exception.Create(E.Message);
    end;
  end;
end;

function TDriverQueryWireMongoDB.ExecuteQuery: IDBResultSet;
var
  LUtil: IUtilSingleton;
  LResultSet: TMongoDBQuery;
  LObject: TObject;
begin
  if FDriverTransaction.TransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LUtil := TUtilSingleton.GetInstance;
  LResultSet := TMongoDBQuery.Create(nil);
  LObject := nil;
  
  try
    LResultSet.SetConnection(FConnection);
    LResultSet.Collection := LUTil.ParseCommandNoSQL('collection', FCommandText);
    
    // Using ORMBr mapping to create fields definition
    LObject := TMappingExplorer
                 .GetInstance
                   .Repository
                     .FindEntityByName('T' + LResultSet.Collection).Create;
                     
    TBind.Instance
         .SetInternalInitFieldDefsObjectClass(LResultSet, LObject);
         
    LResultSet.CreateDataSet;
    LResultSet.LogChanges := False;
    
    try
      LResultSet.Find(FCommandText);
      _SetMonitorLog(FCommandText, '', nil);
    except
      on E: Exception do
      begin
        _SetMonitorLog(FCommandText, E.Message, nil);
        LResultSet.Free;
        raise Exception.Create(E.Message);
      end;
    end;
    
    Result := TDriverResultSetWireMongoDB.Create(LResultSet, FMonitorCallback);
    if LResultSet.RecordCount = 0 then
       Result.FetchingAll := True;
  finally
    if Assigned(LObject) then
      LObject.Free;
  end;
end;

function TDriverQueryWireMongoDB.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverResultSetWireMongoDB }

constructor TDriverResultSetWireMongoDB.Create(const ADataSet: TMongoDBQuery;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(ADataSet, AMonitorCallback);
end;

{ TMongoDBQuery }

constructor TMongoDBQuery.Create(AOwner: TComponent);
begin
  inherited;
end;

destructor TMongoDBQuery.Destroy;
begin
  inherited;
end;

procedure TMongoDBQuery.Find(ACommandText: String);
var
  LDocQuery: IJSONDocument;
  LDocRecord: IJSONDocument;
  LDocFields: IJSONDocument;
  LQuery: TMongoWireQuery;
  LUtil: IUtilSingleton;
  LObject: TObject;
  LFilter: String;
begin
  LUtil := TUtilSingleton.GetInstance;
  LFilter := LUtil.ParseCommandNoSQL('filter', ACommandText, '{}');
  LDocQuery  := JSON(LFilter);
  LDocFields := JSON('{_id:0}');
  LDocRecord := JSON;
  LQuery := TMongoWireQuery.Create(FConnection.MongoWire);
  DisableControls;
  try
    LQuery.Query(FCollection, LDocQuery, LDocFields);
    while LQuery.Next(LDocRecord) do
    begin
      LObject := TMappingExplorer
                   .GetInstance
                     .Repository
                       .FindEntityByName('T' + FCollection).Create;
      LObject.MethodCall('Create', []);
      try
        TORMBrJson
          .JsonToObject(LDocRecord.ToString, LObject);
        /// <summary>
        /// Popula do dataset usado pelo ORMBr
        /// </summary>
        Append;
        TBind.Instance
             .SetPropertyToField(LObject, Self);
        Post;
      finally
        LObject.Free;
      end;
    end;
  finally
    LQuery.Free;
    First;
    EnableControls;
  end;
end;

function TMongoDBQuery.GetSequence(AMongoCampo: String): Int64;
begin
  // Implementation commented out in original
  Result := 0;
end;

procedure TMongoDBQuery.SetConnection(AConnection: TMongoWireConnection);
begin
  FConnection := AConnection;
end;

end.
