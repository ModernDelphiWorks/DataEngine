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

unit DriverSQLite3;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  SQLiteTable3,
  Datasnap.DBClient,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverSQLite3 = class(TDriverConnection)
  protected
    FConnection: TSQLiteDatabase;
    FScripts: TStringList;
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

  TDriverQuerySQLite3 = class(TDriverQuery)
  private
    FSQLQuery: TSQLitePreparedStatement;
    FConnection: TSQLiteDatabase;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetSQLite3 = class(TDriverResultSetBase)
  private
    procedure CreateFieldDefs;
  protected
    FConnection: TSQLiteDatabase;
    FDataSet: ISQLiteTable;
    FRecordCount: UInt32;
    FFetchingAll: Boolean;
    FFirstNext: Boolean;
    FFieldDefs: TFieldDefs;
    FDataSetInternal: TClientDataSet;
  public
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADataSet: ISQLiteTable; const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure Close; override;
    function NotEof: Boolean; override;
    function RecordCount: UInt32; override;
    function FieldDefs: TFieldDefs; override;
    function GetFieldValue(const AFieldName: String): Variant; overload; override;
    function GetFieldValue(const AFieldIndex: UInt16): Variant; overload; override;
    function GetFieldType(const AFieldName: String): TFieldType; override;
    function GetField(const AFieldName: String): TField; override;
    function RowsAffected: UInt32; override;
  end;

implementation

{ TDriverSQLite3 }

constructor TDriverSQLite3.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TSQLiteDatabase;
  FConnection.Connected := True;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FScripts := TStringList.Create;
end;

destructor TDriverSQLite3.Destroy;
begin
  FScripts.Free;
  FConnection.Connected := False;
  inherited;
end;

procedure TDriverSQLite3.Connect;
begin
  FConnection.Connected := True;
end;

procedure TDriverSQLite3.Disconnect;
begin
  // This native driver, for some reason, needs to keep the connection open
  // until the end of application usage.
  // FConnection.Connected := False; is called in Destroy;
end;

function TDriverSQLite3.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

procedure TDriverSQLite3.ExecuteDirect(const ASQL: String);
begin
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    FConnection.ExecSQL(ASQL);
    // SQLiteTable3 ExecSQL doesn't easily return rows affected for direct exec
    FRowsAffected := 0; 
    _SetMonitorLog(ASQL, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog(ASQL, E.Message, nil);
      raise;
    end;
  end;
end;

procedure TDriverSQLite3.ExecuteDirect(const ASQL: String;
  const AParams: TParams);
var
  LExeSQL: ISQLitePreparedStatement;
  LAffectedRows: Integer;
  LFor: Integer;
begin
  try
    if not Assigned(FConnection) then
      raise Exception.Create('Connection not assigned.');
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    if ASQL = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    LExeSQL := TSQLitePreparedStatement.Create(FConnection);
    LExeSQL.ClearParams;
    
    // There is a bug in SetParamVariant(Name, Value) passing NAME parameter,
    // so we use INDEX passing.
    for LFor := 0 to AParams.Count - 1 do
      LExeSQL.SetParamVariant(AParams.Items[LFor].Name, AParams.Items[LFor].Value);
      
    LExeSQL.PrepareStatement(ASQL);
    LExeSQL.ExecSQL(LAffectedRows);
    FRowsAffected := LAffectedRows;
    
    _SetMonitorLog(ASQL, '', AParams);
  except
    on E: Exception do
    begin
      _SetMonitorLog(ASQL, E.Message, nil);
      raise;
    end;
  end;
end;

procedure TDriverSQLite3.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverSQLite3.AddScript(const AScript: String);
begin
  FScripts.Add(AScript);
end;

procedure TDriverSQLite3.ExecuteScripts;
var
  LFor: Integer;
begin
  try
    for LFor := 0 to FScripts.Count - 1 do
    begin
       FConnection.ExecSQL(FScripts[LFor]);
       _SetMonitorLog(FScripts[LFor], '', nil);
    end;
  except
    on E: Exception do
    begin
      _SetMonitorLog('Error executing script', E.Message, nil);
      raise;
    end;
  end;
  FScripts.Clear;
end;

function TDriverSQLite3.CreateQuery: IDBQuery;
begin
  Result := TDriverQuerySQLite3.Create(FConnection, FDriverTransaction, FMonitorCallback);
end;

function TDriverSQLite3.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQuerySQLite3.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQuerySQLite3 }

constructor TDriverQuerySQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  FConnection := AConnection;
  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TSQLitePreparedStatement.Create(AConnection);
end;

destructor TDriverQuerySQLite3.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQuerySQLite3._GetCommandText: String;
begin
  Result := FSQLQuery.SQL;
end;

procedure TDriverQuerySQLite3._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL := ACommandText;
end;

procedure TDriverQuerySQLite3.ExecuteDirect;
begin
  try
    if FDriverTransaction.TransactionActive = nil then
      raise Exception.Create('Transaction not assigned.');

    FSQLQuery.ExecSQL;
    _SetMonitorLog(FSQLQuery.SQL, '', nil);
  except
    on E: Exception do
    begin
      _SetMonitorLog(FSQLQuery.SQL, E.Message, nil);
      raise;
    end;
  end;
end;

function TDriverQuerySQLite3.ExecuteQuery: IDBResultSet;
var
  LStatement: TSQLitePreparedStatement;
  LResultSet: ISQLiteTable;
begin
  LStatement := TSQLitePreparedStatement.Create(FConnection, FSQLQuery.SQL);
  try
    try
      if FDriverTransaction.TransactionActive = nil then
        raise Exception.Create('Transaction not assigned.');
        
      LResultSet := LStatement.ExecQueryIntf;
      _SetMonitorLog(FSQLQuery.SQL, '', nil);
    except
      on E: Exception do
      begin
        _SetMonitorLog(FSQLQuery.SQL, E.Message, nil);
        raise;
      end;
    end;
    
    Result := TDriverResultSetSQLite3.Create(FConnection, LResultSet, FMonitorCallback);
    if LResultSet.Eof then
      // In this driver/wrapper, Eof being true immediately might mean empty set or just positioned at end
      // Assuming FetchingAll logic is handled by caller or specific to this driver's behavior
      // Leaving as per original logic but standardized
      (Result as TDriverResultSetSQLite3).FFetchingAll := True;
  finally
    LStatement.Free;
  end;
end;

function TDriverQuerySQLite3.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

{ TDriverResultSetSQLite3 }

constructor TDriverResultSetSQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADataSet: ISQLiteTable; const AMonitorCallback: TMonitorProc);
var
  LField: TField;
  LFor: Integer;
begin
  inherited Create(AMonitorCallback);
  FConnection := AConnection;
  FDataSet := ADataSet;
  FFieldDefs := TFieldDefs.Create(nil);
  FDataSetInternal := TClientDataSet.Create(nil);
  
  for LFor := 0 to FDataSet.FieldCount - 1 do
  begin
    case FDataSet.Fields[LFor].FieldType of
      0: LField := TStringField.Create(FDataSetInternal);
      1: LField := TIntegerField.Create(FDataSetInternal);
      2: LField := TFloatField.Create(FDataSetInternal);
      3: LField := TWideStringField.Create(FDataSetInternal);
      4: LField := TBlobField.Create(FDataSetInternal);
      15: LField := TDateField.Create(FDataSetInternal);
      16: LField := TDateTimeField.Create(FDataSetInternal);
      else LField := TStringField.Create(FDataSetInternal); // Default fallback
    end;
    LField.FieldName := FDataSet.Fields[LFor].Name;
    LField.DataSet := FDataSetInternal;
    LField.FieldKind := fkData;
  end;
  
  FDataSetInternal.CreateDataSet;
  CreateFieldDefs;
end;

destructor TDriverResultSetSQLite3.Destroy;
begin
  FDataSet := nil;
  FFieldDefs.Free;
  FDataSetInternal.Free;
  inherited;
end;

procedure TDriverResultSetSQLite3.Close;
begin
  if Assigned(FDataSet) then
    FDataSet := nil;
end;

function TDriverResultSetSQLite3.FieldDefs: TFieldDefs;
begin
  Result := FFieldDefs;
end;

function TDriverResultSetSQLite3.GetFieldValue(const AFieldName: String): Variant;
begin
  Result := FDataSet.FieldByName[AFieldName].Value;
end;

function TDriverResultSetSQLite3.GetField(const AFieldName: String): TField;
begin
  if not FDataSetInternal.Active then
    FDataSetInternal.CreateDataSet;
    
  FDataSetInternal.Edit;
  FDataSetInternal.FieldByName(AFieldName).Value := FDataSet.FieldByName[AFieldName].Value;
  FDataSetInternal.Post;
  Result := FDataSetInternal.FieldByName(AFieldName);
end;

function TDriverResultSetSQLite3.GetFieldType(const AFieldName: String): TFieldType;
begin
  Result := TFieldType(FDataSet.FindField(AFieldName).FieldType);
end;

function TDriverResultSetSQLite3.GetFieldValue(const AFieldIndex: UInt16): Variant;
begin
  if Cardinal(AFieldIndex) > Cardinal(FDataSet.FieldCount - 1) then
    Exit(Variants.Null);

  if FDataSet.Fields[AFieldIndex].IsNull then
    Result := Variants.Null
  else
    Result := FDataSet.Fields[AFieldIndex].Value;
end;

function TDriverResultSetSQLite3.NotEof: Boolean;
begin
  if not FFirstNext then
    FFirstNext := True
  else
    FDataSet.Next;
  Result := not FDataSet.Eof;
end;

function TDriverResultSetSQLite3.RecordCount: UInt32;
begin
  Result := FDataSet.Row;
end;

function TDriverResultSetSQLite3.RowsAffected: UInt32;
begin
  // SQLiteTable3 doesn't typically store rows affected for result sets
  Result := 0;
end;

procedure TDriverResultSetSQLite3.CreateFieldDefs;
var
  LFor: Integer;
begin
  FFieldDefs.Clear;
  for LFor := 0 to FDataSet.FieldCount - 1 do
  begin
    with FFieldDefs.AddFieldDef do
    begin
      Name := FDataSet.Fields[LFor].Name;
      DataType := TFieldType(FDataSet.Fields[LFor].FieldType);
    end;
  end;
end;

end.
