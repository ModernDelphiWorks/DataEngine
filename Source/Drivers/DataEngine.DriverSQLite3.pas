{
  ------------------------------------------------------------------------------
  DataEngine
  High-performance database engine abstraction framework for Delphi and Lazarus.

  SPDX-License-Identifier: MIT
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the MIT License.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.DriverSQLite3;

interface

uses
  System.Classes,
  System.SysUtils,
  System.Variants,
  Data.DB,
  SQLiteTable3,
  Datasnap.DBClient,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.Metadata.Manager;

type
  TDriverSQLite3 = class(TDriverConnection)
  protected
    FConnection: TSQLiteDatabase;
    FScripts: TStringList;
  public
    constructor Create(const AConnection: TComponent;
      const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc;
      const ACache: IDBCacheProvider = nil;
      const AMetadataCache: IDBMetadataCache = nil); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure _InternalExecuteDirect(const ASQL: String;
      const AParams: TParams = nil); override;
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
      const AMonitorCallback: TMonitorProc; const ADriver: TDriverName;
      const ACache: IDBCacheProvider = nil;
      const AMetadataCache: IDBMetadataCache = nil);
    destructor Destroy; override;
    procedure _InternalExecuteDirect; override;
    function _InternalExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
    function _GetParams: TParams; override;
    procedure Prepare; override;
    procedure Unprepare; override;
    procedure _ApplyParams(const ASQLiteStmt: ISQLitePreparedStatement);
  end;

  TDriverDataSetSQLite3 = class(TDriverDataSetBase)
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
      const ADataSet: ISQLiteTable; const AMonitorCallback: TMonitorProc;
      const ACachedFieldDefs: TFieldDefs = nil);
    destructor Destroy; override;
    procedure Close; override;
    function NotEof: Boolean; virtual;
    function RecordCount: UInt32; override;
    function FieldDefs: TFieldDefs; override;
    function _GetFieldValue(const AFieldName: String): Variant; override;
    function GetFieldValue(const AFieldIndex: UInt16): Variant; overload;
    function GetFieldType(const AFieldName: String): TFieldType; virtual;
    function GetField(const AFieldName: String): TField; virtual;
    function RowsAffected: UInt32; override;
    function Eof: Boolean; override;
    procedure Next; override;
    procedure First; override;
    function FieldByName(const AFieldName: String): TField; override;
    function FindField(const AFieldName: string): TField; override;
    function Fields: TFields; override;
  end;

implementation

uses
  DataEngine.CacheManager;

{ TDriverSQLite3 }

constructor TDriverSQLite3.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDriverName;
  const AMonitorCallback: TMonitorProc; const ACache: IDBCacheProvider;
  const AMetadataCache: IDBMetadataCache);
begin
  inherited Create(AConnection, ADriverTransaction, ADriverName, AMonitorCallback, ACache, AMetadataCache);
  FConnection := AConnection as TSQLiteDatabase;
  FConnection.Connected := True;
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

procedure TDriverSQLite3._InternalExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: ISQLitePreparedStatement;
  LAffectedRows: Integer;
  LFor: Integer;
begin
  if Assigned(AParams) and (AParams.Count > 0) then
  begin
    LExeSQL := TSQLitePreparedStatement.Create(FConnection);
    LExeSQL.ClearParams;

    for LFor := 0 to AParams.Count - 1 do
      LExeSQL.SetParamVariant(AParams.Items[LFor].Name, AParams.Items[LFor].Value);

    LExeSQL.PrepareStatement(ASQL);
    LExeSQL.ExecSQL(LAffectedRows);
    FRowsAffected := LAffectedRows;
  end
  else
  begin
    FConnection.ExecSQL(ASQL);
    FRowsAffected := 0;
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
  Result := TDriverQuerySQLite3.Create(FConnection, FDriverTransaction, FMonitorCallback, FDriver, FCacheProvider, FMetadataCache);
end;

function TDriverSQLite3.CreateDataSet(const ASQL: String): IDBResultSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQuerySQLite3.Create(FConnection, FDriverTransaction, FMonitorCallback, FDriver, FCacheProvider, FMetadataCache);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQuerySQLite3 }

constructor TDriverQuerySQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc; const ADriver: TDriverName;
  const ACache: IDBCacheProvider;
  const AMetadataCache: IDBMetadataCache);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');

  inherited Create(ADriverTransaction, AMonitorCallback, ADriver, ACache, AMetadataCache);
  FConnection := AConnection;
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

procedure TDriverQuerySQLite3._InternalExecuteDirect;
begin
  _ApplyParams(FSQLQuery);
  FSQLQuery.ExecSQL;
end;

function TDriverQuerySQLite3._InternalExecuteQuery: IDBDataSet;
var
  LStatement: TSQLitePreparedStatement;
  LResultSet: ISQLiteTable;
  LHasMetadataCache: Boolean;
  LFieldDefs: TFieldDefs;
begin
  LFieldDefs := TFieldDefs.Create(nil);
  try
    LHasMetadataCache := _TryApplyMetadataCache(FSQLQuery.SQL, LFieldDefs);
    
    LStatement := TSQLitePreparedStatement.Create(FConnection, FSQLQuery.SQL);
    try
      try
        if FDriverTransaction.TransactionActive = nil then
          raise Exception.Create('Transaction not assigned.');
          
        _ApplyParams(LStatement);
        LResultSet := LStatement.ExecQueryIntf;
        _SetMonitorLog(FSQLQuery.SQL, '', nil);
      except
        on E: Exception do
        begin
          _SetMonitorLog(FSQLQuery.SQL, E.Message, nil);
          raise;
        end;
      end;
      
      Result := TDriverDataSetSQLite3.Create(FConnection, LResultSet, FMonitorCallback, LFieldDefs);
      
      if not LHasMetadataCache then
        _TrySaveMetadataCache(FSQLQuery.SQL, Result.FieldDefs);

      if LResultSet.Eof then
        (Result as TDriverDataSetSQLite3).FFetchingAll := True;
    finally
      LStatement.Free;
    end;
  finally
    LFieldDefs.Free;
  end;
end;

function TDriverQuerySQLite3.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQuerySQLite3._GetParams: TParams;
begin
  Result := inherited _GetParams;
end;

procedure TDriverQuerySQLite3.Prepare;
begin
  if FSQLQuery.SQL <> '' then
    FSQLQuery.PrepareStatement;
end;

procedure TDriverQuerySQLite3.Unprepare;
begin
  // SQLiteNative wrapper may not have explicit unprepare
end;

procedure TDriverQuerySQLite3._ApplyParams(const ASQLiteStmt: ISQLitePreparedStatement);
var
  LFor: Integer;
  LParam: TParam;
begin
  if not Assigned(Params) then Exit;
  for LFor := 0 to Params.Count - 1 do
  begin
    LParam := Params[LFor];
    ASQLiteStmt.SetParamVariant(LParam.Name, LParam.Value);
  end;
end;

{ TDriverDataSetSQLite3 }

constructor TDriverDataSetSQLite3.Create(const AConnection: TSQLiteDatabase;
  const ADataSet: ISQLiteTable; const AMonitorCallback: TMonitorProc;
  const ACachedFieldDefs: TFieldDefs);
var
  LField: TField;
  LFor: Integer;
begin
  inherited Create;
  FMonitorCallback := AMonitorCallback;
  FConnection := AConnection;
  FDataSet := ADataSet;
  FFieldDefs := TFieldDefs.Create(nil);
  FDataSetInternal := TClientDataSet.Create(nil);

  if Assigned(ACachedFieldDefs) and (ACachedFieldDefs.Count > 0) then
    FFieldDefs.Assign(ACachedFieldDefs)
  else
    CreateFieldDefs;
  
  for LFor := 0 to FFieldDefs.Count - 1 do
  begin
    case FFieldDefs.Items[LFor].DataType of
      ftString: LField := TStringField.Create(FDataSetInternal);
      ftInteger: LField := TIntegerField.Create(FDataSetInternal);
      ftFloat: LField := TFloatField.Create(FDataSetInternal);
      ftWideString: LField := TWideStringField.Create(FDataSetInternal);
      ftBlob: LField := TBlobField.Create(FDataSetInternal);
      ftDate: LField := TDateField.Create(FDataSetInternal);
      ftDateTime: LField := TDateTimeField.Create(FDataSetInternal);
      else LField := TStringField.Create(FDataSetInternal); // Default fallback
    end;
    LField.FieldName := FFieldDefs.Items[LFor].Name;
    LField.DataSet := FDataSetInternal;
    LField.FieldKind := fkData;
  end;
  
  FDataSetInternal.CreateDataSet;
  
  while not FDataSet.EOF do
  begin
    FDataSetInternal.Append;
    for LFor := 0 to FDataSet.FieldCount - 1 do
      FDataSetInternal.Fields[LFor].Value := FDataSet.FieldsVal[LFor];
    FDataSetInternal.Post;
    FDataSet.Next;
  end;
  FDataSetInternal.First;
end;

destructor TDriverDataSetSQLite3.Destroy;
begin
  FDataSet := nil;
  FFieldDefs.Free;
  FDataSetInternal.Free;
  inherited;
end;

procedure TDriverDataSetSQLite3.Close;
begin
  if Assigned(FDataSet) then
    FDataSet := nil;
end;

function TDriverDataSetSQLite3.FieldDefs: TFieldDefs;
begin
  Result := FFieldDefs;
end;

function TDriverDataSetSQLite3._GetFieldValue(const AFieldName: String): Variant;
begin
  Result := FDataSet.FieldByName[AFieldName].Value;
end;

function TDriverDataSetSQLite3.GetField(const AFieldName: String): TField;
begin
  if not FDataSetInternal.Active then
    FDataSetInternal.CreateDataSet;
    
  FDataSetInternal.Edit;
  FDataSetInternal.FieldByName(AFieldName).Value := FDataSet.FieldByName[AFieldName].Value;
  FDataSetInternal.Post;
  Result := FDataSetInternal.FieldByName(AFieldName);
end;

function TDriverDataSetSQLite3.GetFieldType(const AFieldName: String): TFieldType;
begin
  Result := TFieldType(FDataSet.FindField(AFieldName).FieldType);
end;

function TDriverDataSetSQLite3.GetFieldValue(const AFieldIndex: UInt16): Variant;
begin
  if Cardinal(AFieldIndex) > Cardinal(FDataSet.FieldCount - 1) then
    Exit(Null);

  if FDataSet.Fields[AFieldIndex].IsNull then
    Result := Null
  else
    Result := FDataSet.Fields[AFieldIndex].Value;
end;

function TDriverDataSetSQLite3.NotEof: Boolean;
begin
  if not FFirstNext then
    FFirstNext := True
  else
    FDataSet.Next;
  Result := not FDataSet.Eof;
end;

function TDriverDataSetSQLite3.Eof: Boolean;
begin
  Result := FDataSetInternal.Eof;
end;

procedure TDriverDataSetSQLite3.Next;
begin
  FDataSetInternal.Next;
end;

procedure TDriverDataSetSQLite3.First;
begin
  FDataSetInternal.First;
end;

function TDriverDataSetSQLite3.FieldByName(const AFieldName: String): TField;
begin
  Result := FDataSetInternal.FieldByName(AFieldName);
end;

function TDriverDataSetSQLite3.FindField(const AFieldName: string): TField;
begin
  Result := FDataSetInternal.FindField(AFieldName);
end;

function TDriverDataSetSQLite3.Fields: TFields;
begin
  Result := FDataSetInternal.Fields;
end;

function TDriverDataSetSQLite3.RecordCount: UInt32;
begin
  Result := FDataSet.Row;
end;

function TDriverDataSetSQLite3.RowsAffected: UInt32;
begin
  // SQLiteTable3 doesn't typically store rows affected for result sets
  Result := 0;
end;

procedure TDriverDataSetSQLite3.CreateFieldDefs;
var
  LFor: Integer;
  LField: TSQLiteField;
  LType: TFieldType;
begin
  FFieldDefs.Clear;
  for LFor := 0 to FDataSet.FieldCount - 1 do
  begin
    LField := FDataSet.Fields[LFor];
    case LField.FieldType of
      1: LType := ftLargeint; // dtInt
      2: LType := ftFloat;    // dtNumeric
      15, 16: LType := ftDateTime; // dtDate, dtDateTime
      else LType := ftString;
    end;
    FFieldDefs.Add(LField.Name, LType, 255);
  end;
end;

end.


