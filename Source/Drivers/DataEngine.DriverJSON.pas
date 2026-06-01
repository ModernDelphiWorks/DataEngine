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

unit DataEngine.DriverJSON;

interface

uses
  DB,
  Classes,
  SysUtils,
  SyncObjs,
  System.JSON,
  System.IOUtils,
  FireDAC.Comp.Client,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TJSONConnection = class(TComponent)
  private
    FFileName: string;
  public
    property FileName: string read FFileName write FFileName;
  end;

  TDriverTransactionJSON = class(TDriverTransaction)
  public
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
  end;

  TDriverConnectionJSON = class(TDriverConnection)
  private
    FJSONData: TJSONArray;
    FLock: TCriticalSection;
    procedure _LoadFromFile;
    procedure _SaveToFile;
  protected
    procedure _InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil); override;
  public
    constructor Create(const AConnection: TComponent; const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc;
      const ACache: IDBCacheProvider = nil; const AMetadataCache: IDBMetadataCache = nil); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String): IDBDataSet; override;
    // Internal access for children
    property JSONData: TJSONArray read FJSONData;
    property Lock: TCriticalSection read FLock;
    procedure Save;
  end;

  TDriverQueryJSON = class(TDriverQuery)
  private
    FDriverConnectionJSON: TDriverConnectionJSON;
  protected
    procedure _InternalExecuteDirect; override;
    function _InternalExecuteQuery: IDBDataSet; override;
  public
    constructor Create(const ADriverConnectionJSON: TDriverConnectionJSON;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc; const ADriver: TDBEngineDriver;
      const ACache: IDBCacheProvider = nil; const AMetadataCache: IDBMetadataCache = nil); reintroduce; virtual;
    procedure Prepare; override;
  end;

  TDriverDataSetJSON = class(TDriverDataSet<TFDMemTable>)
  private
    FConnectionJSON: TDriverConnectionJSON;
    procedure _PopulateFromJSON(const AJSON: TJSONArray);
  protected
    procedure _SetActive(const Value: Boolean); override;
  public
    constructor Create(const AConnectionJSON: TDriverConnectionJSON; const AMonitorCallback: TMonitorProc);
    procedure Open; override;
  end;

implementation

{ TDriverTransactionJSON }

procedure TDriverTransactionJSON.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  // NoSQL Transaction simulation
end;

procedure TDriverTransactionJSON.Commit;
begin
  // NoSQL Transaction simulation
end;

procedure TDriverTransactionJSON.Rollback;
begin
  // NoSQL Transaction simulation
end;

{ TDriverConnectionJSON }

constructor TDriverConnectionJSON.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc; const ACache: IDBCacheProvider;
  const AMetadataCache: IDBMetadataCache);
begin
  inherited Create(AConnection, ADriverTransaction, ADriverName, AMonitorCallback, ACache, AMetadataCache);
  FLock := TCriticalSection.Create;
  FJSONData := nil;
end;

destructor TDriverConnectionJSON.Destroy;
begin
  Disconnect;
  FLock.Free;
  inherited;
end;

procedure TDriverConnectionJSON.Connect;
begin
  if IsConnected then
    Exit;

  _LoadFromFile;
end;

procedure TDriverConnectionJSON.Disconnect;
begin
  FLock.Enter;
  try
    if Assigned(FJSONData) then
      FreeAndNil(FJSONData);
  finally
    FLock.Leave;
  end;
end;

function TDriverConnectionJSON.IsConnected: Boolean;
begin
  Result := Assigned(FJSONData);
end;

procedure TDriverConnectionJSON._LoadFromFile;
var
  LFileName: string;
  LContent: string;
begin
  LFileName := TJSONConnection(FConnection).FileName;

  FLock.Enter;
  try
    if Assigned(FJSONData) then
      FreeAndNil(FJSONData);

    if TFile.Exists(LFileName) then
    begin
      LContent := TFile.ReadAllText(LFileName);
      if string.IsNullOrWhiteSpace(LContent) then
        FJSONData := TJSONArray.Create
      else
        FJSONData := TJSONObject.ParseJSONValue(LContent) as TJSONArray;
    end
    else
      FJSONData := TJSONArray.Create;
  finally
    FLock.Leave;
  end;
end;

procedure TDriverConnectionJSON._SaveToFile;
var
  LFileName: string;
  LTempName: string;
  LContent: string;
begin
  LFileName := TJSONConnection(FConnection).FileName;
  LTempName := LFileName + '.tmp';

  FLock.Enter;
  try
    LContent := FJSONData.ToJSON;
    TFile.WriteAllText(LTempName, LContent);

    if TFile.Exists(LFileName) then
      TFile.Delete(LFileName);

    TFile.Move(LTempName, LFileName);
  finally
    FLock.Leave;
  end;
end;

procedure TDriverConnectionJSON.Save;
begin
  _SaveToFile;
end;

procedure TDriverConnectionJSON._InternalExecuteDirect(const ASQL: String; const AParams: TParams);
begin
  // Direct execution for JSON might be used for DML scripts
end;

function TDriverConnectionJSON.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryJSON.Create(Self, FDriverTransaction, FMonitorCallback, FDriver, FCacheProvider, FMetadataCache);
end;

function TDriverConnectionJSON.CreateDataSet(const ASQL: String): IDBDataSet;
begin
  Result := TDriverDataSetJSON.Create(Self, FMonitorCallback);
  Result.CommandText := ASQL;
end;

{ TDriverQueryJSON }

constructor TDriverQueryJSON.Create(const ADriverConnectionJSON: TDriverConnectionJSON;
  const ADriverTransaction: TDriverTransaction; const AMonitorCallback: TMonitorProc;
  const ADriver: TDBEngineDriver; const ACache: IDBCacheProvider;
  const AMetadataCache: IDBMetadataCache);
begin
  inherited Create(ADriverTransaction, AMonitorCallback, ADriver, ACache, AMetadataCache);
  FDriverConnectionJSON := ADriverConnectionJSON;
end;

procedure TDriverQueryJSON.Prepare;
begin
  inherited;
end;

procedure TDriverQueryJSON._InternalExecuteDirect;
var
  LSQL: string;
  LJSONObject: TJSONObject;
  LParam: TParam;
  LIndex: Integer;
begin
  LSQL := CommandText.ToUpper.Trim;

  // Simple INSERT simulation: INSERT INTO <Table> (Fields) VALUES (Params)
  if LSQL.StartsWith('INSERT') then
  begin
    FDriverConnectionJSON.Lock.Enter;
    try
      LJSONObject := TJSONObject.Create;
      for LIndex := 0 to Params.Count - 1 do
      begin
        LParam := Params[LIndex];
        LJSONObject.AddPair(LParam.Name, TJSONString.Create(VarToStr(LParam.Value)));
      end;
      FDriverConnectionJSON.JSONData.AddElement(LJSONObject);
      FDriverConnectionJSON.Save;
    finally
      FDriverConnectionJSON.Lock.Leave;
    end;
  end;
  // Note: Full SQL parsing is out of scope. We assume basic DML interception.
end;

function TDriverQueryJSON._InternalExecuteQuery: IDBDataSet;
begin
  Result := TDriverDataSetJSON.Create(FDriverConnectionJSON, FMonitorCallback);
  Result.CommandText := CommandText;
  Result.Open;
end;

{ TDriverDataSetJSON }

constructor TDriverDataSetJSON.Create(const AConnectionJSON: TDriverConnectionJSON;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(TFDMemTable.Create(nil), AMonitorCallback);
  FConnectionJSON := AConnectionJSON;
end;

procedure TDriverDataSetJSON.Open;
begin
  inherited;
end;

procedure TDriverDataSetJSON._SetActive(const Value: Boolean);
begin
  if Value and not FDataSet.Active then
  begin
    _PopulateFromJSON(FConnectionJSON.JSONData);
  end;
  inherited _SetActive(Value);
end;

procedure TDriverDataSetJSON._PopulateFromJSON(const AJSON: TJSONArray);
var
  LVal: TJSONValue;
  LObj: TJSONObject;
  LPair: TJSONPair;
  LField: TField;
  LIndex: Integer;
begin
  if not Assigned(AJSON) then
    Exit;

  FDataSet.Close;
  FDataSet.FieldDefs.Clear;

  // Inference of fields from first object
  if AJSON.Count > 0 then
  begin
    LObj := AJSON.Items[0] as TJSONObject;
    for LIndex := 0 to LObj.Count - 1 do
    begin
      LPair := LObj.Pairs[LIndex];
      FDataSet.FieldDefs.Add(LPair.JsonString.Value, ftString, 255); // Simple inference
    end;
  end;

  FDataSet.CreateDataSet;

  for LIndex := 0 to AJSON.Count - 1 do
  begin
    LObj := AJSON.Items[LIndex] as TJSONObject;
    FDataSet.Append;
    for LVal in LObj do
    begin
      LPair := LVal as TJSONPair;
      LField := FDataSet.FindField(LPair.JsonString.Value);
      if Assigned(LField) then
        LField.Value := LPair.JsonValue.Value;
    end;
    FDataSet.Post;
  end;
end;

end.
