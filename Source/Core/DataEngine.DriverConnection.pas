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

{$ifdef fpc}
  {$mode delphi}{$H+}
{$endif}

unit DataEngine.DriverConnection;

interface

uses
  DB,
  Math,
  Classes,
  SysUtils,
  Variants,
  SyncObjs,
  System.Diagnostics,
  Generics.Collections,
  DataEngine.Consts,
  DataEngine.FactoryInterfaces;

type
  TDriverTransaction = class;

  TDriverConnection = class abstract
  protected
    FDriverTransaction: TDriverTransaction;
    FMonitorCallback: TMonitorProc;
    FUserMonitorCallback: TMonitorProc;
    FDriver: TDriverName;
    FRowsAffected: UInt32;
    FCacheProvider: IDBCacheProvider;
    FMetadataCache: IDBMetadataCache;
    FResiliencePolicy: IDBResiliencePolicy;
    FObservers: TList<IDBObserver>;
    FObserverLock: TCriticalSection;
    FSlowQueryThreshold: Integer;
    procedure _Notify(const AParam: TMonitorParam); virtual;
    procedure _InternalNotify(const AEventType: TMonitorEventType; const ACommand: string;
      const AParams: TParams; const AExecutionTime: Int64 = 0; const AFetchTime: Int64 = 0;
      const ARowsAffected: UInt32 = 0; const AException: Exception = nil); virtual;
    procedure _SetMonitorLog(const ASQL: String; const ATransactionName: String;
      const AParams: TParams);
    function _GetTransactionName: String; virtual;
    procedure _HandleCacheInvalidation(const ASQL: string; const AParams: TParams = nil); virtual;
    procedure _UpdateNativeParams(ANativeParams, ASourceParams: TParams); virtual;
    procedure _InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil); virtual; abstract;
    function _IsAlive: Boolean; virtual;
  public
    constructor Create(const AConnection: TComponent; const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc;
      const ACache: IDBCacheProvider = nil; const AMetadataCache: IDBMetadataCache = nil); virtual;
    procedure Connect; virtual;
    procedure Disconnect; virtual;
    procedure ExecuteDirect(const ASQL: String); overload; virtual;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); overload; virtual;
    procedure ExecuteScript(const AScript: String); virtual;
    procedure AddScript(const AScript: String); virtual;
    procedure ExecuteScripts; virtual;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); virtual;
    function IsConnected: Boolean; virtual;
    function CreateQuery: IDBQuery; virtual;
    function CreateDataSet(const ASQL: String): IDBDataSet; virtual;
    function GetSQLScripts: String; virtual;
    function RowsAffected: UInt32; virtual;
    function GetDriver: TDriverName; virtual;
    function MonitorCallback: TMonitorProc; virtual;
    function Options: IOptions; virtual;
    function Cache: IDBCacheProvider; virtual;
    function MetadataCache: IDBMetadataCache; virtual;
    procedure SetCacheProvider(ACache: IDBCacheProvider); virtual;
    procedure SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache); virtual;
    procedure RefreshMetadata(const ATableName: string); virtual;
    function TryGetCachedMetadata(const ATableName: string; ADestination: TFieldDefs): Boolean; virtual;
    function BulkLoader: IDBBulkLoader; virtual;
    function IsAlive: Boolean; virtual;
    function ResiliencePolicy: IDBResiliencePolicy; virtual;
    procedure SetResiliencePolicy(APolicy: IDBResiliencePolicy); virtual;
    procedure AddObserver(const AObserver: IDBObserver); virtual;
    procedure RemoveObserver(const AObserver: IDBObserver); virtual;
    function SlowQueryThreshold: Integer; virtual;
    procedure SetSlowQueryThreshold(const AValue: Integer); virtual;
    destructor Destroy; override;
  end;

  TDriverTransaction = class abstract(TInterfacedObject, IDBTransaction)
  protected
    FTransactionList: TDictionary<String, TComponent>;
    FTransactionActive: TComponent;
    FMonitorCallback: TMonitorProc;
    FLock: TCriticalSection;
    function _GetTransaction(const AKey: String): TComponent; virtual;
    function _InTransaction: Boolean; virtual;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); virtual;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); virtual;
    procedure Commit; virtual;
    procedure Rollback; virtual;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); virtual;
    procedure UseTransaction(const AKey: String); virtual;
    function TransactionActive: TComponent; virtual;
    function InTransaction: Boolean; virtual;
  end;

  TOptions = class(TInterfacedObject, IOptions)
  strict private
    FStoreGUIDAsOctet: Boolean;
  public
    constructor Create;
    function StoreGUIDAsOctet(const AValue: Boolean): IOptions; overload;
    function StoreGUIDAsOctet: Boolean; overload;
  end;

  TDriverQuery = class(TInterfacedObject, IDBQuery)
  protected
    FDriverTransaction: TDriverTransaction;
    FMonitorCallback: TMonitorProc;
    FRowsAffected: UInt32;
    FCacheProvider: IDBCacheProvider;
    FDriver: TDriverName;
    FMetadataCache: IDBMetadataCache;
    FFetchOptions: TFetchOptions;
    FSlowQueryThreshold: Integer;
    FParams: TParams;
  protected
    procedure _Notify(const AEventType: TMonitorEventType; const ACommand: string;
      AParams: TParams; const AExecutionTime: Int64 = 0; const AFetchTime: Int64 = 0;
      const ARowsAffected: UInt32 = 0; const AException: Exception = nil); virtual;
    procedure _SetMonitorLog(const ASQL: String; const ATransactionName: String;
      const AParams: TParams);
    function _GetTransactionName: String; virtual;
    procedure _SetCommandText(const ACommandText: String); virtual;
    function _GetCommandText: String; virtual;
    function _GetParams: TParams; virtual;
    function _TryApplyMetadataCache(const ASQL: string; ADestination: TFieldDefs): Boolean; virtual;
    procedure _TrySaveMetadataCache(const ASQL: string; ASource: TFieldDefs); virtual;
    procedure _RefreshMetadata; virtual;
    function _GetFetchOptions: TFetchOptions; virtual;
    procedure _SetFetchOptions(const Value: TFetchOptions); virtual;
    function _GetSlowQueryThreshold: Integer; virtual;
    procedure _SetSlowQueryThreshold(const Value: Integer); virtual;
    function _InternalExecuteQuery: IDBDataSet; virtual; abstract;
    procedure _InternalExecuteDirect; virtual; abstract;
  public
    constructor Create(const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc; const ADriver: TDriverName;
      const ACache: IDBCacheProvider = nil; const AMetadataCache: IDBMetadataCache = nil;
      const ASlowQueryThreshold: Integer = DEFAULT_SLOW_QUERY_THRESHOLD); virtual;
    destructor Destroy; override;
    procedure ExecuteDirect; virtual;
    function ExecuteQuery: IDBDataSet; virtual;
    function RowsAffected: UInt32; virtual;
    property Params: TParams read _GetParams;
    procedure Prepare; virtual;
    procedure Unprepare; virtual;
    function ParamByName(const AValue: string): TParam; virtual;
  end;

  TDriverDataSetBase = class abstract(TInterfacedObject, IDBDataSet)
  protected
    FRecordCount: UInt32;
    FMonitorCallback: TMonitorProc;
    FFetchOptions: TFetchOptions;
    FFetchingAll: Boolean;
    FFetchStopwatch: TStopwatch;
    FFetchStarted: Boolean;
    FCommandText: string;
    FNotEofStarted: Boolean;
    function _GetFilter: String; virtual;
    function _GetFiltered: Boolean; virtual;
    function _GetFilterOptions: TFilterOptions; virtual;
    function _GetActive: Boolean; virtual;
    function _GetCommandText: String; virtual;
    function _GetAfterCancel: TDataSetNotifyEvent; virtual;
    function _GetAfterClose: TDataSetNotifyEvent; virtual;
    function _GetAfterDelete: TDataSetNotifyEvent; virtual;
    function _GetAfterEdit: TDataSetNotifyEvent; virtual;
    function _GetAfterInsert: TDataSetNotifyEvent; virtual;
    function _GetAfterOpen: TDataSetNotifyEvent; virtual;
    function _GetAfterPost: TDataSetNotifyEvent; virtual;
    function _GetAfterRefresh: TDataSetNotifyEvent; virtual;
    function _GetAfterScroll: TDataSetNotifyEvent; virtual;
    function _GetAutoCalcFields: Boolean; virtual;
    function _GetBeforeCancel: TDataSetNotifyEvent; virtual;
    function _GetBeforeClose: TDataSetNotifyEvent; virtual;
    function _GetBeforeDelete: TDataSetNotifyEvent; virtual;
    function _GetBeforeEdit: TDataSetNotifyEvent; virtual;
    function _GetBeforeInsert: TDataSetNotifyEvent; virtual;
    function _GetBeforeOpen: TDataSetNotifyEvent; virtual;
    function _GetBeforePost: TDataSetNotifyEvent; virtual;
    function _GetBeforeRefresh: TDataSetNotifyEvent; virtual;
    function _GetBeforeScroll: TDataSetNotifyEvent; virtual;
    function _GetOnCalcFields: TDataSetNotifyEvent; virtual;
    function _GetOnDeleteError: TDataSetErrorEvent; virtual;
    function _GetOnEditError: TDataSetErrorEvent; virtual;
    function _GetOnFilterRecord: TFilterRecordEvent; virtual;
    function _GetOnNewRecord: TDataSetNotifyEvent; virtual;
    function _GetOnPostError: TDataSetErrorEvent; virtual;
    function _GetSortFields: String; virtual;
    function _GetBookmark: TBookmark; virtual;
    function _GetBlockReadSize: Integer; virtual;
    function _GetFieldValue(const FieldName: string): Variant; virtual;
    function _GetFetchingAll: Boolean; virtual;
    function _GetFetchOptions: TFetchOptions; virtual;
    procedure _SetFilter(const Value: String); virtual;
    procedure _SetFiltered(const Value: Boolean); virtual;
    procedure _SetFilterOptions(Value: TFilterOptions); virtual;
    procedure _SetActive(const Value: Boolean); virtual;
    procedure _SetCommandText(const ACommandText: String); virtual;
    procedure _SetUniDirectional(const Value: Boolean); virtual;
    procedure _SetReadOnly(const Value: Boolean); virtual;
    procedure _SetCachedUpdates(const Value: Boolean); virtual;
    procedure _SetAfterCancel(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterOpen(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterClose(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterDelete(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterEdit(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterInsert(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterPost(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterRefresh(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAfterScroll(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetAutoCalcFields(const Value: Boolean); virtual;
    procedure _SetBeforeCancel(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeDelete(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeEdit(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeInsert(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeOpen(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeClose(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforePost(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeRefresh(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetBeforeScroll(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetOnFilterRecord(const Value: TFilterRecordEvent); virtual;
    procedure _SetOnCalcFields(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetOnDeleteError(const Value: TDataSetErrorEvent); virtual;
    procedure _SetOnEditError(const Value: TDataSetErrorEvent); virtual;
    procedure _SetOnNewRecord(const Value: TDataSetNotifyEvent); virtual;
    procedure _SetOnPostError(const Value: TDataSetErrorEvent); virtual;
    procedure _SetSortFields(const Value: String); virtual;
    procedure _SetBookmark(const Value: TBookmark); virtual;
    procedure _SetBlockReadSize(const Value: Integer); virtual;
    procedure _SetFieldValue(const FieldName: string; const Value: Variant); virtual;
    procedure _SetFetchingAll(const Value: Boolean); virtual;
    procedure _SetFetchOptions(const Value: TFetchOptions); virtual;
  public
    constructor Create; virtual;
    destructor Destroy; override;
    procedure Close; virtual;
    procedure Open; virtual;
    procedure OpenAsync(const AOnComplete: TProc; const AOnError: TProc<Exception> = nil); virtual;
    procedure Refresh; virtual;
    procedure Delete; virtual;
    procedure Cancel; virtual;
    procedure Clear; virtual;
    procedure DisableControls; virtual;
    procedure EnableControls; virtual;
    procedure Next; virtual;
    procedure Prior; virtual;
    procedure Append; virtual;
    procedure Insert; virtual;
    procedure Edit; virtual;
    procedure Post; virtual;
    procedure First; virtual;
    procedure Last; virtual;
    procedure MoveBy(Distance: Integer); virtual;
    procedure CheckRequiredFields; virtual;
    procedure FreeBookmark(Bookmark: TBookmark); virtual;
    procedure ClearFields; virtual;
    procedure ApplyUpdates; virtual;
    procedure CancelUpdates; virtual;
    function Locate(const KeyFields: String; const KeyValues: Variant; Options: TLocateOptions): Boolean; virtual;
    function Lookup(const KeyFields: String; const KeyValues: Variant; const ResultFields: String): Variant; virtual;
    function FieldCount: UInt16; virtual;
    function State: TDataSetState; virtual;
    function Modified: Boolean; virtual;
    function FieldByName(const AFieldName: String): TField; virtual;
    function FindField(const AFieldName: string): TField; virtual;
    function FindFirst: Boolean; virtual;
    function FindLast: Boolean; virtual;
    function FindNext: Boolean; virtual;
    function FindPrior: Boolean; virtual;
    function RecordCount: UInt32; virtual;
    function FieldDefs: TFieldDefs; virtual;
    function Eof: Boolean; virtual;
    function NotEof: Boolean;
    function GetFieldValue(const AFieldName: string): Variant;
    function Bof: Boolean; virtual;
    function RecNo: Integer; virtual;
    function CanRefresh: Boolean; virtual;
    function DataSetField: TDataSetField; virtual;
    function DefaultFields: Boolean; virtual;
    function Designer: TDataSetDesigner; virtual;
    function FieldList: TFieldList; virtual;
    function Found: Boolean; virtual;
    function ObjectView: Boolean; virtual;
    function RecordSize: Word; virtual;
    function SparseArrays: Boolean; virtual;
    function FieldDefList: TFieldDefList; virtual;
    function Fields: TFields; virtual;
    function AggFields: TFields; virtual;
    function UpdatesPending: Boolean; virtual;
    function UpdateStatus: TUpdateStatus; virtual;
    function CanModify: Boolean; virtual;
    function IsEmpty: Boolean; virtual;
    function DataSource: TDataSource; virtual;
    function AsDataSet: TDataSet; virtual;
    function IsUniDirectional: Boolean; virtual;
    function IsReadOnly: Boolean; virtual;
    function IsCachedUpdates: Boolean; virtual;
    function RowsAffected: UInt32; virtual;
    function NormalizeFieldValue(const AFieldName: String; const AValue: Variant): Variant; virtual;
  end;

  TDriverDataSet<T: TDataSet> = class(TDriverDataSetBase)
  protected
    FDataSet: T;
    procedure _SetMonitorLog(const ASQL: String; ATransactionName: String;
      const AParams: TParams);
    function _GetFilter: String; override;
    function _GetFiltered: Boolean; override;
    function _GetFilterOptions: TFilterOptions; override;
    function _GetActive: Boolean; override;
    function _GetCommandText: String; override;
    function _GetAfterCancel: TDataSetNotifyEvent; override;
    function _GetAfterClose: TDataSetNotifyEvent; override;
    function _GetAfterDelete: TDataSetNotifyEvent; override;
    function _GetAfterEdit: TDataSetNotifyEvent; override;
    function _GetAfterInsert: TDataSetNotifyEvent; override;
    function _GetAfterOpen: TDataSetNotifyEvent; override;
    function _GetAfterPost: TDataSetNotifyEvent; override;
    function _GetAfterRefresh: TDataSetNotifyEvent; override;
    function _GetAfterScroll: TDataSetNotifyEvent; override;
    function _GetAutoCalcFields: Boolean; override;
    function _GetBeforeCancel: TDataSetNotifyEvent; override;
    function _GetBeforeClose: TDataSetNotifyEvent; override;
    function _GetBeforeDelete: TDataSetNotifyEvent; override;
    function _GetBeforeEdit: TDataSetNotifyEvent; override;
    function _GetBeforeInsert: TDataSetNotifyEvent; override;
    function _GetBeforeOpen: TDataSetNotifyEvent; override;
    function _GetBeforePost: TDataSetNotifyEvent; override;
    function _GetBeforeRefresh: TDataSetNotifyEvent; override;
    function _GetBeforeScroll: TDataSetNotifyEvent; override;
    function _GetOnCalcFields: TDataSetNotifyEvent; override;
    function _GetOnDeleteError: TDataSetErrorEvent; override;
    function _GetOnEditError: TDataSetErrorEvent; override;
    function _GetOnFilterRecord: TFilterRecordEvent; override;
    function _GetOnNewRecord: TDataSetNotifyEvent; override;
    function _GetOnPostError: TDataSetErrorEvent; override;
    function _GetSortFields: String; override;
    function _GetBookmark: TBookmark; override;
    function _GetBlockReadSize: Integer; override;
    function _GetFieldValue(const FieldName: string): Variant; override;
    procedure _SetFilter(const Value: String); override;
    procedure _SetFiltered(const Value: Boolean); override;
    procedure _SetFilterOptions(Value: TFilterOptions); override;
    procedure _SetActive(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetAfterCancel(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterOpen(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterClose(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterDelete(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterEdit(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterInsert(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterPost(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterRefresh(const Value: TDataSetNotifyEvent); override;
    procedure _SetAfterScroll(const Value: TDataSetNotifyEvent); override;
    procedure _SetAutoCalcFields(const Value: Boolean); override;
    procedure _SetBeforeCancel(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeDelete(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeEdit(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeInsert(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeOpen(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeClose(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforePost(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeRefresh(const Value: TDataSetNotifyEvent); override;
    procedure _SetBeforeScroll(const Value: TDataSetNotifyEvent); override;
    procedure _SetOnFilterRecord(const Value: TFilterRecordEvent); override;
    procedure _SetOnCalcFields(const Value: TDataSetNotifyEvent); override;
    procedure _SetOnDeleteError(const Value: TDataSetErrorEvent); override;
    procedure _SetOnEditError(const Value: TDataSetErrorEvent); override;
    procedure _SetOnNewRecord(const Value: TDataSetNotifyEvent); override;
    procedure _SetOnPostError(const Value: TDataSetErrorEvent); override;
    procedure _SetSortFields(const Value: String); override;
    procedure _SetFieldValue(const FieldName: string; const Value: Variant); override;
    procedure _SetFetchingAll(const Value: Boolean); override;
  public
    constructor Create(const ADataSet: T; const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure Close; override;
    procedure Open; override;
    procedure Refresh; override;
    procedure Delete; override;
    procedure Cancel; override;
    procedure Clear; override;
    procedure DisableControls; override;
    procedure EnableControls; override;
    procedure Next; override;
    procedure Prior; override;
    procedure Append; override;
    procedure Insert; override;
    procedure Edit; override;
    procedure Post; override;
    procedure First; override;
    procedure Last; override;
    procedure MoveBy(Distance: Integer); override;
    procedure CheckRequiredFields; override;
    procedure FreeBookmark(Bookmark: TBookmark); override;
    procedure ClearFields; override;
    procedure ApplyUpdates; override;
    procedure CancelUpdates; override;
    function Locate(const KeyFields: String; const KeyValues: Variant; Options: TLocateOptions): Boolean; override;
    function Lookup(const KeyFields: String; const KeyValues: Variant; const ResultFields: String): Variant; override;
    function FieldCount: UInt16; override;
    function State: TDataSetState; override;
    function Modified: Boolean; override;
    function FieldByName(const AFieldName: String): TField; override;
    function FindField(const AFieldName: string): TField; override;
    function FindFirst: Boolean; override;
    function FindLast: Boolean; override;
    function FindNext: Boolean; override;
    function FindPrior: Boolean; override;
    function RecordCount: UInt32; override;
    function FieldDefs: TFieldDefs; override;
    function Eof: Boolean; override;
    function Bof: Boolean; override;
    function RecNo: Integer; override;
    function CanRefresh: Boolean; override;
    function DataSetField: TDataSetField; override;
    function DefaultFields: Boolean; override;
    function Designer: TDataSetDesigner; override;
    function FieldList: TFieldList; override;
    function Found: Boolean; override;
    function ObjectView: Boolean; override;
    function RecordSize: Word; override;
    function SparseArrays: Boolean; override;
    function FieldDefList: TFieldDefList; override;
    function Fields: TFields; override;
    function AggFields: TFields; override;
    function UpdateStatus: TUpdateStatus; override;
    function CanModify: Boolean; override;
    function IsEmpty: Boolean; override;
    function DataSource: TDataSource; override;
    function AsDataSet: TDataSet; override;
    function IsUniDirectional: Boolean; override;
    function IsReadOnly: Boolean; override;
    function IsCachedUpdates: Boolean; override;
    function RowsAffected: UInt32; override;
    function NormalizeFieldValue(const AFieldName: String; const AValue: Variant): Variant; override;
  end;

  TDefaultResiliencePolicy = class(TInterfacedObject, IDBResiliencePolicy)
  private
    FOnRetry: TResilienceEvent;
  public
    function GetRetryCount: Integer;
    function GetDelay: Integer;
    function ShouldRetry(const AException: Exception; const AAttempt: Integer): Boolean;
    procedure ExecuteDelay(const AAttempt: Integer);
    procedure SetOnRetry(const AEvent: TResilienceEvent);
    function GetOnRetry: TResilienceEvent;
  end;

implementation

uses
  DataEngine.CacheTypes,
  DataEngine.CacheManager,
  DataEngine.Metadata.Manager,
  DataEngine.AsyncRunner;

{ TDriverDataSetBase }

constructor TDriverDataSetBase.Create;
begin
  inherited Create;
  FRecordCount := 0;
  FMonitorCallback := nil;
  FFetchOptions := TFetchOptions.Create(fmAll);
  FFetchingAll := True;
end;

destructor TDriverDataSetBase.Destroy;
begin
  inherited;
end;

procedure TDriverDataSetBase.Close;
begin
end;

procedure TDriverDataSetBase.Open;
begin
end;

procedure TDriverDataSetBase.OpenAsync(const AOnComplete: TProc; const AOnError: TProc<Exception>);
begin
  TDataEngineAsync.Run(
    procedure
    begin
      Open;
    end,
    AOnComplete,
    AOnError
  );
end;

procedure TDriverDataSetBase.Refresh;
begin
end;

procedure TDriverDataSetBase.Delete;
begin
end;

procedure TDriverDataSetBase.Cancel;
begin
end;

procedure TDriverDataSetBase.Clear;
begin
end;

procedure TDriverDataSetBase.DisableControls;
begin
end;

procedure TDriverDataSetBase.EnableControls;
begin
end;

procedure TDriverDataSetBase.Next;
begin
end;

procedure TDriverDataSetBase.Prior;
begin
end;

procedure TDriverDataSetBase.Append;
begin
end;

procedure TDriverDataSetBase.Insert;
begin
end;

procedure TDriverDataSetBase.Edit;
begin
end;

procedure TDriverDataSetBase.Post;
begin
end;

procedure TDriverDataSetBase.First;
begin
end;

procedure TDriverDataSetBase.Last;
begin
end;

procedure TDriverDataSetBase.MoveBy(Distance: Integer);
begin
end;

procedure TDriverDataSetBase.CheckRequiredFields;
begin
end;

procedure TDriverDataSetBase.FreeBookmark(Bookmark: TBookmark);
begin
end;

procedure TDriverDataSetBase.ClearFields;
begin
end;

procedure TDriverDataSetBase.ApplyUpdates;
begin
end;

procedure TDriverDataSetBase.CancelUpdates;
begin
end;

function TDriverDataSetBase.Locate(const KeyFields: String; const KeyValues: Variant; Options: TLocateOptions): Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.Lookup(const KeyFields: String; const KeyValues: Variant; const ResultFields: String): Variant;
begin
  Result := Null;
end;

function TDriverDataSetBase.FieldCount: UInt16;
begin
  Result := 0;
end;

function TDriverDataSetBase.State: TDataSetState;
begin
  Result := dsInactive;
end;

function TDriverDataSetBase.Modified: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.FieldByName(const AFieldName: String): TField;
begin
  Result := nil;
end;

function TDriverDataSetBase.FindField(const AFieldName: string): TField;
begin
  Result := nil;
end;

function TDriverDataSetBase.FindFirst: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.FindLast: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.FindNext: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.FindPrior: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.RecordCount: UInt32;
begin
  Result := FRecordCount;
end;

function TDriverDataSetBase.FieldDefs: TFieldDefs;
begin
  Result := nil;
end;

function TDriverDataSetBase.Eof: Boolean;
begin
  Result := True;
end;

function TDriverDataSetBase.NotEof: Boolean;
begin
  if FNotEofStarted then
    Next
  else
    FNotEofStarted := True;
  Result := not Eof;
end;

function TDriverDataSetBase.GetFieldValue(const AFieldName: string): Variant;
begin
  Result := _GetFieldValue(AFieldName);
end;

function TDriverDataSetBase.Bof: Boolean;
begin
  Result := True;
end;

function TDriverDataSetBase.RecNo: Integer;
begin
  Result := -1;
end;

function TDriverDataSetBase.CanRefresh: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.DataSetField: TDataSetField;
begin
  Result := nil;
end;

function TDriverDataSetBase.DefaultFields: Boolean;
begin
  Result := True;
end;

function TDriverDataSetBase.Designer: TDataSetDesigner;
begin
  Result := nil;
end;

function TDriverDataSetBase.FieldList: TFieldList;
begin
  Result := nil;
end;

function TDriverDataSetBase.Found: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.ObjectView: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.RecordSize: Word;
begin
  Result := 0;
end;

function TDriverDataSetBase.SparseArrays: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.FieldDefList: TFieldDefList;
begin
  Result := nil;
end;

function TDriverDataSetBase.Fields: TFields;
begin
  Result := nil;
end;

function TDriverDataSetBase.AggFields: TFields;
begin
  Result := nil;
end;

function TDriverDataSetBase.UpdatesPending: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.UpdateStatus: TUpdateStatus;
begin
  Result := usUnmodified;
end;

function TDriverDataSetBase.CanModify: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.IsEmpty: Boolean;
begin
  Result := True;
end;

function TDriverDataSetBase.DataSource: TDataSource;
begin
  Result := nil;
end;

function TDriverDataSetBase.AsDataSet: TDataSet;
begin
  Result := nil;
end;

function TDriverDataSetBase.IsUniDirectional: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.IsReadOnly: Boolean;
begin
  Result := True;
end;

function TDriverDataSetBase.IsCachedUpdates: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase.RowsAffected: UInt32;
begin
  Result := 0;
end;

function TDriverDataSetBase.NormalizeFieldValue(const AFieldName: String; const AValue: Variant): Variant;
begin
  Result := AValue;
end;

function TDriverDataSetBase._GetFilter: String;
begin
  Result := string.Empty;
end;

function TDriverDataSetBase._GetFiltered: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase._GetFilterOptions: TFilterOptions;
begin
  Result := [];
end;

function TDriverDataSetBase._GetActive: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase._GetCommandText: String;
begin
  Result := FCommandText;
end;

procedure TDriverDataSetBase._SetCommandText(const ACommandText: String);
begin
  FCommandText := ACommandText;
end;

function TDriverDataSetBase._GetAfterCancel: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterClose: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterDelete: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterEdit: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterInsert: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterOpen: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterPost: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterRefresh: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAfterScroll: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetAutoCalcFields: Boolean;
begin
  Result := False;
end;

function TDriverDataSetBase._GetBeforeCancel: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeClose: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeDelete: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeEdit: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeInsert: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeOpen: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforePost: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeRefresh: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBeforeScroll: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetOnCalcFields: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetOnDeleteError: TDataSetErrorEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetOnEditError: TDataSetErrorEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetOnFilterRecord: TFilterRecordEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetOnNewRecord: TDataSetNotifyEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetOnPostError: TDataSetErrorEvent;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetSortFields: String;
begin
  Result := string.Empty;
end;

function TDriverDataSetBase._GetBookmark: TBookmark;
begin
  Result := nil;
end;

function TDriverDataSetBase._GetBlockReadSize: Integer;
begin
  Result := 0;
end;

function TDriverDataSetBase._GetFieldValue(const FieldName: string): Variant;
begin
  Result := Null;
end;

function TDriverDataSetBase._GetFetchingAll: Boolean;
begin
  Result := FFetchingAll;
end;

function TDriverDataSetBase._GetFetchOptions: TFetchOptions;
begin
  Result := FFetchOptions;
end;

procedure TDriverDataSetBase._SetFilter(const Value: String);
begin
end;

procedure TDriverDataSetBase._SetFiltered(const Value: Boolean);
begin
end;

procedure TDriverDataSetBase._SetFilterOptions(Value: TFilterOptions);
begin
end;

procedure TDriverDataSetBase._SetActive(const Value: Boolean);
begin
end;



procedure TDriverDataSetBase._SetUniDirectional(const Value: Boolean);
begin
end;

procedure TDriverDataSetBase._SetReadOnly(const Value: Boolean);
begin
end;

procedure TDriverDataSetBase._SetCachedUpdates(const Value: Boolean);
begin
end;

procedure TDriverDataSetBase._SetAfterCancel(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterOpen(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterClose(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterDelete(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterEdit(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterInsert(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterPost(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterRefresh(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAfterScroll(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetAutoCalcFields(const Value: Boolean);
begin
end;

procedure TDriverDataSetBase._SetBeforeCancel(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeDelete(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeEdit(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeInsert(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeOpen(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeClose(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforePost(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeRefresh(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetBeforeScroll(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetOnFilterRecord(const Value: TFilterRecordEvent);
begin
end;

procedure TDriverDataSetBase._SetOnCalcFields(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetOnDeleteError(const Value: TDataSetErrorEvent);
begin
end;

procedure TDriverDataSetBase._SetOnEditError(const Value: TDataSetErrorEvent);
begin
end;

procedure TDriverDataSetBase._SetOnNewRecord(const Value: TDataSetNotifyEvent);
begin
end;

procedure TDriverDataSetBase._SetOnPostError(const Value: TDataSetErrorEvent);
begin
end;

procedure TDriverDataSetBase._SetSortFields(const Value: String);
begin
end;

procedure TDriverDataSetBase._SetBookmark(const Value: TBookmark);
begin
end;

procedure TDriverDataSetBase._SetBlockReadSize(const Value: Integer);
begin
end;

procedure TDriverDataSetBase._SetFieldValue(const FieldName: string; const Value: Variant);
begin
end;

procedure TDriverDataSetBase._SetFetchingAll(const Value: Boolean);
begin
  FFetchingAll := Value;
end;

procedure TDriverDataSetBase._SetFetchOptions(const Value: TFetchOptions);
begin
  FFetchOptions := Value;
  FFetchingAll := (Value.Mode = fmAll);
end;

{ TDriverDataSet<T> }

constructor TDriverDataSet<T>.Create(const ADataSet: T; const AMonitorCallback: TMonitorProc);
begin
  Create;
  FDataSet := ADataSet;
  FMonitorCallback := AMonitorCallback;
  FRecordCount := 0;
  if Assigned(FDataSet) then
  begin
    try
      FRecordCount := FDataSet.RecordCount;
    except
      FRecordCount := 0;
    end;

    if FDataSet.Active then
    begin
      FFetchStopwatch := TStopwatch.StartNew;
      FFetchStarted := True;

      if FDataSet.Eof then
      begin
        FFetchingAll := True;
        FFetchStarted := False;
        FFetchStopwatch.Stop;
        if Assigned(FMonitorCallback) then
          FMonitorCallback(TMonitorParam.Create(teFetchEnd, _GetCommandText, nil, 0, FFetchStopwatch.ElapsedMilliseconds, FRecordCount));
      end;
    end;
  end;
end;

function TDriverDataSet<T>.Designer: TDataSetDesigner;
begin
  Result := FDataSet.Designer;
end;

destructor TDriverDataSet<T>.Destroy;
begin
  FreeAndNil(FDataSet);
  inherited;
end;

function TDriverDataSet<T>.AsDataSet: TDataSet;
begin
  Result := FDataSet;
end;

function TDriverDataSet<T>.Bof: Boolean;
begin
  Result := FDataSet.Bof;
end;

function TDriverDataSet<T>.DataSetField: TDataSetField;
begin
  Result := FDataSet.DataSetField;
end;

function TDriverDataSet<T>.DataSource: TDataSource;
begin
  Result := FDataSet.DataSource;
end;

function TDriverDataSet<T>.AggFields: TFields;
begin
  Result := FDataSet.AggFields;
end;

procedure TDriverDataSet<T>.Append;
begin
  FDataSet.Append;
end;

procedure TDriverDataSet<T>.ApplyUpdates;
begin
end;

procedure TDriverDataSet<T>.Cancel;
begin
  FDataSet.Cancel;
end;

procedure TDriverDataSet<T>.CancelUpdates;
begin
end;

function TDriverDataSet<T>.CanModify: Boolean;
begin
  Result := FDataSet.CanModify;
end;

function TDriverDataSet<T>.CanRefresh: Boolean;
begin
  Result := FDataSet.CanRefresh;
end;

procedure TDriverDataSet<T>.CheckRequiredFields;
var
  LField: TField;
begin
  for LField in FDataSet.Fields do
    if LField.Required and LField.IsNull then
      raise EDatabaseError.CreateFmt('Field %s is required', [LField.FieldName]);
end;

procedure TDriverDataSet<T>.Clear;
begin
  if FDataSet.IsEmpty then
    Exit;
  FDataSet.DisableControls;
  try
    FDataSet.First;
    while not FDataSet.Eof do
      FDataSet.Delete;
  finally
    FDataSet.EnableControls;
  end;
end;

procedure TDriverDataSet<T>.ClearFields;
begin
  FDataSet.ClearFields;
end;

procedure TDriverDataSet<T>.Close;
begin
  FFetchStarted := False;
  FDataSet.Close;
end;

function TDriverDataSet<T>.DefaultFields: Boolean;
begin
  Result := FDataSet.DefaultFields;
end;

procedure TDriverDataSet<T>.Delete;
begin
  FDataSet.Delete;
end;

procedure TDriverDataSet<T>.DisableControls;
begin
  FDataSet.DisableControls;
end;

procedure TDriverDataSet<T>.Edit;
begin
  FDataSet.Edit;
end;

procedure TDriverDataSet<T>.EnableControls;
begin
  FDataSet.EnableControls;
end;

function TDriverDataSet<T>.Eof: Boolean;
begin
  Result := FDataSet.Eof;
end;

function TDriverDataSet<T>.FieldByName(const AFieldName: String): TField;
begin
  Result := FDataSet.FieldByName(AFieldName);
end;

function TDriverDataSet<T>.FieldCount: UInt16;
begin
  Result := FDataSet.FieldCount;
end;

function TDriverDataSet<T>.FieldDefList: TFieldDefList;
begin
  Result := FDataSet.FieldDefList;
end;

function TDriverDataSet<T>.FieldDefs: TFieldDefs;
begin
  Result := FDataSet.FieldDefs;
end;

function TDriverDataSet<T>.FieldList: TFieldList;
begin
  Result := FDataSet.FieldList;
end;

function TDriverDataSet<T>.Fields: TFields;
begin
  Result := FDataSet.Fields;
end;

function TDriverDataSet<T>.FindField(const AFieldName: string): TField;
begin
  Result := FDataSet.FindField(AFieldName);
end;

function TDriverDataSet<T>.FindFirst: Boolean;
begin
  Result := FDataSet.FindFirst;
end;

function TDriverDataSet<T>.FindLast: Boolean;
begin
  Result := FDataSet.FindLast;
end;

function TDriverDataSet<T>.FindNext: Boolean;
begin
  Result := FDataSet.FindNext;
end;

function TDriverDataSet<T>.FindPrior: Boolean;
begin
  Result := FDataSet.FindPrior;
end;

procedure TDriverDataSet<T>.First;
begin
  FDataSet.First;
end;

function TDriverDataSet<T>.Found: Boolean;
begin
  Result := FDataSet.Found;
end;

procedure TDriverDataSet<T>.FreeBookmark(Bookmark: TBookmark);
begin
  FDataSet.FreeBookmark(Bookmark);
end;

procedure TDriverDataSet<T>.Insert;
begin
  FDataSet.Insert;
end;

function TDriverDataSet<T>.IsCachedUpdates: Boolean;
begin
  Result := False;
end;

function TDriverDataSet<T>.IsEmpty: Boolean;
begin
  Result := FDataSet.IsEmpty;
end;

function TDriverDataSet<T>.IsReadOnly: Boolean;
begin
  Result := not FDataSet.CanModify;
end;

function TDriverDataSet<T>.IsUniDirectional: Boolean;
begin
  Result := False;
end;

procedure TDriverDataSet<T>.Last;
begin
  FDataSet.Last;
end;

function TDriverDataSet<T>.Locate(const KeyFields: String; const KeyValues: Variant;
  Options: TLocateOptions): Boolean;
begin
  Result := FDataSet.Locate(KeyFields, KeyValues, Options);
end;

function TDriverDataSet<T>.Lookup(const KeyFields: String; const KeyValues: Variant;
  const ResultFields: String): Variant;
begin
  Result := FDataSet.Lookup(KeyFields, KeyValues, ResultFields);
end;

function TDriverDataSet<T>.Modified: Boolean;
begin
  Result := FDataSet.Modified;
end;

procedure TDriverDataSet<T>.MoveBy(Distance: Integer);
begin
  FDataSet.MoveBy(Distance);
end;

procedure TDriverDataSet<T>.Next;
begin
  FDataSet.Next;
  if FDataSet.Eof and FFetchStarted then
  begin
    FFetchingAll := True;
    FFetchStarted := False;
    FFetchStopwatch.Stop;
    if Assigned(FMonitorCallback) then
      FMonitorCallback(TMonitorParam.Create(teFetchEnd, _GetCommandText, nil, 0, FFetchStopwatch.ElapsedMilliseconds, RecordCount));
  end;
end;

function TDriverDataSet<T>.NormalizeFieldValue(const AFieldName: String; const AValue: Variant): Variant;
begin
  // Implementação padrão: retorna o valor sem modificação
  // Drivers específicos (ex.: FireDAC) podem sobrescrever para normalizar BLOBs, nulos, etc.
  Result := AValue;
end;

function TDriverDataSet<T>.ObjectView: Boolean;
begin
  Result := FDataSet.ObjectView;
end;

procedure TDriverDataSet<T>.Open;
var
  LStopwatch: TStopwatch;
begin
  LStopwatch := TStopwatch.StartNew;
  try
    FDataSet.Open;
    LStopwatch.Stop;
    
    if Assigned(FMonitorCallback) then
      FMonitorCallback(TMonitorParam.Create(teQueryEnd, _GetCommandText, nil, LStopwatch.ElapsedMilliseconds, 0, 0));

    FFetchStopwatch := TStopwatch.StartNew;
    FFetchStarted := True;

    if FDataSet.Eof and FFetchStarted then
    begin
      FFetchingAll := True;
      FFetchStarted := False;
      FFetchStopwatch.Stop;
      if Assigned(FMonitorCallback) then
        FMonitorCallback(TMonitorParam.Create(teFetchEnd, _GetCommandText, nil, 0, FFetchStopwatch.ElapsedMilliseconds, RecordCount));
    end;
  except
    on E: Exception do
    begin
        LStopwatch.Stop;
        if Assigned(FMonitorCallback) then
           FMonitorCallback(TMonitorParam.Create(teError, _GetCommandText, nil, 0, LStopwatch.ElapsedMilliseconds, 0, E));
        raise;
    end;
  end;
end;

procedure TDriverDataSet<T>.Post;
begin
  FDataSet.Post;
end;

procedure TDriverDataSet<T>.Prior;
begin
  FDataSet.Prior;
end;

procedure TDriverDataSet<T>.Refresh;
begin
  FDataSet.Refresh;
end;

function TDriverDataSet<T>.RecNo: Integer;
begin
  Result := FDataSet.RecNo;
end;

function TDriverDataSet<T>.RecordCount: UInt32;
begin
  Result := FDataSet.RecordCount;
end;

function TDriverDataSet<T>.RecordSize: Word;
begin
  Result := FDataSet.RecordSize;
end;

function TDriverDataSet<T>.RowsAffected: UInt32;
begin
  Result := 0;
end;

function TDriverDataSet<T>.SparseArrays: Boolean;
begin
  Result := FDataSet.SparseArrays;
end;

function TDriverDataSet<T>.State: TDataSetState;
begin
  Result := FDataSet.State;
end;

function TDriverDataSet<T>.UpdateStatus: TUpdateStatus;
begin
  Result := FDataSet.UpdateStatus;
end;

function TDriverDataSet<T>._GetActive: Boolean;
begin
  Result := FDataSet.Active;
end;

function TDriverDataSet<T>._GetAfterCancel: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterCancel;
end;

function TDriverDataSet<T>._GetAfterClose: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterClose;
end;

function TDriverDataSet<T>._GetAfterDelete: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterDelete;
end;

function TDriverDataSet<T>._GetAfterEdit: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterEdit;
end;

function TDriverDataSet<T>._GetAfterInsert: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterInsert;
end;

function TDriverDataSet<T>._GetAfterOpen: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterOpen;
end;

function TDriverDataSet<T>._GetAfterPost: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterPost;
end;

function TDriverDataSet<T>._GetAfterRefresh: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterRefresh;
end;

function TDriverDataSet<T>._GetAfterScroll: TDataSetNotifyEvent;
begin
  Result := FDataSet.AfterScroll;
end;

function TDriverDataSet<T>._GetAutoCalcFields: Boolean;
begin
  Result := FDataSet.AutoCalcFields;
end;

function TDriverDataSet<T>._GetBeforeCancel: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeCancel;
end;

function TDriverDataSet<T>._GetBeforeClose: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeClose;
end;

function TDriverDataSet<T>._GetBeforeDelete: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeDelete;
end;

function TDriverDataSet<T>._GetBeforeEdit: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeEdit;
end;

function TDriverDataSet<T>._GetBeforeInsert: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeInsert;
end;

function TDriverDataSet<T>._GetBeforeOpen: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeOpen;
end;

function TDriverDataSet<T>._GetBeforePost: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforePost;
end;

function TDriverDataSet<T>._GetBeforeRefresh: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeRefresh;
end;

function TDriverDataSet<T>._GetBeforeScroll: TDataSetNotifyEvent;
begin
  Result := FDataSet.BeforeScroll;
end;

function TDriverDataSet<T>._GetBlockReadSize: Integer;
begin
  Result := FDataSet.BlockReadSize;
end;

function TDriverDataSet<T>._GetBookmark: TBookmark;
begin
  Result := FDataSet.Bookmark;
end;

function TDriverDataSet<T>._GetCommandText: String;
begin
  Result := FCommandText;
end;

procedure TDriverDataSet<T>._SetCommandText(const ACommandText: String);
begin
  FCommandText := ACommandText;
end;

function TDriverDataSet<T>._GetFieldValue(const FieldName: string): Variant;
begin
  Result := FDataSet.FieldValues[FieldName];
end;

function TDriverDataSet<T>._GetFilter: String;
begin
  Result := FDataSet.Filter;
end;

function TDriverDataSet<T>._GetFiltered: Boolean;
begin
  Result := FDataSet.Filtered;
end;

function TDriverDataSet<T>._GetFilterOptions: TFilterOptions;
begin
  Result := FDataSet.FilterOptions;
end;

function TDriverDataSet<T>._GetOnCalcFields: TDataSetNotifyEvent;
begin
  Result := FDataSet.OnCalcFields;
end;

function TDriverDataSet<T>._GetOnDeleteError: TDataSetErrorEvent;
begin
  Result := FDataSet.OnDeleteError;
end;

function TDriverDataSet<T>._GetOnEditError: TDataSetErrorEvent;
begin
  Result := FDataSet.OnEditError;
end;

function TDriverDataSet<T>._GetOnFilterRecord: TFilterRecordEvent;
begin
  Result := FDataSet.OnFilterRecord;
end;

function TDriverDataSet<T>._GetOnNewRecord: TDataSetNotifyEvent;
begin
  Result := FDataSet.OnNewRecord;
end;

function TDriverDataSet<T>._GetOnPostError: TDataSetErrorEvent;
begin
  Result := FDataSet.OnPostError;
end;

function TDriverDataSet<T>._GetSortFields: String;
begin
  Result := string.Empty;
end;

procedure TDriverDataSet<T>._SetActive(const Value: Boolean);
begin
  FDataSet.Active := Value;
end;

procedure TDriverDataSet<T>._SetAfterCancel(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterCancel := Value;
end;

procedure TDriverDataSet<T>._SetAfterClose(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterClose := Value;
end;

procedure TDriverDataSet<T>._SetAfterDelete(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterDelete := Value;
end;

procedure TDriverDataSet<T>._SetAfterEdit(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterEdit := Value;
end;

procedure TDriverDataSet<T>._SetAfterInsert(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterInsert := Value;
end;

procedure TDriverDataSet<T>._SetAfterOpen(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterOpen := Value;
end;

procedure TDriverDataSet<T>._SetAfterPost(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterPost := Value;
end;

procedure TDriverDataSet<T>._SetAfterRefresh(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterRefresh := Value;
end;

procedure TDriverDataSet<T>._SetAfterScroll(const Value: TDataSetNotifyEvent);
begin
  FDataSet.AfterScroll := Value;
end;

procedure TDriverDataSet<T>._SetAutoCalcFields(const Value: Boolean);
begin
  FDataSet.AutoCalcFields := Value;
end;

procedure TDriverDataSet<T>._SetBeforeCancel(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeCancel := Value;
end;

procedure TDriverDataSet<T>._SetBeforeClose(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeClose := Value;
end;

procedure TDriverDataSet<T>._SetBeforeDelete(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeDelete := Value;
end;

procedure TDriverDataSet<T>._SetBeforeEdit(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeEdit := Value;
end;

procedure TDriverDataSet<T>._SetBeforeInsert(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeInsert := Value;
end;

procedure TDriverDataSet<T>._SetBeforeOpen(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeOpen := Value;
end;

procedure TDriverDataSet<T>._SetBeforePost(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforePost := Value;
end;

procedure TDriverDataSet<T>._SetBeforeRefresh(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeRefresh := Value;
end;

procedure TDriverDataSet<T>._SetBeforeScroll(const Value: TDataSetNotifyEvent);
begin
  FDataSet.BeforeScroll := Value;
end;

procedure TDriverDataSet<T>._SetCachedUpdates(const Value: Boolean);
begin
end;



procedure TDriverDataSet<T>._SetFetchingAll(const Value: Boolean);
begin
  FFetchingAll := Value;
end;

procedure TDriverDataSet<T>._SetFieldValue(const FieldName: string; const Value: Variant);
begin
  FDataSet.FieldValues[FieldName] := Value;
end;

procedure TDriverDataSet<T>._SetFilter(const Value: String);
begin
  FDataSet.Filter := Value;
end;

procedure TDriverDataSet<T>._SetFiltered(const Value: Boolean);
begin
  FDataSet.Filtered := Value;
end;

procedure TDriverDataSet<T>._SetFilterOptions(Value: TFilterOptions);
begin
  FDataSet.FilterOptions := Value;
end;

procedure TDriverDataSet<T>._SetMonitorLog(const ASQL: String; ATransactionName: String;
  const AParams: TParams);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

procedure TDriverDataSet<T>._SetOnCalcFields(const Value: TDataSetNotifyEvent);
begin
  FDataSet.OnCalcFields := Value;
end;

procedure TDriverDataSet<T>._SetOnDeleteError(const Value: TDataSetErrorEvent);
begin
  FDataSet.OnDeleteError := Value;
end;

procedure TDriverDataSet<T>._SetOnEditError(const Value: TDataSetErrorEvent);
begin
  FDataSet.OnEditError := Value;
end;

procedure TDriverDataSet<T>._SetOnFilterRecord(const Value: TFilterRecordEvent);
begin
  FDataSet.OnFilterRecord := Value;
end;

procedure TDriverDataSet<T>._SetOnNewRecord(const Value: TDataSetNotifyEvent);
begin
  FDataSet.OnNewRecord := Value;
end;

procedure TDriverDataSet<T>._SetOnPostError(const Value: TDataSetErrorEvent);
begin
  FDataSet.OnPostError := Value;
end;

procedure TDriverDataSet<T>._SetReadOnly(const Value: Boolean);
begin
end;

procedure TDriverDataSet<T>._SetSortFields(const Value: String);
begin
end;

procedure TDriverDataSet<T>._SetUniDirectional(const Value: Boolean);
begin
end;

{ TOptions }

constructor TOptions.Create;
begin
  FStoreGUIDAsOctet := False;
end;

function TOptions.StoreGUIDAsOctet(const AValue: Boolean): IOptions;
begin
  Result := Self;
  FStoreGUIDAsOctet := AValue;
end;

function TOptions.StoreGUIDAsOctet: Boolean;
begin
  Result := FStoreGUIDAsOctet;
end;

{ TDriverQuery }

constructor TDriverQuery.Create(const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc; const ADriver: TDriverName;
  const ACache: IDBCacheProvider; const AMetadataCache: IDBMetadataCache;
  const ASlowQueryThreshold: Integer);
begin
  inherited Create;
  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FDriver := ADriver;
  FCacheProvider := ACache;
  FMetadataCache := AMetadataCache;
  FRowsAffected := 0;
  FFetchOptions := TFetchOptions.Create(fmAll);
  FSlowQueryThreshold := ASlowQueryThreshold;
end;

destructor TDriverQuery.Destroy;
begin
  FreeAndNil(FParams);
  inherited;
end;

procedure TDriverQuery._Notify(const AEventType: TMonitorEventType; const ACommand: string;
  AParams: TParams; const AExecutionTime: Int64; const AFetchTime: Int64;
  const ARowsAffected: UInt32; const AException: Exception);
var
  LParam: TMonitorParam;
begin
  if not Assigned(FMonitorCallback) then
    Exit;

  LParam := TMonitorParam.Create(AEventType, ACommand, AParams, AExecutionTime, AFetchTime, ARowsAffected, AException);

  // Notify MonitorCallback (which is now the connection's proxy)
  FMonitorCallback(LParam);

  // Check for SlowQuery metric event
  if (AEventType = teQueryEnd) and (AExecutionTime > FSlowQueryThreshold) then
  begin
    FMonitorCallback(TMonitorParam.Create(teMetric, '[SLOW QUERY (' + IntToStr(AExecutionTime) + ' ms)] ' + ACommand, AParams, AExecutionTime));
  end;
end;

procedure TDriverQuery.ExecuteDirect;
var
  LStopwatch: TStopwatch;
begin
  LStopwatch := TStopwatch.StartNew;
  _Notify(teQueryStart, _GetCommandText, _GetParams);
  try
    try
      if Assigned(FCacheProvider) then
        TCacheManager.InvalidateBySQL(FCacheProvider, _GetCommandText, _GetParams, FMonitorCallback);
      _InternalExecuteDirect;
      LStopwatch.Stop;
      _Notify(teQueryEnd, _GetCommandText, _GetParams, LStopwatch.ElapsedMilliseconds, 0, RowsAffected);
    except
      on E: Exception do
      begin
        LStopwatch.Stop;
        _Notify(teError, _GetCommandText, _GetParams, LStopwatch.ElapsedMilliseconds, 0, 0, E);
        raise;
      end;
    end;
  finally
  end;
end;

function TDriverQuery.ExecuteQuery: IDBDataSet;
var
  LKey: string;
  LStopwatch: TStopwatch;
begin
  LStopwatch := TStopwatch.StartNew;
  _Notify(teQueryStart, _GetCommandText, _GetParams);

  if Assigned(FCacheProvider) then
  begin
    LKey := TCacheManager.GenerateQueryHash(_GetCommandText, _GetParams, FDriver);
    Result := FCacheProvider.GetValue(LKey);
    if Assigned(Result) then
    begin
      LStopwatch.Stop;
      Result.CommandText := _GetCommandText;
      _Notify(teMetric, '[CACHE HIT] ' + _GetCommandText, _GetParams, LStopwatch.ElapsedMilliseconds);
      Exit;
    end;
  end;

  try
    try
      Result := _InternalExecuteQuery;
      LStopwatch.Stop;

      _Notify(teQueryEnd, _GetCommandText, _GetParams, LStopwatch.ElapsedMilliseconds, 0, 0);

      if Assigned(FCacheProvider) and Assigned(Result) then
      begin
        _Notify(teMetric, '[CACHE MISS] ' + _GetCommandText, _GetParams, LStopwatch.ElapsedMilliseconds);
        FCacheProvider.SetValue(LKey, TCacheManager.CreateSnapshot(Result), 0, TCacheManager.ExtractTables(_GetCommandText));
      end;
    except
      on E: Exception do
      begin
        LStopwatch.Stop;
        _Notify(teError, _GetCommandText, _GetParams, LStopwatch.ElapsedMilliseconds, 0, 0, E);
        raise;
      end;
    end;
  finally
  end;
end;

function TDriverQuery.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

procedure TDriverQuery.Prepare;
begin
end;

procedure TDriverQuery.Unprepare;
begin
end;

function TDriverQuery.ParamByName(const AValue: string): TParam;
begin
  Result := Params.FindParam(AValue);
  if not Assigned(Result) then
    Result := Params.CreateParam(ftUnknown, AValue, ptInput);
end;

function TDriverQuery._GetCommandText: String;
begin
  Result := string.Empty;
end;

procedure TDriverQuery._SetCommandText(const ACommandText: String);
begin
end;

function TDriverQuery._GetParams: TParams;
begin
  if not Assigned(FParams) then
    FParams := TParams.Create(nil);
  Result := FParams;
end;

procedure TDriverQuery._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
    if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

function TDriverQuery._GetTransactionName: String;
begin
  Result := 'DIRECT';
  if Assigned(FDriverTransaction) and (FDriverTransaction.TransactionActive <> nil) then
    Result := FDriverTransaction.TransactionActive.Name;
end;

function TDriverQuery._TryApplyMetadataCache(const ASQL: string; ADestination: TFieldDefs): Boolean;
var
  LTables: TArray<string>;
  LTable: string;
begin
  Result := False;
  if Assigned(FMetadataCache) then
  begin
    LTables := TCacheManager.ExtractTables(ASQL);
    if Length(LTables) = 1 then
    begin
      LTable := LTables[0];
      Result := TMetadataCacheManager.TryGetMetadata(FMetadataCache, FDriver, LTable, ADestination);
      if Result and Assigned(FMonitorCallback) then
        FMonitorCallback(TMonitorParam.Create('[METADATA HIT] ' + LTable, nil));
    end;
  end;
end;

procedure TDriverQuery._TrySaveMetadataCache(const ASQL: string; ASource: TFieldDefs);
var
  LTables: TArray<string>;
  LTable: string;
begin
  if Assigned(FMetadataCache) then
  begin
    LTables := TCacheManager.ExtractTables(ASQL);
    if Length(LTables) = 1 then
    begin
      LTable := LTables[0];
      TMetadataCacheManager.SaveMetadata(FMetadataCache, FDriver, LTable, ASource);
      if Assigned(FMonitorCallback) then
        FMonitorCallback(TMonitorParam.Create('[METADATA SAVED] ' + LTable, nil));
    end;
  end;
end;

procedure TDriverQuery._RefreshMetadata;
var
  LTables: TArray<string>;
  LTable: string;
begin
  if Assigned(FMetadataCache) then
  begin
    LTables := TCacheManager.ExtractTables(_GetCommandText);
    if Length(LTables) = 1 then
    begin
      LTable := LTables[0];
      FMetadataCache.Evict(TMetadataCacheManager.GenerateKey(LTable, FDriver));
      if Assigned(FMonitorCallback) then
        FMonitorCallback(TMonitorParam.Create('[METADATA REFRESHED] ' + LTable, nil));
    end;
  end;
end;

function TDriverQuery._GetFetchOptions: TFetchOptions;
begin
  Result := FFetchOptions;
end;

procedure TDriverQuery._SetFetchOptions(const Value: TFetchOptions);
begin
  FFetchOptions := Value;
end;

function TDriverQuery._GetSlowQueryThreshold: Integer;
begin
  Result := FSlowQueryThreshold;
end;

procedure TDriverQuery._SetSlowQueryThreshold(const Value: Integer);
begin
  FSlowQueryThreshold := Value;
end;

{ TDriverConnection }

procedure TDriverConnection.Connect;
begin
end;

procedure TDriverConnection.Disconnect;
begin
end;

procedure TDriverConnection.ExecuteDirect(const ASQL: String);
var
  LStopwatch: TStopwatch;
begin
  LStopwatch := TStopwatch.StartNew;
  _InternalNotify(teQueryStart, ASQL, nil);
  try
    try
      _HandleCacheInvalidation(ASQL, nil);
      _InternalExecuteDirect(ASQL, nil);
      LStopwatch.Stop;
      _InternalNotify(teQueryEnd, ASQL, nil, LStopwatch.ElapsedMilliseconds, 0, FRowsAffected);
    except
      on E: Exception do
      begin
        LStopwatch.Stop;
        _InternalNotify(teError, ASQL, nil, LStopwatch.ElapsedMilliseconds, 0, 0, E);
        raise;
      end;
    end;
  finally
  end;
end;

procedure TDriverConnection.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LStopwatch: TStopwatch;
begin
  LStopwatch := TStopwatch.StartNew;
  _InternalNotify(teQueryStart, ASQL, AParams);
  try
    try
      _HandleCacheInvalidation(ASQL, AParams);
      _InternalExecuteDirect(ASQL, AParams);
      LStopwatch.Stop;
      _InternalNotify(teQueryEnd, ASQL, AParams, LStopwatch.ElapsedMilliseconds, 0, FRowsAffected);
    except
      on E: Exception do
      begin
        LStopwatch.Stop;
        _InternalNotify(teError, ASQL, AParams, LStopwatch.ElapsedMilliseconds, 0, 0, E);
        raise;
      end;
    end;
  finally
  end;
end;

procedure TDriverConnection.ExecuteScript(const AScript: String);
begin
end;

procedure TDriverConnection.AddScript(const AScript: String);
begin
end;

procedure TDriverConnection.ExecuteScripts;
begin
end;

procedure TDriverConnection.ApplyUpdates(const ADataSets: array of IDBDataSet);
var
  LDataSet: IDBDataSet;
  LTables: TArray<string>;
  LTable: string;
begin
  if Assigned(FCacheProvider) then
  begin
    for LDataSet in ADataSets do
    begin
      LTables := TCacheManager.ExtractTables(LDataSet.CommandText);
      for LTable in LTables do
        FCacheProvider.InvalidateByTable(LTable);
    end;
  end;
end;

function TDriverConnection.IsConnected: Boolean;
begin
  Result := False;
end;

function TDriverConnection.MonitorCallback: TMonitorProc;
begin
  Result := FMonitorCallback;
end;

function TDriverConnection.Options: IOptions;
begin
  Result := nil;
end;

function TDriverConnection.CreateQuery: IDBQuery;
begin
  Result := nil;
end;

function TDriverConnection.Cache: IDBCacheProvider;
begin
  Result := FCacheProvider;
end;

procedure TDriverConnection.SetCacheProvider(ACache: IDBCacheProvider);
begin
  FCacheProvider := ACache;
end;

function TDriverConnection.MetadataCache: IDBMetadataCache;
begin
  Result := FMetadataCache;
end;

procedure TDriverConnection.SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache);
begin
  FMetadataCache := AMetadataCache;
end;

procedure TDriverConnection.RefreshMetadata(const ATableName: string);
begin
  if Assigned(FMetadataCache) then
    FMetadataCache.Evict(DataEngine.Metadata.Manager.TMetadataCacheManager.GenerateKey(ATableName, FDriver));
end;

function TDriverConnection.TryGetCachedMetadata(const ATableName: string; ADestination: TFieldDefs): Boolean;
begin
  Result := TMetadataCacheManager.TryGetMetadata(FMetadataCache, FDriver, ATableName, ADestination);
end;

function TDriverConnection.BulkLoader: IDBBulkLoader;
begin
  Result := nil;
end;

function TDriverConnection.IsAlive: Boolean;
begin
  Result := _IsAlive;
end;

function TDriverConnection.ResiliencePolicy: IDBResiliencePolicy;
begin
  if not Assigned(FResiliencePolicy) then
    FResiliencePolicy := TDefaultResiliencePolicy.Create;
  Result := FResiliencePolicy;
end;

procedure TDriverConnection.SetResiliencePolicy(APolicy: IDBResiliencePolicy);
begin
  FResiliencePolicy := APolicy;
end;

procedure TDriverConnection.AddObserver(const AObserver: IDBObserver);
begin
  FObserverLock.Enter;
  try
    if not Assigned(FObservers) then
      FObservers := TList<IDBObserver>.Create;
    if not FObservers.Contains(AObserver) then
      FObservers.Add(AObserver);
  finally
    FObserverLock.Leave;
  end;
end;

procedure TDriverConnection.RemoveObserver(const AObserver: IDBObserver);
begin
  FObserverLock.Enter;
  try
    if Assigned(FObservers) then
      FObservers.Remove(AObserver);
  finally
    FObserverLock.Leave;
  end;
end;

function TDriverConnection.SlowQueryThreshold: Integer;
begin
  Result := FSlowQueryThreshold;
end;

procedure TDriverConnection.SetSlowQueryThreshold(const AValue: Integer);
begin
  FSlowQueryThreshold := AValue;
end;

procedure TDriverConnection._Notify(const AParam: TMonitorParam);
var
  LObserver: IDBObserver;
  LTempList: TArray<IDBObserver>;
begin
  FObserverLock.Enter;
  try
    if Assigned(FObservers) and (FObservers.Count > 0) then
      LTempList := FObservers.ToArray
    else
      LTempList := nil;
  finally
    FObserverLock.Leave;
  end;

  if Assigned(LTempList) then
    for LObserver in LTempList do
      LObserver.OnNotify(AParam);
end;

procedure TDriverConnection._InternalNotify(const AEventType: TMonitorEventType;
  const ACommand: string; const AParams: TParams; const AExecutionTime: Int64;
  const AFetchTime: Int64; const ARowsAffected: UInt32; const AException: Exception);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create(AEventType, ACommand, AParams, AExecutionTime, AFetchTime, ARowsAffected, AException));
end;

function TDriverConnection._IsAlive: Boolean;
begin
  Result := IsConnected;
end;

function TDriverConnection.CreateDataSet(const ASQL: String): IDBDataSet;
begin
  Result := nil;
end;

function TDriverConnection.GetSQLScripts: String;
begin
  Result := string.Empty;
end;

constructor TDriverConnection.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriverName: TDriverName;
  const AMonitorCallback: TMonitorProc; const ACache: IDBCacheProvider;
  const AMetadataCache: IDBMetadataCache);
begin
  FDriverTransaction := ADriverTransaction;
  FUserMonitorCallback := AMonitorCallback;
  FDriver := ADriverName;
  FRowsAffected := 0;
  FCacheProvider := ACache;
  FMetadataCache := AMetadataCache;
  FResiliencePolicy := nil;
  FSlowQueryThreshold := DEFAULT_SLOW_QUERY_THRESHOLD;
  FObservers := nil;
  FObserverLock := TCriticalSection.Create;


  FMonitorCallback := procedure(const AParam: TMonitorParam)
    begin
      // 1. Notify observers
      _Notify(AParam);

      // 2. Notify user callback
      if Assigned(FUserMonitorCallback) then
        FUserMonitorCallback(AParam);
    end;
end;

destructor TDriverConnection.Destroy;
begin
  if Assigned(FObservers) then
    FObservers.Free;
  FObserverLock.Free;
  inherited;
end;

function TDriverConnection.GetDriver: TDriverName;
begin
  Result := FDriver;
end;

function TDriverConnection.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

procedure TDriverConnection._SetMonitorLog(const ASQL, ATransactionName: String;
  const AParams: TParams);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

function TDriverConnection._GetTransactionName: String;
begin
  Result := 'DIRECT';
  if Assigned(FDriverTransaction) and (FDriverTransaction.TransactionActive <> nil) then
    Result := FDriverTransaction.TransactionActive.Name;
end;

procedure TDriverConnection._HandleCacheInvalidation(const ASQL: string; const AParams: TParams);
begin
  TCacheManager.InvalidateBySQL(FCacheProvider, ASQL, AParams, FMonitorCallback);
end;

procedure TDriverConnection._UpdateNativeParams(ANativeParams, ASourceParams: TParams);
var
  LFor: Integer;
begin
  if Assigned(ANativeParams) and Assigned(ASourceParams) then
  begin
    for LFor := 0 to ASourceParams.Count - 1 do
    begin
      ANativeParams[LFor].DataType := ASourceParams[LFor].DataType;
      ANativeParams[LFor].Value := ASourceParams[LFor].Value;
    end;
  end;
end;

{ TDriverTransaction }

procedure TDriverTransaction.AddTransaction(const AKey: String; const ATransaction: TComponent);
var
  LKeyUC: String;
begin
  LKeyUC := UpperCase(AKey);
  FLock.Enter;
  try
    if FTransactionList.ContainsKey(LKeyUC) then
      raise Exception.Create('Transaction with the same name already exists.');
    if ATransaction.Name = EmptyStr then
      ATransaction.Name := AKey;
    FTransactionList.Add(LKeyUC, ATransaction);
  finally
    FLock.Leave;
  end;
end;

constructor TDriverTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  FLock := TCriticalSection.Create;
  FMonitorCallback := AMonitorCallback;
  FTransactionList := TDictionary<String, TComponent>.Create;
end;

destructor TDriverTransaction.Destroy;
begin
  FLock.Enter;
  try
    FTransactionActive := nil;
    FTransactionList.Clear;
  finally
    FLock.Leave;
  end;
  FTransactionList.Free;
  FLock.Free;
  inherited;
end;

procedure TDriverTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
end;

procedure TDriverTransaction.Commit;
begin
end;

procedure TDriverTransaction.Rollback;
begin
end;

function TDriverTransaction.InTransaction: Boolean;
begin
  FLock.Enter;
  try
    Result := Assigned(FTransactionActive) and _InTransaction;
  finally
    FLock.Leave;
  end;
end;

function TDriverTransaction._InTransaction: Boolean;
begin
  Result := False;
end;

function TDriverTransaction.TransactionActive: TComponent;
begin
  FLock.Enter;
  try
    Result := FTransactionActive;
  finally
    FLock.Leave;
  end;
end;

procedure TDriverTransaction.UseTransaction(const AKey: String);
var
  LKeyUC: String;
begin
  LKeyUC := UpperCase(AKey);
  FLock.Enter;
  try
    if not FTransactionList.TryGetValue(LKeyUC, FTransactionActive) then
      raise Exception.Create('Transaction not found.');
  finally
    FLock.Leave;
  end;
end;

function TDriverTransaction._GetTransaction(const AKey: String): TComponent;
var
  LKeyUC: String;
begin
  LKeyUC := UpperCase(AKey);
  FLock.Enter;
  try
    if not FTransactionList.TryGetValue(LKeyUC, Result) then
      raise Exception.Create('Transaction not found.');
  finally
    FLock.Leave;
  end;
end;

{ TDefaultResiliencePolicy }

procedure TDefaultResiliencePolicy.ExecuteDelay(const AAttempt: Integer);
begin
  Sleep(GetDelay);
end;

function TDefaultResiliencePolicy.GetDelay: Integer;
begin
  Result := DEFAULT_RETRY_DELAY;
end;

function TDefaultResiliencePolicy.GetOnRetry: TResilienceEvent;
begin
  Result := FOnRetry;
end;

function TDefaultResiliencePolicy.GetRetryCount: Integer;
begin
  Result := DEFAULT_RETRY_COUNT;
end;

procedure TDefaultResiliencePolicy.SetOnRetry(const AEvent: TResilienceEvent);
begin
  FOnRetry := AEvent;
end;

function TDefaultResiliencePolicy.ShouldRetry(const AException: Exception;
  const AAttempt: Integer): Boolean;
begin
  Result := AAttempt < GetRetryCount;
end;

end.

