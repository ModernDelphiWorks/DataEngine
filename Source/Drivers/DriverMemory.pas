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

unit DriverMemory;

interface

uses
  DB,
  Math,
  Classes,
  SysUtils,
  StrUtils,
  Variants,
  Generics.Collections,
  DriverConnection,
  FactoryInterfaces,
  System.Fluent,
  System.Fluent.Collections,
  System.Fluent.Helpers;

type
  TMemoryRecord = class;
  TMemoryConnection = class;

  IEntityCollection<T: class> = interface
    ['{C58080A0-5196-412E-9ABD-356AD42CEADF}']
    procedure Add(const AEntity: T);
    function AsEnumerable: IFluentEnumerable<T>;
    function Count: Integer;
  end;

  TEntityCollectionAdapter<T: class> = class(TInterfacedObject, IEntityCollection<T>)
  private
    FList: TFluentList<T>;
  public
    constructor Create(const AList: TFluentList<T>);
    destructor Destroy; override;
    procedure Add(const AEntity: T);
    function AsEnumerable: IFluentEnumerable<T>;
    function Count: Integer;
  end;

  TJoins = record
    TableName: string;
    Join: string;
    JoinCondition: string;
    Where: string;
  end;

  TJoinFields = record
    LeftField: string;
    RightField: string;
  end;

  TJoinConditionParser = class
  private
    FMonitorCallback: TMonitorProc;
    procedure _SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
  public
    constructor Create(const AMonitorCallback: TMonitorProc);
    function Parse(const AJoinCondition: string): TJoinFields;
  end;

  TJoinExecutor = class
  private
    FMonitorCallback: TMonitorProc;
    procedure _SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
    function _ExtractFieldName(const FullFieldName: string): string;
  public
    constructor Create(const AMonitorCallback: TMonitorProc);
    function ExecuteJoin(const ATable, AJoinTable: IEntityCollection<TMemoryRecord>;
      const ATableName, AJoinTableName: string; const AJoinFields: TJoinFields): TFluentList<TMemoryRecord>;
  end;

  TWhereFilter = class
  private
    FMonitorCallback: TMonitorProc;
    procedure _SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
  public
    constructor Create(const AMonitorCallback: TMonitorProc);
    function ApplyWhere(const ARecords: TFluentList<TMemoryRecord>; const AWhere: string): TFluentList<TMemoryRecord>;
  end;

  ISqlParser = interface
    ['{FA1CA4A8-803D-4D70-BB80-EEE60231CD26}']
    function ParseSelect(const ASQL: string): TJoins;
  end;

  IQueryExecutor = interface
    ['{EA32C54B-11F0-479F-A98F-EA70C5B696D6}']
    function ExecuteSelect(const ATable: IEntityCollection<TMemoryRecord>;
      const AJoinTable: IEntityCollection<TMemoryRecord>;
      const ATableName, AJoinTableName, AJoinCondition, AWhere: string): TFluentList<TMemoryRecord>;
  end;

  TSqlParser = class(TInterfacedObject, ISqlParser)
  private
    FCommandMonitor: ICommandMonitor;
    FMonitorCallback: TMonitorProc;
    procedure _SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
  public
    constructor Create(const AMonitorCallback: TMonitorProc);
    function ParseSelect(const ASQL: string): TJoins;
  end;

  TQueryExecutor = class(TInterfacedObject, IQueryExecutor)
  private
    FJoinConditionParser: TJoinConditionParser;
    FJoinExecutor: TJoinExecutor;
    FWhereFilter: TWhereFilter;
    FCommandMonitor: ICommandMonitor;
    FMonitorCallback: TMonitorProc;
    procedure _SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
  public
    constructor Create(const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    function ExecuteSelect(const ATable: IEntityCollection<TMemoryRecord>;
      const AJoinTable: IEntityCollection<TMemoryRecord>;
      const ATableName, AJoinTableName, AJoinCondition, AWhere: string): TFluentList<TMemoryRecord>;
  end;

  TMemoryRecord = class
  private
    FFields: TFluentDictionary<string, Variant>;
  public
    constructor Create;
    destructor Destroy; override;
    function Clone: TMemoryRecord;
    property Fields: TFluentDictionary<string, Variant> read FFields;
  end;

  // ---------------------------------------------------------------------------
  // TMemoryConnection (Component)
  // ---------------------------------------------------------------------------
  TMemoryConnection = class(TComponent)
  private
    FTables: TFluentDictionary<string, TFluentList<TMemoryRecord>>;
    FConnected: Boolean;
    FInTransaction: Boolean;
    FSqlParser: ISqlParser;
    FQueryExecutor: IQueryExecutor;
    FMonitorCallback: TMonitorProc;
    FRowsAffected: UInt32;
    FBackupTables: TFluentDictionary<string, TFluentList<TMemoryRecord>>;
    procedure _SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
    procedure _ParseAndExecuteSQL(const ASQL: string; const AParams: TParams = nil);
    function _ExecuteSelect(const ASQL: string; const AParams: TParams): TFluentList<TMemoryRecord>;
    procedure _ExecuteInsert(const ASQL: string; const AParams: TParams);
    procedure _ExecuteUpdate(const ASQL: string; const AParams: TParams);
    procedure _ExecuteDelete(const ASQL: string; const AParams: TParams);
    procedure _ClearTables(ATables: TFluentDictionary<string, TFluentList<TMemoryRecord>>);
    procedure _SnapshotTables;
    procedure _RestoreTables;
    function _ParseValues(const AValueString: string): TArray<string>;
    function _ParseValue(const AValue: string): Variant;
  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;
    procedure Connect;
    procedure Disconnect;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;
    function InTransaction: Boolean;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams);
    // Returns a List of records, not a DataSet. DataSet is handled by TMemoryDataSet.
    function ExecuteSelect(const ASQL: String; const AParams: TParams): TFluentList<TMemoryRecord>;
    
    property Connected: Boolean read FConnected;
    property RowsAffected: UInt32 read FRowsAffected;
    property MonitorCallback: TMonitorProc read FMonitorCallback write FMonitorCallback;
  end;

  // ---------------------------------------------------------------------------
  // TMemoryDataSet (Component inheriting from TDataSet)
  // ---------------------------------------------------------------------------
  TMemoryDataSet = class(TDataSet)
  private
    FConnection: TMemoryConnection;
    FSQL: TStrings;
    FParams: TParams;
    FRecords: TFluentList<TMemoryRecord>;
    FCurrentIndex: Integer;
    FIsOpen: Boolean;
    procedure SetConnection(const Value: TMemoryConnection);
    procedure SetSQL(const Value: TStrings);
    procedure SetParams(const Value: TParams);
  protected
    // TDataSet abstract methods
    function GetRecord(Buffer: TRecordBuffer; GetMode: TGetMode; DoCheck: Boolean): TGetResult; override;
    function GetRecordCount: Integer; override;
    function GetRecNo: Integer; override;
    procedure InternalInitFieldDefs; override;
    procedure InternalOpen; override;
    procedure InternalClose; override;
    procedure InternalHandleException; override;
    procedure InternalGotoBookmark(Bookmark: TBookmark); override;
    procedure InternalSetToRecord(Buffer: TRecordBuffer); override;
    function IsCursorOpen: Boolean; override;
    procedure SetRecNo(Value: Integer); override;
    function AllocRecordBuffer: TRecordBuffer; override;
    procedure FreeRecordBuffer(var Buffer: TRecordBuffer); override;
    procedure GetBookmarkData(Buffer: TRecordBuffer; Data: TBookmark); override;
    function GetBookmarkFlag(Buffer: TRecordBuffer): TBookmarkFlag; override;
    procedure SetBookmarkFlag(Buffer: TRecordBuffer; Value: TBookmarkFlag); override;
    procedure SetFieldData(Field: TField; Buffer: TValueBuffer); overload; override;
    
    // Modern Delphi GetFieldData overload
    function GetFieldData(Field: TField; var Buffer: TValueBuffer): Boolean; overload; override;
    // Older Delphi / Generic overload compatibility
    function GetFieldData(Field: TField; Buffer: TValueBuffer): Boolean; overload; override;
  private
    function _VarTypeToFieldType(const AVarType: Word): TFieldType;
    function _GetFieldSize(const AFieldType: TFieldType; const AValue: Variant): Integer;
  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;
    
    procedure Execute; // Executes SQL without returning cursor (INSERT/UPDATE/DELETE)
    
    property Connection: TMemoryConnection read FConnection write SetConnection;
    property SQL: TStrings read FSQL write SetSQL;
    property Params: TParams read FParams write SetParams;
  end;

  // ---------------------------------------------------------------------------
  // Drivers (Adapters)
  // ---------------------------------------------------------------------------

  TDriverMemory = class(TDriverConnection)
  private
    FConnection: TMemoryConnection;
    FOwnsConnection: Boolean;
    function _GetTransactionActive: TComponent;
  public
    constructor Create(const AConnection: TComponent; const ADriverTransaction: TDriverTransaction;
      const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure ExecuteDirect(const ASQL: String); overload; override;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); overload; override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryMemory = class(TDriverQuery)
  private
    FDataSet: TMemoryDataSet;
    function _GetTransactionActive: TComponent;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TMemoryConnection; const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  // Inherits from TDriverDataSet<T> where T is TMemoryDataSet
  TDriverDataSetMemory = class(TDriverDataSet<TMemoryDataSet>)
  protected
    function _GetCommandText: String; override;
    procedure _SetCommandText(const ACommandText: String); override;
    function RowsAffected: UInt32; override;
  public
    constructor Create(const ADataSet: TMemoryDataSet; const AMonitorCallback: TMonitorProc);
  end;

implementation

{ TEntityCollectionAdapter<T> }

constructor TEntityCollectionAdapter<T>.Create(const AList: TFluentList<T>);
begin
  inherited Create;
  FList := AList;
end;

destructor TEntityCollectionAdapter<T>.Destroy;
begin
  FList := nil;
  inherited;
end;

procedure TEntityCollectionAdapter<T>.Add(const AEntity: T);
begin
  if Assigned(FList) then
    FList.Add(AEntity);
end;

function TEntityCollectionAdapter<T>.AsEnumerable: IFluentEnumerable<T>;
begin
  if Assigned(FList) then
    Result := FList.AsEnumerable
  else
    Result := TFluentList<T>.Create.AsEnumerable;
end;

function TEntityCollectionAdapter<T>.Count: Integer;
begin
  if Assigned(FList) then
    Result := FList.Count
  else
    Result := 0;
end;

{ TJoinConditionParser }

constructor TJoinConditionParser.Create(const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
end;

procedure TJoinConditionParser._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

function TJoinConditionParser.Parse(const AJoinCondition: string): TJoinFields;
begin
  _SetMonitorLog(Format('Parsing join condition: %s', [AJoinCondition]), '', nil);
  Result.LeftField := Trim(Copy(AJoinCondition, 1, Pos('=', AJoinCondition) - 1));
  if Pos('.', Result.LeftField) > 0 then
    Result.LeftField := Copy(Result.LeftField, Pos('.', Result.LeftField) + 1);
  Result.RightField := Trim(Copy(AJoinCondition, Pos('=', AJoinCondition) + 1, Length(AJoinCondition)));
  if Pos('.', Result.RightField) > 0 then
    Result.RightField := Copy(Result.RightField, Pos('.', Result.RightField) + 1);
  _SetMonitorLog(Format('Parsed join: Left=%s, Right=%s', [Result.LeftField, Result.RightField]), '', nil);
end;

{ TJoinExecutor }

constructor TJoinExecutor.Create(const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
end;

function TJoinExecutor.ExecuteJoin(const ATable, AJoinTable: IEntityCollection<TMemoryRecord>;
  const ATableName, AJoinTableName: string; const AJoinFields: TJoinFields): TFluentList<TMemoryRecord>;
var
  LResult: TFluentList<TMemoryRecord>;
  LNewRecord: TMemoryRecord;
  LPair: TPair<string, Variant>;
  LLeftValue, LRightValue: Variant;
  LLeft, LRight: TMemoryRecord;
begin
  LResult := TFluentList<TMemoryRecord>.Create;
  try
    if (ATable = nil) or (AJoinTable = nil) then
    begin
      _SetMonitorLog(Format('Join failed: ATable=%s, AJoinTable=%s',
        [IfThen(ATable = nil, 'nil', 'not nil'), IfThen(AJoinTable = nil, 'nil', 'not nil')]), '', nil);
      Exit(LResult);
    end;

    _SetMonitorLog(Format('Join condition: %s = %s', [AJoinFields.LeftField, AJoinFields.RightField]), '', nil);

    for LLeft in ATable.AsEnumerable do
    begin
      if (LLeft = nil) or (not LLeft.Fields.TryGetValue(AJoinFields.LeftField, LLeftValue)) then
      begin
        _SetMonitorLog(Format('Skipping LLeft: nil or %s not found', [AJoinFields.LeftField]), '', nil);
        Continue;
      end;
      for LRight in AJoinTable.AsEnumerable do
      begin
        if (LRight = nil) or (not LRight.Fields.TryGetValue(AJoinFields.RightField, LRightValue)) then
        begin
          _SetMonitorLog(Format('Skipping LRight: nil or %s not found', [AJoinFields.RightField]), '', nil);
          Continue;
        end;
        if VarToStr(LLeftValue) = VarToStr(LRightValue) then
        begin
          LNewRecord := TMemoryRecord.Create;
          try
            for LPair in LLeft.Fields do
              LNewRecord.Fields.Add(ATableName + '.' + LPair.Key, LPair.Value);
            for LPair in LRight.Fields do
              LNewRecord.Fields.Add(AJoinTableName + '.' + LPair.Key, LPair.Value);
            LResult.Add(LNewRecord);
            _SetMonitorLog(Format('Join match: %s=%s, %s=%s',
              [AJoinFields.LeftField, VarToStr(LLeftValue), AJoinFields.RightField, VarToStr(LRightValue)]), '', nil);
          except
            LNewRecord.Free;
            raise;
          end;
        end;
      end;
    end;
  finally
    Result := LResult;
  end;
end;

function TJoinExecutor._ExtractFieldName(const FullFieldName: string): string;
begin
  Result := FullFieldName.Substring(FullFieldName.LastIndexOf('.') + 1);
end;

procedure TJoinExecutor._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

{ TWhereFilter }

constructor TWhereFilter.Create(const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
end;

procedure TWhereFilter._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

function TWhereFilter.ApplyWhere(const ARecords: TFluentList<TMemoryRecord>; const AWhere: string): TFluentList<TMemoryRecord>;
var
  LResult: TFluentList<TMemoryRecord>;
  LWhereParts: TArray<string>;
begin
  LResult := TFluentList<TMemoryRecord>.Create;
  try
    if (ARecords = nil) or (AWhere = '') then
    begin
      if ARecords <> nil then
        LResult.AddRange(ARecords.ToArray);
      Exit(LResult);
    end;

    LWhereParts := AWhere.Split([' AND ']);
    LResult.AddRange(
      ARecords.AsEnumerable.Where(
        function(R: TMemoryRecord): Boolean
        var
          LWhere: string;
          LMatch: Boolean;
          LVariant: Variant;
          LTryVariant: Variant;
          LWhereCondition, LField, LValue, LOperator: string;
        begin
          LMatch := True;
          for LWhere in LWhereParts do
          begin
            LWhereCondition := Trim(LWhere);
            if Pos('LIKE', UpperCase(LWhereCondition)) > 0 then
            begin
              LField := Trim(Copy(LWhereCondition, 1, Pos('LIKE', UpperCase(LWhereCondition)) - 1));
              LValue := Trim(Copy(LWhereCondition, Pos('LIKE', UpperCase(LWhereCondition)) + 5, Length(LWhereCondition)));
              LValue := StringReplace(LValue, '''', '', [rfReplaceAll]);
              LValue := UpperCase(LValue);
              if R.Fields.TryGetValue(LField, LTryVariant) then
              begin
                LVariant := LTryVariant;
                if LValue.StartsWith('%') and LValue.EndsWith('%') then
                  LMatch := LMatch and (Pos(UpperCase(LValue.Trim(['%'])), UpperCase(VarToStr(LVariant))) > 0)
                else if LValue.EndsWith('%') then
                  LMatch := LMatch and UpperCase(LVariant).StartsWith(UpperCase(LValue.Trim(['%'])))
                else
                  LMatch := LMatch and (UpperCase(LVariant) = UpperCase(LValue));
                _SetMonitorLog(Format('WHERE LIKE: %s LIKE %s, Match=%s',
                  [LField, LValue, BoolToStr(LMatch, True)]), '', nil);
              end
              else
              begin
                LMatch := False;
                _SetMonitorLog(Format('WHERE LIKE: %s not found', [LField]), '', nil);
              end;
            end
            else
            begin
              LOperator := IfThen(Pos('=', LWhereCondition) > 0, '=', IfThen(Pos('>', LWhereCondition) > 0, '>', '<'));
              LField := Trim(Copy(LWhereCondition, 1, Pos(LOperator, LWhereCondition) - 1));
              LValue := Trim(Copy(LWhereCondition, Pos(LOperator, LWhereCondition) + 1, Length(LWhereCondition)));
              LValue := StringReplace(LValue, '''', '', [rfReplaceAll]);
              LValue := StringReplace(LValue, '.', ',', [rfReplaceAll]);
              LValue := UpperCase(LValue);
              if R.Fields.TryGetValue(LField, LTryVariant) then
              begin
                LVariant := StringReplace(LTryVariant, '.', ',', [rfReplaceAll]);
                if LOperator = '>' then
                  LMatch := LMatch and (StrToFloatDef(LVariant, 0) > StrToFloatDef(LValue, 0))
                else if LOperator = '=' then
                  LMatch := LMatch and (LVariant = LValue);
                _SetMonitorLog(Format('WHERE %s: %s %s %s, Match=%s',
                  [LField, VarToStr(LVariant), LOperator, LValue, BoolToStr(LMatch, True)]), '', nil);
              end
              else
              begin
                LMatch := False;
                _SetMonitorLog(Format('WHERE %s: %s not found', [LField]), '', nil);
              end;
            end;
          end;
          Result := LMatch;
        end).ToArray
      );
    _SetMonitorLog(Format('WHERE result count: %d', [LResult.Count]), '', nil);
  finally
    Result := LResult;
  end;
end;

{ TSqlParser }

constructor TSqlParser.Create(const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
end;

procedure TSqlParser._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
  if Assigned(FCommandMonitor) then
    FCommandMonitor.Command('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams);
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

function TSqlParser.ParseSelect(const ASQL: string): TJoins;
begin
  _SetMonitorLog(Format('Parsing SQL: %s', [ASQL]), '', nil);
  Result.TableName := Trim(Copy(ASQL, Pos('FROM', UpperCase(ASQL)) + 5, Pos(' ', ASQL + ' ', Pos('FROM', UpperCase(ASQL)) + 5) - Pos('FROM', UpperCase(ASQL)) - 5));
  Result.Where := IfThen(Pos('WHERE', UpperCase(ASQL)) > 0, Trim(Copy(ASQL, Pos('WHERE', UpperCase(ASQL)) + 6, Length(ASQL))), '');
  Result.Join := IfThen(Pos('INNER JOIN', UpperCase(ASQL)) > 0, Trim(Copy(ASQL, Pos('INNER JOIN', UpperCase(ASQL)) + 10, Pos(' ON ', UpperCase(ASQL)) - Pos('INNER JOIN', UpperCase(ASQL)) - 10)), '');
  Result.JoinCondition := IfThen(Result.Join <> '', Trim(Copy(ASQL, Pos(' ON ', UpperCase(ASQL)) + 4, Pos(' WHERE ', ASQL + ' ') - Pos(' ON ', UpperCase(ASQL)) - 4)), '');
  _SetMonitorLog(Format('Parsed: Table=%s, Join=%s, Condition=%s, Where=%s',
    [Result.TableName, Result.Join, Result.JoinCondition, Result.Where]), '', nil);
end;

{ TQueryExecutor }

constructor TQueryExecutor.Create(const AMonitorCallback: TMonitorProc);
begin
  inherited Create;
  FMonitorCallback := AMonitorCallback;
  FJoinConditionParser := TJoinConditionParser.Create(AMonitorCallback);
  FJoinExecutor := TJoinExecutor.Create(AMonitorCallback);
  FWhereFilter := TWhereFilter.Create(AMonitorCallback);
end;

destructor TQueryExecutor.Destroy;
begin
  FJoinConditionParser.Free;
  FJoinExecutor.Free;
  FWhereFilter.Free;
  inherited;
end;

function TQueryExecutor.ExecuteSelect(const ATable: IEntityCollection<TMemoryRecord>;
  const AJoinTable: IEntityCollection<TMemoryRecord>;
  const ATableName, AJoinTableName, AJoinCondition, AWhere: string): TFluentList<TMemoryRecord>;
var
  LResult: TFluentList<TMemoryRecord>;
  LJoinFields: TJoinFields;
  LTempList: TFluentList<TMemoryRecord>;
  LRecord: TMemoryRecord;
  LIndex: NativeInt;
begin
  try
    if (AJoinTable <> nil) and (AJoinCondition <> '') then
    begin
      LJoinFields := FJoinConditionParser.Parse(AJoinCondition);
      LTempList := FJoinExecutor.ExecuteJoin(ATable, AJoinTable, ATableName, AJoinTableName, LJoinFields);
      try
        LResult := FWhereFilter.ApplyWhere(LTempList, AWhere);
      finally
        for LIndex := LTempList.Count - 1 downto 0 do
        begin
          if LResult.IndexOf(LTempList[LIndex]) = -1 then
          begin
            LTempList[LIndex].Free;
            LTempList.Delete(LIndex);
          end;
        end;
        LTempList.Free;
      end;
    end
    else
    begin
      LResult := TFluentList<TMemoryRecord>.Create;
      if ATable <> nil then
      begin
        for LRecord in ATable.AsEnumerable do
        begin
          if LRecord <> nil then
          begin
            LResult.Add(LRecord.Clone);
          end;
        end;
      end;
      LTempList := FWhereFilter.ApplyWhere(LResult, AWhere);
      try
        for LIndex := LResult.Count - 1 downto 0 do
        begin
          if LTempList.IndexOf(LResult[LIndex]) = -1 then
          begin
            LResult[LIndex].Free;
            LResult.Delete(LIndex);
          end;
        end;
      finally
        LTempList.Free;
      end;
    end;
    _SetMonitorLog(Format('Select result count: %d', [LResult.Count]), '', nil);
  finally
    Result := LResult;
  end;
end;

procedure TQueryExecutor._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
  if Assigned(FCommandMonitor) then
    FCommandMonitor.Command('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams);
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

{ TMemoryRecord }

constructor TMemoryRecord.Create;
begin
  FFields := TFluentDictionary<string, Variant>.Create;
end;

destructor TMemoryRecord.Destroy;
begin
  FFields.Clear;
  FFields.Free;
  inherited;
end;

function TMemoryRecord.Clone: TMemoryRecord;
var
  LPair: TPair<string, Variant>;
begin
  Result := TMemoryRecord.Create;
  for LPair in FFields do
    Result.Fields.Add(LPair.Key, LPair.Value);
end;

{ TMemoryConnection }

constructor TMemoryConnection.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  FTables := TFluentDictionary<string, TFluentList<TMemoryRecord>>.Create([doOwnsValues]);
  FBackupTables := TFluentDictionary<string, TFluentList<TMemoryRecord>>.Create([doOwnsValues]);
  FConnected := False;
  FRowsAffected := 0;
  FSqlParser := TSqlParser.Create(FMonitorCallback);
  FQueryExecutor := TQueryExecutor.Create(FMonitorCallback);
end;

destructor TMemoryConnection.Destroy;
begin
  _ClearTables(FTables);
  _ClearTables(FBackupTables);
  FTables.Free;
  FBackupTables.Free;
  inherited;
end;

procedure TMemoryConnection.Connect;
begin
  FConnected := True;
  _SetMonitorLog('Connected to Memory DB', '', nil);
end;

procedure TMemoryConnection.Disconnect;
begin
  FConnected := False;
  _SetMonitorLog('Disconnected from Memory DB', '', nil);
end;

procedure TMemoryConnection.StartTransaction;
begin
  if FInTransaction then
    raise Exception.Create('Transaction already active.');
  FInTransaction := True;
  _SnapshotTables;
end;

procedure TMemoryConnection.Commit;
begin
  if not FInTransaction then
    raise Exception.Create('No active transaction to commit.');
  FInTransaction := False;
  _ClearTables(FBackupTables); // Clear backup, keeping changes
end;

procedure TMemoryConnection.Rollback;
begin
  if not FInTransaction then
    raise Exception.Create('No active transaction to rollback.');
  FInTransaction := False;
  _RestoreTables; // Restore from backup
end;

function TMemoryConnection.InTransaction: Boolean;
begin
  Result := FInTransaction;
end;

procedure TMemoryConnection._SnapshotTables;
var
  LTablePair: TPair<string, TFluentList<TMemoryRecord>>;
  LNewTable: TFluentList<TMemoryRecord>;
  LRecord: TMemoryRecord;
begin
  _ClearTables(FBackupTables);
  for LTablePair in FTables do
  begin
    LNewTable := TFluentList<TMemoryRecord>.Create;
    for LRecord in LTablePair.Value do
      LNewTable.Add(LRecord.Clone);
    FBackupTables.Add(LTablePair.Key, LNewTable);
  end;
end;

procedure TMemoryConnection._RestoreTables;
var
  LTablePair: TPair<string, TFluentList<TMemoryRecord>>;
  LNewTable: TFluentList<TMemoryRecord>;
  LRecord: TMemoryRecord;
begin
  _ClearTables(FTables);
  for LTablePair in FBackupTables do
  begin
    LNewTable := TFluentList<TMemoryRecord>.Create;
    for LRecord in LTablePair.Value do
      LNewTable.Add(LRecord.Clone);
    FTables.Add(LTablePair.Key, LNewTable);
  end;
  _ClearTables(FBackupTables);
end;

procedure TMemoryConnection.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LSQL: string;
  LFor: Integer;
begin
  if not FConnected then
    Connect;

  LSQL := ASQL;
  if Assigned(AParams) then
  begin
    for LFor := 0 to AParams.Count - 1 do
    begin
      if AParams.Items[LFor].DataType in [ftString, ftMemo, ftWideString] then
        LSQL := StringReplace(LSQL, ':' + AParams.Items[LFor].Name, QuotedStr(VarToStr(AParams.Items[LFor].Value)), [rfReplaceAll])
      else
        LSQL := StringReplace(LSQL, ':' + AParams.Items[LFor].Name, VarToStr(AParams.Items[LFor].Value), [rfReplaceAll]);
    end;
  end;
  _SetMonitorLog(Format('Executing SQL: %s', [LSQL]), '', nil);
  _ParseAndExecuteSQL(LSQL, AParams);
end;

function TMemoryConnection.ExecuteSelect(const ASQL: String; const AParams: TParams): TFluentList<TMemoryRecord>;
begin
  if not FConnected then
    Connect;
  Result := _ExecuteSelect(ASQL, AParams);
end;

procedure TMemoryConnection._SetMonitorLog(const ASQL, ATransactionName: String; const AParams: TParams);
begin
  if Assigned(FMonitorCallback) then
    FMonitorCallback(TMonitorParam.Create('[Transaction: ' + ATransactionName + '] - ' + TrimRight(ASQL), AParams));
end;

procedure TMemoryConnection._ParseAndExecuteSQL(const ASQL: string; const AParams: TParams);
var
  LSQL: string;
  LSelectResult: TFluentList<TMemoryRecord>;
begin
  LSQL := UpperCase(Trim(ASQL));

  if LSQL.StartsWith('SELECT') then
  begin
    LSelectResult := _ExecuteSelect(LSQL, AParams);
    if Assigned(LSelectResult) then
      LSelectResult.Free; // Direct execution of select ignores result
  end
  else if LSQL.StartsWith('INSERT INTO') then
    _ExecuteInsert(LSQL, AParams)
  else if LSQL.StartsWith('UPDATE') then
    _ExecuteUpdate(LSQL, AParams)
  else if LSQL.StartsWith('DELETE') then
    _ExecuteDelete(LSQL, AParams);
end;

function TMemoryConnection._ExecuteSelect(const ASQL: string; const AParams: TParams): TFluentList<TMemoryRecord>;
var
  LParseResult: TJoins;
  LTable: TFluentList<TMemoryRecord>;
  LJoinTable: TFluentList<TMemoryRecord>;
  LTableAdapter: IEntityCollection<TMemoryRecord>;
  LJoinTableAdapter: IEntityCollection<TMemoryRecord>;
begin
  LParseResult := FSqlParser.ParseSelect(ASQL);
  LTable := nil;
  LJoinTable := nil;

  if FTables.TryGetValue(LParseResult.TableName, LTable) then
  begin
    LTableAdapter := TEntityCollectionAdapter<TMemoryRecord>.Create(LTable);
    try
      if LParseResult.Join <> '' then
      begin
        if FTables.TryGetValue(LParseResult.Join, LJoinTable) then
          LJoinTableAdapter := TEntityCollectionAdapter<TMemoryRecord>.Create(LJoinTable)
        else
          LJoinTableAdapter := TEntityCollectionAdapter<TMemoryRecord>.Create(nil);
      end
      else
        LJoinTableAdapter := TEntityCollectionAdapter<TMemoryRecord>.Create(nil);

      try
        Result := FQueryExecutor.ExecuteSelect(
          LTableAdapter,
          LJoinTableAdapter,
          LParseResult.TableName,
          LParseResult.Join,
          LParseResult.JoinCondition,
          LParseResult.Where
        );
      finally
        LJoinTableAdapter := nil;
      end;
    finally
      LTableAdapter := nil;
    end;
  end
  else
  begin
    Result := TFluentList<TMemoryRecord>.Create;
  end;
end;

procedure TMemoryConnection._ExecuteInsert(const ASQL: string; const AParams: TParams);
var
  LTableName: string;
  LFields: TArray<string>;
  LValues: TArray<string>;
  LRecord: TMemoryRecord;
  LFor: Integer;
  LField: string;
  LValueStr: string;
  LValue: Variant;
  LTable: TFluentList<TMemoryRecord>;
begin
  LTableName := Trim(Copy(ASQL, Pos('INTO', UpperCase(ASQL)) + 5, Pos('(', ASQL) - Pos('INTO', UpperCase(ASQL)) - 5));
  if not FTables.TryGetValue(LTableName, LTable) then
  begin
    LTable := TFluentList<TMemoryRecord>.Create;
    FTables.Add(LTableName, LTable);
  end;

  LRecord := TMemoryRecord.Create;
  try
    LFields := Copy(ASQL, Pos('(', ASQL) + 1, Pos(')', ASQL) - Pos('(', ASQL) - 1).Split([',']);
    LValueStr := Copy(ASQL, Pos('VALUES', UpperCase(ASQL)) + 7, Pos(')', ASQL, Pos('VALUES', UpperCase(ASQL))) - Pos('VALUES', UpperCase(ASQL)) - 7);
    LValues := _ParseValues(LValueStr);

    for LFor := 0 to Min(Length(LFields), Length(LValues)) - 1 do
    begin
      LField := Trim(LFields[LFor]);
      LValueStr := Trim(LValues[LFor]);
      LValue := _ParseValue(LValueStr);
      
      LRecord.Fields.Add(LField, LValue);
      _SetMonitorLog(Format('Insert %s: %s', [LField, VarToStr(LValue)]), '', nil);
    end;
    LTable.Add(LRecord);
    FRowsAffected := 1;
    _SetMonitorLog(Format('Inserted record in %s, count = %d', [LTableName, LTable.Count]), '', nil);
  except
    LRecord.Free;
    raise;
  end;
end;

procedure TMemoryConnection._ExecuteUpdate(const ASQL: string; const AParams: TParams);
var
  LTableName: string;
  LSetClause: string;
  LWhere: string;
  LTable: TFluentList<TMemoryRecord>;
  LSetPairs: TArray<string>;
  LField: string;
  LValueStr: string;
  LValue: Variant;
  LRecord: TMemoryRecord;
  LFor: Integer;
  LIndex: Integer;
  LV: Variant;
  LUpdated: Boolean;
  LWhereField, LWhereValueStr, LWhereOperator: string;
  LWhereValue: Variant;
begin
  LTableName := Trim(Copy(ASQL, 7, Pos('SET', ASQL) - 7));
  LSetClause := Trim(Copy(ASQL, Pos('SET', ASQL) + 4, Pos('WHERE', ASQL + ' ') - Pos('SET', ASQL) - 4));
  LWhere := Trim(Copy(ASQL, Pos('WHERE', ASQL) + 6, Length(ASQL)));

  _SetMonitorLog(Format('Update SQL: %s', [ASQL]), '', nil);

  if FTables.TryGetValue(LTableName, LTable) then
  begin
    LSetPairs := LSetClause.Split([',']);
    FRowsAffected := 0;
    
    // Parse WHERE clause (Simple: Field = Value)
    if LWhere <> '' then
    begin
      if Pos('=', LWhere) > 0 then LWhereOperator := '='
      else if Pos('>', LWhere) > 0 then LWhereOperator := '>'
      else if Pos('<', LWhere) > 0 then LWhereOperator := '<'
      else LWhereOperator := '='; // Default or error

      LWhereField := Trim(Copy(LWhere, 1, Pos(LWhereOperator, LWhere) - 1));
      LWhereValueStr := Trim(Copy(LWhere, Pos(LWhereOperator, LWhere) + 1, Length(LWhere)));
      LWhereValue := _ParseValue(LWhereValueStr);

      _SetMonitorLog(Format('Where: %s %s %s', [LWhereField, LWhereOperator, VarToStr(LWhereValue)]), '', nil);

      for LIndex := 0 to LTable.Count - 1 do
      begin
        LRecord := LTable[LIndex];
        LUpdated := False;
        if LRecord.Fields.TryGetValue(LWhereField, LV) then
        begin
          if LWhereOperator = '=' then
            LUpdated := (LV = LWhereValue)
          else if LWhereOperator = '>' then
            LUpdated := (LV > LWhereValue)
          else if LWhereOperator = '<' then
            LUpdated := (LV < LWhereValue);

          if LUpdated then
          begin
             for LFor := 0 to Length(LSetPairs) - 1 do
              begin
                LField := Trim(Copy(LSetPairs[LFor], 1, Pos('=', LSetPairs[LFor]) - 1));
                LValueStr := Trim(Copy(LSetPairs[LFor], Pos('=', LSetPairs[LFor]) + 1, Length(LSetPairs[LFor])));
                LValue := _ParseValue(LValueStr);
                LRecord.Fields.AddOrSetValue(LField, LValue);
                _SetMonitorLog(Format('Updated: %s = %s', [LField, VarToStr(LValue)]), '', nil);
              end;
              Inc(FRowsAffected);
          end;
        end;
      end;
    end
    else
    begin
      // Update ALL
       for LIndex := 0 to LTable.Count - 1 do
       begin
          LRecord := LTable[LIndex];
          for LFor := 0 to Length(LSetPairs) - 1 do
          begin
            LField := Trim(Copy(LSetPairs[LFor], 1, Pos('=', LSetPairs[LFor]) - 1));
            LValueStr := Trim(Copy(LSetPairs[LFor], Pos('=', LSetPairs[LFor]) + 1, Length(LSetPairs[LFor])));
            LValue := _ParseValue(LValueStr);
            LRecord.Fields.AddOrSetValue(LField, LValue);
          end;
          Inc(FRowsAffected);
       end;
    end;
    _SetMonitorLog(Format('Rows affected: %d', [FRowsAffected]), '', nil);
  end;
end;

function TMemoryConnection._ParseValues(const AValueString: string): TArray<string>;
var
  LList: TList<string>;
  LCurrent: string;
  LInQuote: Boolean;
  LChar: Char;
  I: Integer;
begin
  LList := TList<string>.Create;
  try
    LCurrent := '';
    LInQuote := False;
    for I := 1 to Length(AValueString) do
    begin
      LChar := AValueString[I];
      if LChar = '''' then
        LInQuote := not LInQuote;
      
      if (LChar = ',') and not LInQuote then
      begin
        LList.Add(Trim(LCurrent));
        LCurrent := '';
      end
      else
        LCurrent := LCurrent + LChar;
    end;
    if Trim(LCurrent) <> '' then
      LList.Add(Trim(LCurrent));
      
    Result := LList.ToArray;
  finally
    LList.Free;
  end;
end;

function TMemoryConnection._ParseValue(const AValue: string): Variant;
var
  LVal: string;
  LInt: Integer;
  LFloat: Double;
begin
  LVal := Trim(AValue);
  if (LVal.StartsWith('''') and LVal.EndsWith('''')) then
  begin
    // String
    Result := Copy(LVal, 2, Length(LVal) - 2);
  end
  else if TryStrToInt(LVal, LInt) then
  begin
    Result := LInt;
  end
  else if TryStrToFloat(LVal, LFloat) then
  begin
    Result := LFloat;
  end
  else if SameText(LVal, 'NULL') then
  begin
    Result := Null;
  end
  else
  begin
    // Fallback to string if not quoted but looks like string? 
    // Or maybe assume it's a string if not number.
    Result := LVal;
  end;
end;

procedure TMemoryConnection._ExecuteDelete(const ASQL: string; const AParams: TParams);
var
  LTableName: string;
  LWhere: string;
  LTable: TFluentList<TMemoryRecord>;
  LRecords: IFluentEnumerable<TMemoryRecord>;
  LField: string;
  LValue: string;
  LRecord: TMemoryRecord;
begin
  LTableName := Trim(Copy(ASQL, 7, Pos('WHERE', ASQL + ' ') - 7));
  LWhere := Trim(Copy(ASQL, Pos('WHERE', ASQL) + 6, Length(ASQL)));

  if FTables.TryGetValue(LTableName, LTable) then
  begin
    if LWhere <> '' then
    begin
      LField := Trim(Copy(LWhere, 1, Pos('=', LWhere) - 1));
      LValue := Trim(Copy(LWhere, Pos('=', LWhere) + 1, Length(LWhere)));
      LValue := StringReplace(LValue, '''', '', [rfReplaceAll]);
      LRecords := LTable.AsEnumerable.Where(
        function(R: TMemoryRecord): Boolean
        var
          LV: Variant;
        begin
          Result := R.Fields.TryGetValue(LField, LV) and (VarToStr(LV) = LValue);
        end);
      for LRecord in LRecords do
      begin
        LTable.Remove(LRecord);
        Inc(FRowsAffected);
      end;
    end;
  end;
end;

procedure TMemoryConnection._ClearTables(ATables: TFluentDictionary<string, TFluentList<TMemoryRecord>>);
var
  LTable: TFluentList<TMemoryRecord>;
  LRecord: TMemoryRecord;
begin
  for LTable in ATables.Values do
  begin
    for LRecord in LTable do
      LRecord.Free;
    LTable.Clear;
  end;
  ATables.Clear;
end;

{ TMemoryDataSet }

constructor TMemoryDataSet.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  FSQL := TStringList.Create;
  FParams := TParams.Create;
  FRecords := nil;
  FCurrentIndex := -1;
  FIsOpen := False;
end;

destructor TMemoryDataSet.Destroy;
begin
  FSQL.Free;
  FParams.Free;
  if Assigned(FRecords) then
  begin
    // Records are owned by the connection list in this simplified implementation 
    // or by the result list. If ExecuteSelect returns a new list, we own it.
    // ExecuteSelect returns a NEW list with CLONED records or NEW records.
    // So we must free them.
    FRecords.Clear; // TMemoryRecord destruction
    FRecords.Free;
  end;
  inherited;
end;

procedure TMemoryDataSet.SetConnection(const Value: TMemoryConnection);
begin
  FConnection := Value;
end;

procedure TMemoryDataSet.SetSQL(const Value: TStrings);
begin
  FSQL.Assign(Value);
end;

procedure TMemoryDataSet.SetParams(const Value: TParams);
begin
  FParams.Assign(Value);
end;

procedure TMemoryDataSet.Execute;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned');
  FConnection.ExecuteDirect(FSQL.Text, FParams);
end;

procedure TMemoryDataSet.InternalOpen;
var
  LKey: string;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned');
    
  // Execute Select and get records
  if Assigned(FRecords) then
  begin
    FRecords.Clear;
    FRecords.Free;
  end;
  
  FRecords := FConnection.ExecuteSelect(FSQL.Text, FParams);
  FCurrentIndex := -1;
  FIsOpen := True;
  
  InternalInitFieldDefs;
  
  if DefaultFields then
    CreateFields;
    
  BindFields(True);
end;

procedure TMemoryDataSet.InternalClose;
begin
  FIsOpen := False;
  if Assigned(FRecords) then
  begin
    FRecords.Clear;
    FreeAndNil(FRecords);
  end;
  BindFields(False);
  if DefaultFields then
    DestroyFields;
end;

procedure TMemoryDataSet.InternalInitFieldDefs;
var
  LRecord: TMemoryRecord;
  LPair: TPair<string, Variant>;
  LFieldType: TFieldType;
  LFieldSize: Integer;
begin
  FieldDefs.Clear;
  if (Assigned(FRecords)) and (FRecords.Count > 0) then
  begin
    LRecord := FRecords[0];
    for LPair in LRecord.Fields do
    begin
      LFieldType := _VarTypeToFieldType(VarType(LPair.Value));
      LFieldSize := _GetFieldSize(LFieldType, LPair.Value);
      FieldDefs.Add(LPair.Key, LFieldType, LFieldSize);
    end;
  end;
end;

function TMemoryDataSet._VarTypeToFieldType(const AVarType: Word): TFieldType;
begin
  case AVarType of
    varSmallint, varInteger, varShortInt, varByte, varWord, varLongWord, varInt64:
      Result := ftInteger; // Or ftLargeInt depending on precision
    varSingle, varDouble, varCurrency:
      Result := ftFloat;
    varDate:
      Result := ftDate; // or ftDateTime
    varBoolean:
      Result := ftBoolean;
    varString, varUString, varOleStr:
      Result := ftString;
  else
    Result := ftString; // Default
  end;
end;

function TMemoryDataSet._GetFieldSize(const AFieldType: TFieldType; const AValue: Variant): Integer;
begin
  if AFieldType = ftString then
    Result := Max(Length(VarToStr(AValue)), 255) // Default to 255 or actual length
  else
    Result := 0;
end;

function TMemoryDataSet.GetRecordCount: Integer;
begin
  if Assigned(FRecords) then
    Result := FRecords.Count
  else
    Result := 0;
end;

function TMemoryDataSet.GetRecNo: Integer;
begin
  Result := FCurrentIndex + 1;
end;

procedure TMemoryDataSet.SetRecNo(Value: Integer);
begin
  if (Value >= 1) and (Value <= RecordCount) then
    FCurrentIndex := Value - 1;
end;

function TMemoryDataSet.IsCursorOpen: Boolean;
begin
  Result := FIsOpen;
end;

procedure TMemoryDataSet.InternalHandleException;
begin
  // Default implementation
end;

procedure TMemoryDataSet.InternalGotoBookmark(Bookmark: TBookmark);
var
  LIndex: Integer;
begin
  if Bookmark <> nil then
  begin
    Move(Bookmark^, LIndex, SizeOf(Integer));
    if (LIndex >= 0) and (LIndex < FRecords.Count) then
      FCurrentIndex := LIndex;
  end;
end;

procedure TMemoryDataSet.InternalSetToRecord(Buffer: TRecordBuffer);
begin
  FCurrentIndex := PInteger(Buffer)^;
end;

function TMemoryDataSet.AllocRecordBuffer: TRecordBuffer;
begin
  Result := AllocMem(SizeOf(Integer));
end;

procedure TMemoryDataSet.FreeRecordBuffer(var Buffer: TRecordBuffer);
begin
  FreeMem(Buffer);
end;

function TMemoryDataSet.GetRecord(Buffer: TRecordBuffer; GetMode: TGetMode; DoCheck: Boolean): TGetResult;
begin
  Result := grOK;
  case GetMode of
    gmCurrent:
      begin
        if (FCurrentIndex < 0) or (FCurrentIndex >= RecordCount) then
          Result := grError;
      end;
    gmNext:
      begin
        if FCurrentIndex < RecordCount - 1 then
          Inc(FCurrentIndex)
        else
          Result := grEOF;
      end;
    gmPrior:
      begin
        if FCurrentIndex > 0 then
          Dec(FCurrentIndex)
        else
          Result := grBOF;
      end;
  end;
  
  if Result = grOK then
    PInteger(Buffer)^ := FCurrentIndex;
end;

procedure TMemoryDataSet.GetBookmarkData(Buffer: TRecordBuffer; Data: TBookmark);
begin
  Move(PInteger(Buffer)^, Data^, SizeOf(Integer));
end;

function TMemoryDataSet.GetBookmarkFlag(Buffer: TRecordBuffer): TBookmarkFlag;
begin
  Result := bfCurrent; // Simplified
end;

procedure TMemoryDataSet.SetBookmarkFlag(Buffer: TRecordBuffer; Value: TBookmarkFlag);
begin
  // Simplified
end;

// overload for newer Delphi versions
function TMemoryDataSet.GetFieldData(Field: TField; var Buffer: TValueBuffer): Boolean;
var
  RecIdx: Integer;
  LVal: Variant;
  LInt: Integer;
  LFloat: Double;
  LDate: TDateTime;
  LBool: WordBool;
  LStr: string;
  LBytes: TBytes;
begin
  RecIdx := PInteger(ActiveBuffer)^;
  Result := False;
  if (RecIdx >= 0) and (RecIdx < FRecords.Count) then
  begin
    if FRecords[RecIdx].Fields.TryGetValue(Field.FieldName, LVal) then
    begin
      if not VarIsNull(LVal) then
      begin
        case Field.DataType of
          ftString, ftMemo, ftWideString:
          begin
            LStr := VarToStr(LVal);
            LBytes := TEncoding.UTF8.GetBytes(LStr);
            if Length(LBytes) > Length(Buffer) then
              SetLength(Buffer, Length(LBytes) + 1);
            Move(LBytes[0], Buffer[0], Length(LBytes));
            Buffer[Length(LBytes)] := 0; 
          end;
          ftInteger, ftSmallint, ftWord, ftAutoInc:
          begin
            LInt := LVal;
            Move(LInt, Buffer[0], SizeOf(Integer));
          end;
          ftFloat, ftCurrency:
          begin
            LFloat := LVal;
            Move(LFloat, Buffer[0], SizeOf(Double));
          end;
          ftDate, ftTime, ftDateTime:
          begin
            LDate := VarToDateTime(LVal);
            Move(LDate, Buffer[0], SizeOf(TDateTime));
          end;
          ftBoolean:
          begin
            LBool := LVal;
            Move(LBool, Buffer[0], SizeOf(WordBool));
          end;
        else
           // Fallback
           LStr := VarToStr(LVal);
           LBytes := TEncoding.UTF8.GetBytes(LStr);
           if Length(LBytes) > Length(Buffer) then
              SetLength(Buffer, Length(LBytes) + 1);
           Move(LBytes[0], Buffer[0], Length(LBytes));
           Buffer[Length(LBytes)] := 0;
        end;
        Result := True;
      end;
    end;
  end;
end;

// overload for generic usage or older versions
function TMemoryDataSet.GetFieldData(Field: TField; Buffer: TValueBuffer): Boolean;
var
  RecIdx: Integer;
  LVal: Variant;
  LInt: Integer;
  LFloat: Double;
  LDate: TDateTime;
  LBool: WordBool;
  LStr: string;
  LBytes: TBytes;
begin
  RecIdx := PInteger(ActiveBuffer)^;
  Result := False;
  if (RecIdx >= 0) and (RecIdx < FRecords.Count) then
  begin
    if FRecords[RecIdx].Fields.TryGetValue(Field.FieldName, LVal) then
    begin
      if not VarIsNull(LVal) then
      begin
         case Field.DataType of
          ftString, ftMemo, ftWideString:
          begin
            LStr := VarToStr(LVal);
            LBytes := TEncoding.UTF8.GetBytes(LStr);
            Move(LBytes[0], Buffer[0], Min(Length(LBytes), Field.DataSize));
          end;
          ftInteger, ftSmallint, ftWord, ftAutoInc:
          begin
            LInt := LVal;
            Move(LInt, Buffer[0], SizeOf(Integer));
          end;
          ftFloat, ftCurrency:
          begin
            LFloat := LVal;
            Move(LFloat, Buffer[0], SizeOf(Double));
          end;
          ftDate, ftTime, ftDateTime:
          begin
            LDate := VarToDateTime(LVal);
            Move(LDate, Buffer[0], SizeOf(TDateTime));
          end;
          ftBoolean:
          begin
            LBool := LVal;
            Move(LBool, Buffer[0], SizeOf(WordBool));
          end;
        else
           // Fallback
           LStr := VarToStr(LVal);
           LBytes := TEncoding.UTF8.GetBytes(LStr);
           Move(LBytes[0], Buffer[0], Min(Length(LBytes), Field.DataSize));
        end;
        Result := True;
      end;
    end;
  end;
end;

procedure TMemoryDataSet.SetFieldData(Field: TField; Buffer: TValueBuffer);
begin
  // Read-only for now in this Mock DataSet implementation context
  // or implement logic to update FRecords
end;

{ TDriverMemory }

constructor TDriverMemory.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction; const ADriver: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  inherited Create(AConnection, ADriverTransaction, ADriver, AMonitorCallback);
  if AConnection is TMemoryConnection then
  begin
    FConnection := TMemoryConnection(AConnection);
    FOwnsConnection := False;
  end
  else
  begin
    FConnection := TMemoryConnection.Create(nil);
    FOwnsConnection := True;
  end;
  FConnection.MonitorCallback := AMonitorCallback;
end;

destructor TDriverMemory.Destroy;
begin
  if FOwnsConnection then
    FConnection.Free;
  FConnection := nil;
  FDriverTransaction := nil;
  inherited;
end;

procedure TDriverMemory.Connect;
begin
  FConnection.Connect;
end;

procedure TDriverMemory.Disconnect;
begin
  FConnection.Disconnect;
end;

procedure TDriverMemory.ExecuteDirect(const ASQL: String);
begin
  ExecuteDirect(ASQL, nil);
end;

procedure TDriverMemory.ExecuteDirect(const ASQL: String; const AParams: TParams);
begin
  if not FConnection.Connected then
    FConnection.Connect;
    
  if FDriverTransaction.TransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
    
  if not FDriverTransaction.InTransaction then
    FDriverTransaction.StartTransaction;
    
  try
    FConnection.ExecuteDirect(ASQL, AParams);
    FRowsAffected := FConnection.RowsAffected;
    FDriverTransaction.Commit;
  except
    FDriverTransaction.Rollback;
    raise;
  end;
end;

procedure TDriverMemory.ExecuteScript(const AScript: String);
var
  LScript: TStringList;
  LCommand: string;
  LCurrent: string;
  LFor: Integer;
  LInQuote: Boolean;
  LChar: Char;
begin
  if FDriverTransaction.TransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LScript := TStringList.Create;
  try
    LCurrent := '';
    LInQuote := False;
    for LFor := 1 to Length(AScript) do
    begin
      LChar := AScript[LFor];
      if LChar = '''' then
        LInQuote := not LInQuote;
      if (LChar = ';') and not LInQuote then
      begin
        if Trim(LCurrent) <> '' then
          LScript.Add(Trim(LCurrent));
        LCurrent := '';
      end
      else
        LCurrent := LCurrent + LChar;
    end;
    if Trim(LCurrent) <> '' then
      LScript.Add(Trim(LCurrent));

    for LFor := 0 to LScript.Count - 1 do
    begin
      LCommand := Trim(LScript[LFor]);
      if LCommand <> '' then
      begin
        _SetMonitorLog(Format('Executing script command: %s', [LCommand]), '', nil);
        ExecuteDirect(LCommand);
      end;
    end;
  finally
    LScript.Free;
  end;
end;

procedure TDriverMemory.AddScript(const AScript: String);
begin
  // Implementation of storing script if needed, or just execute in ExecuteScripts
end;

procedure TDriverMemory.ExecuteScripts;
begin
  // Implementation
end;

procedure TDriverMemory.ApplyUpdates(const ADataSets: array of IDBDataSet);
begin
  // Simulation: nothing to do in memory
end;

function TDriverMemory.IsConnected: Boolean;
begin
  Result := FConnection.Connected;
end;

function TDriverMemory.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryMemory.Create(FConnection,
                                      FDriverTransaction,
                                      FMonitorCallback);
end;

function TDriverMemory.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LQuery: TDriverQueryMemory;
begin
  // In this pattern, CreateDataSet usually returns a Query/DataSet wrapper
  LQuery := TDriverQueryMemory.Create(FConnection, FDriverTransaction, FMonitorCallback);
  LQuery.CommandText := ASQL;
  Result := LQuery.ExecuteQuery;
end;

function TDriverMemory.GetSQLScripts: String;
begin
  Result := ''; 
end;

function TDriverMemory._GetTransactionActive: TComponent;
begin
  Result := FDriverTransaction.TransactionActive;
end;

{ TDriverQueryMemory }

constructor TDriverQueryMemory.Create(const AConnection: TMemoryConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  inherited Create;
  FDataSet := TMemoryDataSet.Create(nil);
  FDataSet.Connection := AConnection;
  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
end;

destructor TDriverQueryMemory.Destroy;
begin
  FDataSet.Free;
  inherited;
end;

function TDriverQueryMemory._GetTransactionActive: TComponent;
begin
  Result := FDriverTransaction.TransactionActive;
end;

procedure TDriverQueryMemory.ExecuteDirect;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  if not FDriverTransaction.InTransaction then
    FDriverTransaction.StartTransaction;
  try
    FDataSet.SQL.Text := _GetCommandText;
    // FDataSet.Params... copy params if needed
    FDataSet.Execute;
    FRowsAffected := FDataSet.Connection.RowsAffected;
    FDriverTransaction.Commit;
  except
    FDriverTransaction.Rollback;
    raise;
  end;
end;

function TDriverQueryMemory.ExecuteQuery: IDBDataSet;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  if not FDriverTransaction.InTransaction then
    FDriverTransaction.StartTransaction;
  try
    FDataSet.SQL.Text := _GetCommandText;
    FDataSet.Open;
    Result := TDriverDataSetMemory.Create(FDataSet, FMonitorCallback);
    FDriverTransaction.Commit;
  except
    FDriverTransaction.Rollback;
    raise;
  end;
end;

function TDriverQueryMemory.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

procedure TDriverQueryMemory._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

function TDriverQueryMemory._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

{ TDriverDataSetMemory }

constructor TDriverDataSetMemory.Create(const ADataSet: TMemoryDataSet;
  const AMonitorCallback: TMonitorProc);
begin
  // Initialize generic TDriverDataSet<TMemoryDataSet>
  inherited Create(ADataSet, AMonitorCallback);
end;

function TDriverDataSetMemory._GetCommandText: String;
begin
  Result := FDataSet.SQL.Text;
end;

procedure TDriverDataSetMemory._SetCommandText(const ACommandText: String);
begin
  FDataSet.SQL.Text := ACommandText;
end;

function TDriverDataSetMemory.RowsAffected: UInt32;
begin
  if Assigned(FDataSet.Connection) then
    Result := FDataSet.Connection.RowsAffected
  else
    Result := 0;
end;

end.
