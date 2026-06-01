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

unit DataEngine.SQLiteCacheProvider;

interface

uses
  SysUtils,
  Classes,
  DB,
  DataEngine.FactoryInterfaces,
  DataEngine.CacheTypes,
  SQLiteTable3;

type
  TSQLiteCacheProvider = class(TInterfacedObject, IDBCacheProvider, IDBMetadataCache)
  private
    FDB: TSQLiteDatabase;
    FHitCount: Integer;
    FMissCount: Integer;
    FDatabasePath: string;
    procedure _EnsureTable;
  public
    constructor Create(const ADatabasePath: string = 'dataengine_cache.db');
    destructor Destroy; override;
    
    procedure SetValue(const AKey: string; const ADataSet: IDBDataSetSnapshot; const ATTL: Integer = 0; const ATables: TArray<string> = nil);
    function GetValue(const AKey: string): IDBDataSetSnapshot;
    procedure Clear;
    procedure Evict(const AKey: string);
    procedure InvalidateByTable(const ATableName: string);
    function Count: Integer;
    function HitCount: Integer;
    function MissCount: Integer;
    
    procedure Prune;

    // IDBMetadataCache
    procedure SetMetadata(const AKey: string; const AJSON: string; const ATTL: Integer = 0);
    function GetMetadata(const AKey: string): string;
    // Clear and Evict are shared with IDBCacheProvider
  end;

implementation

uses
  DataEngine.CacheManager;

{ TSQLiteCacheProvider }

constructor TSQLiteCacheProvider.Create(const ADatabasePath: string);
begin
  inherited Create;
  FDatabasePath := ADatabasePath;
  FDB := TSQLiteDatabase.Create(ADatabasePath);
  
  FHitCount := 0;
  FMissCount := 0;
  
  _EnsureTable;
end;

destructor TSQLiteCacheProvider.Destroy;
begin
  FDB.Free;
  inherited;
end;

procedure TSQLiteCacheProvider._EnsureTable;
begin
  FDB.ExecSQL(
    'CREATE TABLE IF NOT EXISTS CacheStore (' +
    '  Key TEXT PRIMARY KEY,' +
    '  MetaData TEXT,' +
    '  Data BLOB,' +
    '  ExpireAt DATETIME' +
    ')'
  );
  FDB.ExecSQL('CREATE INDEX IF NOT EXISTS IDX_CacheStore_ExpireAt ON CacheStore(ExpireAt)');
  
  FDB.ExecSQL(
    'CREATE TABLE IF NOT EXISTS CacheTableMapping (' +
    '  Key TEXT,' +
    '  TableName TEXT' +
    ')'
  );
  FDB.ExecSQL('CREATE INDEX IF NOT EXISTS IDX_CacheTableMapping_TableName ON CacheTableMapping(TableName)');
end;

procedure TSQLiteCacheProvider.SetValue(const AKey: string; const ADataSet: IDBDataSetSnapshot; const ATTL: Integer; const ATables: TArray<string>);
var
  LStream: TMemoryStream;
  LStmt: ISQLitePreparedStatement;
  LExpireAt: TDateTime;
  LTTL: Integer;
  LTable: string;
begin
  LTTL := ATTL;
  if LTTL <= 0 then
    LTTL := 30; // 30 minutes fallback

  LExpireAt := Now + (LTTL / (24 * 60)); 

  LStream := TMemoryStream.Create;
  try
    if Assigned(ADataSet) then
    begin
      TCacheManager.SerializeToStream(ADataSet, LStream);
      LStream.Position := 0;
    end;
    
    LStmt := FDB.GetPreparedStatementIntf('INSERT OR REPLACE INTO CacheStore (Key, Data, ExpireAt) VALUES (:Key, :Data, :ExpireAt)');
    LStmt.SetParamText(':Key', AKey);
    if LStream.Size > 0 then
      LStmt.SetParamBlob(':Data', LStream)
    else
      LStmt.SetParamNull(':Data');
    LStmt.SetParamDateTime(':ExpireAt', LExpireAt);
    LStmt.ExecSQL;
    
    if Length(ATables) > 0 then
    begin
       FDB.ExecSQL('DELETE FROM CacheTableMapping WHERE Key = ' + QuotedStr(AKey));
       
       LStmt := FDB.GetPreparedStatementIntf('INSERT INTO CacheTableMapping (Key, TableName) VALUES (:Key, :TableName)');
       for LTable in ATables do
       begin
         LStmt.SetParamText(':Key', AKey);
         LStmt.SetParamText(':TableName', LTable);
         LStmt.ExecSQL;
       end;
    end;
  finally
    LStream.Free;
  end;
end;

function TSQLiteCacheProvider.GetValue(const AKey: string): IDBDataSetSnapshot;
var
  LTable: TSQLiteTable;
  LStream: TMemoryStream;
begin
  LTable := FDB.GetTable('SELECT Data FROM CacheStore WHERE Key = ' + QuotedStr(AKey) + 
    ' AND (ExpireAt IS NULL OR ExpireAt > ' + FloatToStr(Now) + ')');
  try
    if not LTable.EOF then
    begin
      LStream := LTable.FieldAsBlob(LTable.FieldIndex['Data']);
      try
        Result := TCacheManager.DeserializeFromStream(LStream);
        if Assigned(Result) then
          Inc(FHitCount)
        else
          Inc(FMissCount);
      finally
        LStream.Free;
      end;
    end
    else
    begin
      Inc(FMissCount);
      Result := nil;
    end;
  finally
    LTable.Free;
  end;
end;

procedure TSQLiteCacheProvider.Clear;
begin
  FDB.ExecSQL('DELETE FROM CacheTableMapping');
  FDB.ExecSQL('DELETE FROM CacheStore');
  FHitCount := 0;
  FMissCount := 0;
end;

procedure TSQLiteCacheProvider.Evict(const AKey: string);
begin
  FDB.ExecSQL('DELETE FROM CacheTableMapping WHERE Key = ' + QuotedStr(AKey));
  FDB.ExecSQL('DELETE FROM CacheStore WHERE Key = ' + QuotedStr(AKey));
end;

procedure TSQLiteCacheProvider.InvalidateByTable(const ATableName: string);
var
  LTable: TSQLiteTable;
begin
  LTable := FDB.GetTable('SELECT DISTINCT Key FROM CacheTableMapping WHERE TableName = ' + QuotedStr(ATableName));
  try
    while not LTable.EOF do
    begin
      Evict(LTable.Fields[0]);
      LTable.Next;
    end;
    
    // Clear mapping for this table
    FDB.ExecSQL('DELETE FROM CacheTableMapping WHERE TableName = ' + QuotedStr(ATableName));
  finally
    LTable.Free;
  end;
end;

function TSQLiteCacheProvider.Count: Integer;
begin
  Result := FDB.GetTableValue('SELECT COUNT(*) FROM CacheStore');
end;

function TSQLiteCacheProvider.HitCount: Integer;
begin
  Result := FHitCount;
end;

function TSQLiteCacheProvider.MissCount: Integer;
begin
  Result := FMissCount;
end;

procedure TSQLiteCacheProvider.Prune;
begin
  FDB.ExecSQL('DELETE FROM CacheStore WHERE ExpireAt < ' + FloatToStr(Now));
end;

procedure TSQLiteCacheProvider.SetMetadata(const AKey: string; const AJSON: string; const ATTL: Integer);
var
  LStmt: ISQLitePreparedStatement;
  LExpireAt: TDateTime;
  LTTL: Integer;
begin
  LTTL := ATTL;
  if LTTL <= 0 then
    LTTL := 1440; // Default 24 hours (minutes)

  LExpireAt := Now + (LTTL / (24 * 60));

  LStmt := FDB.GetPreparedStatementIntf('INSERT OR REPLACE INTO CacheStore (Key, MetaData, ExpireAt) VALUES (:Key, :MetaData, :ExpireAt)');
  LStmt.SetParamText(':Key', 'META:' + AKey);
  LStmt.SetParamText(':MetaData', AJSON);
  LStmt.SetParamDateTime(':ExpireAt', LExpireAt);
  LStmt.ExecSQL;
end;

function TSQLiteCacheProvider.GetMetadata(const AKey: string): string;
var
  LTable: TSQLiteTable;
begin
  Result := string.Empty;
  LTable := FDB.GetTable('SELECT MetaData FROM CacheStore WHERE Key = ' + QuotedStr('META:' + AKey) + 
    ' AND (ExpireAt IS NULL OR ExpireAt > ' + FloatToStr(Now) + ')');
  try
    if not LTable.EOF then
    begin
      Result := LTable.FieldValByNameAsString['MetaData'];
      Inc(FHitCount);
    end
    else
      Inc(FMissCount);
  finally
    LTable.Free;
  end;
end;

end.
