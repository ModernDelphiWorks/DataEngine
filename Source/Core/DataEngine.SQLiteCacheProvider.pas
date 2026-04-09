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

unit DataEngine.SQLiteCacheProvider;

interface

uses
  SysUtils,
  Classes,
  DB,
  DataEngine.FactoryInterfaces,
  DataEngine.CacheTypes,
  FireDAC.Comp.Client,
  FireDAC.Stan.Intf,
  FireDAC.Stan.Option,
  FireDAC.Stan.Error,
  FireDAC.Phys.Intf,
  FireDAC.Phys.SQLite,
  FireDAC.Phys.SQLiteDef,
  FireDAC.Stan.Def,
  FireDAC.Stan.Async,
  FireDAC.DApt;

type
  TSQLiteCacheProvider = class(TInterfacedObject, IDBCacheProvider, IDBMetadataCache)
  private
    FConn: TFDConnection;
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
  FConn := TFDConnection.Create(nil);
  FConn.DriverName := 'SQLite';
  FConn.Params.Database := ADatabasePath;
  FConn.LoginPrompt := False;
  FConn.Connected := True;
  
  FHitCount := 0;
  FMissCount := 0;
  
  _EnsureTable;
end;

destructor TSQLiteCacheProvider.Destroy;
begin
  FConn.Free;
  inherited;
end;

procedure TSQLiteCacheProvider._EnsureTable;
begin
  FConn.ExecSQL(
    'CREATE TABLE IF NOT EXISTS CacheStore (' +
    '  Key TEXT PRIMARY KEY,' +
    '  MetaData TEXT,' +
    '  Data BLOB,' +
    '  ExpireAt DATETIME' +
    ')'
  );
  FConn.ExecSQL('CREATE INDEX IF NOT EXISTS IDX_CacheStore_ExpireAt ON CacheStore(ExpireAt)');
  
  FConn.ExecSQL(
    'CREATE TABLE IF NOT EXISTS CacheTableMapping (' +
    '  Key TEXT,' +
    '  TableName TEXT' +
    ')'
  );
  FConn.ExecSQL('CREATE INDEX IF NOT EXISTS IDX_CacheTableMapping_TableName ON CacheTableMapping(TableName)');
end;

procedure TSQLiteCacheProvider.SetValue(const AKey: string; const ADataSet: IDBDataSetSnapshot; const ATTL: Integer; const ATables: TArray<string>);
var
  LStream: TMemoryStream;
  LQuery: TFDQuery;
  LExpireAt: TDateTime;
  LTTL: Integer;
  LTable: string;
begin
  LTTL := ATTL;
  if LTTL <= 0 then
    LTTL := 30; // 30 minutes fallback

  LExpireAt := Now + (LTTL / (24 * 60)); 

  LStream := TMemoryStream.Create;
  LQuery := TFDQuery.Create(nil);
  try
    LQuery.Connection := FConn;
    if Assigned(ADataSet) then
    begin
      TCacheManager.SerializeToStream(ADataSet, LStream);
      LStream.Position := 0;
    end;
    
    LQuery.SQL.Text := 'INSERT OR REPLACE INTO CacheStore (Key, Data, ExpireAt) VALUES (:Key, :Data, :ExpireAt)';
    LQuery.ParamByName('Key').AsString := AKey;
    LQuery.ParamByName('Data').LoadFromStream(LStream, ftBlob);
    LQuery.ParamByName('ExpireAt').AsDateTime := LExpireAt;
    LQuery.ExecSQL;
    
    if Length(ATables) > 0 then
    begin
       LQuery.SQL.Text := 'DELETE FROM CacheTableMapping WHERE Key = :Key';
       LQuery.ParamByName('Key').AsString := AKey;
       LQuery.ExecSQL;
       
       LQuery.SQL.Text := 'INSERT INTO CacheTableMapping (Key, TableName) VALUES (:Key, :TableName)';
       for LTable in ATables do
       begin
         LQuery.ParamByName('Key').AsString := AKey;
         LQuery.ParamByName('TableName').AsString := LTable;
         LQuery.ExecSQL;
       end;
    end;
  finally
    LQuery.Free;
    LStream.Free;
  end;
end;

function TSQLiteCacheProvider.GetValue(const AKey: string): IDBDataSetSnapshot;
var
  LQuery: TFDQuery;
  LStream: TStream;
begin
  LQuery := TFDQuery.Create(nil);
  try
    LQuery.Connection := FConn;
    LQuery.SQL.Text := 'SELECT Data FROM CacheStore WHERE Key = :Key AND (ExpireAt IS NULL OR ExpireAt > :Now)';
    LQuery.ParamByName('Key').AsString := AKey;
    LQuery.ParamByName('Now').AsDateTime := Now;
    LQuery.Open;
    
    if not LQuery.Eof then
    begin
      LStream := LQuery.CreateBlobStream(LQuery.FieldByName('Data'), bmRead);
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
    LQuery.Free;
  end;
end;

procedure TSQLiteCacheProvider.Clear;
begin
  FConn.ExecSQL('DELETE FROM CacheTableMapping');
  FConn.ExecSQL('DELETE FROM CacheStore');
  FHitCount := 0;
  FMissCount := 0;
end;

procedure TSQLiteCacheProvider.Evict(const AKey: string);
begin
  FConn.ExecSQL('DELETE FROM CacheTableMapping WHERE Key = :Key', [AKey]);
  FConn.ExecSQL('DELETE FROM CacheStore WHERE Key = :Key', [AKey]);
end;

procedure TSQLiteCacheProvider.InvalidateByTable(const ATableName: string);
var
  LKeys: TStringList;
  LIndex: Integer;
begin
  LKeys := TStringList.Create;
  try
    // Get all keys mapping to this table
    FConn.ExecSQL('SELECT DISTINCT Key FROM CacheTableMapping WHERE TableName = :TableName', [ATableName],
      procedure(ADataSet: TDataSet)
      begin
        LKeys.Add(ADataSet.Fields[0].AsString);
      end
    );
    
    // Invalidate each key
    for LIndex := 0 to LKeys.Count - 1 do
      Evict(LKeys[LIndex]);
      
    // Clear mapping for this table
    FConn.ExecSQL('DELETE FROM CacheTableMapping WHERE TableName = :TableName', [ATableName]);
  finally
    LKeys.Free;
  end;
end;

function TSQLiteCacheProvider.Count: Integer;
begin
  Result := FConn.ExecSQLScalar('SELECT COUNT(*) FROM CacheStore');
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
  FConn.ExecSQL('DELETE FROM CacheStore WHERE ExpireAt < :Now', [Now]);
end;

procedure TSQLiteCacheProvider.SetMetadata(const AKey: string; const AJSON: string; const ATTL: Integer);
var
  LQuery: TFDQuery;
  LExpireAt: TDateTime;
  LTTL: Integer;
begin
  LTTL := ATTL;
  if LTTL <= 0 then
    LTTL := 1440; // Default 24 hours (minutes)

  LExpireAt := Now + (LTTL / (24 * 60));

  LQuery := TFDQuery.Create(nil);
  try
    LQuery.Connection := FConn;
    LQuery.SQL.Text := 'INSERT OR REPLACE INTO CacheStore (Key, MetaData, ExpireAt) VALUES (:Key, :MetaData, :ExpireAt)';
    LQuery.ParamByName('Key').AsString := 'META:' + AKey;
    LQuery.ParamByName('MetaData').AsString := AJSON;
    LQuery.ParamByName('ExpireAt').AsDateTime := LExpireAt;
    LQuery.ExecSQL;
  finally
    LQuery.Free;
  end;
end;

function TSQLiteCacheProvider.GetMetadata(const AKey: string): string;
var
  LQuery: TFDQuery;
begin
  Result := string.Empty;
  LQuery := TFDQuery.Create(nil);
  try
    LQuery.Connection := FConn;
    LQuery.SQL.Text := 'SELECT MetaData FROM CacheStore WHERE Key = :Key AND (ExpireAt IS NULL OR ExpireAt > :Now)';
    LQuery.ParamByName('Key').AsString := 'META:' + AKey;
    LQuery.ParamByName('Now').AsDateTime := Now;
    LQuery.Open;
    
    if not LQuery.Eof then
    begin
      Result := LQuery.FieldByName('MetaData').AsString;
      Inc(FHitCount);
    end
    else
      Inc(FMissCount);
  finally
    LQuery.Free;
  end;
end;

end.
