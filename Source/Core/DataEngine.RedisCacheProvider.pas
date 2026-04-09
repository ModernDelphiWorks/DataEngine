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

unit DataEngine.RedisCacheProvider;

interface

uses
  SysUtils,
  Classes,
  DataEngine.FactoryInterfaces,
  DataEngine.CacheTypes,
  DataEngine.RedisTransport;

type
  TRedisCacheProvider = class(TInterfacedObject, IDBCacheProvider, IDBMetadataCache)
  private
    FTransport: TRedisTransport;
    FHitCount: Integer;
    FMissCount: Integer;
    FPrefix: string;
    FConfig: TRedisConfig;

    function _GetDataKey(const AKey: string): string;
    function _GetTagKey(const ATableName: string): string;
    function _GetMetaKey(const AKey: string): string;
  public
    constructor Create(const AConfig: TRedisConfig); overload;
    constructor Create(const AHost: string = 'localhost'; APort: Integer = 6379); overload;
    destructor Destroy; override;

    // IDBCacheProvider
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

    property Transport: TRedisTransport read FTransport;
  end;

implementation

uses
  DataEngine.CacheManager;

{ TRedisCacheProvider }

constructor TRedisCacheProvider.Create(const AConfig: TRedisConfig);
begin
  inherited Create;
  FConfig := AConfig;
  FTransport := TRedisTransport.Create(FConfig.Host, FConfig.Port);
  FTransport.Password := FConfig.Password;
  FTransport.DBIndex := FConfig.DBIndex;
  FHitCount := 0;
  FMissCount := 0;
  FPrefix := 'DataEngine:';
end;

constructor TRedisCacheProvider.Create(const AHost: string; APort: Integer);
var
  LConfig: TRedisConfig;
begin
  LConfig := TRedisConfig.Default;
  LConfig.Host := AHost;
  LConfig.Port := APort;
  Create(LConfig);
end;

destructor TRedisCacheProvider.Destroy;
begin
  FTransport.Free;
  inherited;
end;

function TRedisCacheProvider._GetDataKey(const AKey: string): string;
begin
  Result := FPrefix + 'Data:' + AKey;
end;

function TRedisCacheProvider._GetTagKey(const ATableName: string): string;
begin
  Result := FPrefix + 'Tags:' + ATableName;
end;

function TRedisCacheProvider._GetMetaKey(const AKey: string): string;
begin
  Result := FPrefix + 'Meta:' + AKey;
end;

procedure TRedisCacheProvider.SetValue(const AKey: string; const ADataSet: IDBDataSetSnapshot; const ATTL: Integer; const ATables: TArray<string>);
var
  LStream: TMemoryStream;
  LData: TBytes;
  LTable: string;
  LTTLSeconds: Integer;
begin
  if not Assigned(ADataSet) then
    Exit;

  LStream := TMemoryStream.Create;
  try
    TCacheManager.SerializeToStream(ADataSet, LStream);
    SetLength(LData, LStream.Size);
    if LStream.Size > 0 then
      Move(LStream.Memory^, LData[0], LStream.Size);

  // 1. SET Data
    FTransport.ExecuteCommand('SET', [TEncoding.UTF8.GetBytes(_GetDataKey(AKey)), LData]);

    // 2. EXPIRE Data
    LTTLSeconds := ATTL * 60; // minutes to seconds
    if LTTLSeconds > 0 then
      FTransport.ExecuteCommand('EXPIRE', [_GetDataKey(AKey), IntToStr(LTTLSeconds)]);

    // 3. SADD Tags
    if Length(ATables) > 0 then
    begin
      for LTable in ATables do
        FTransport.ExecuteCommand('SADD', [_GetTagKey(LTable), AKey]);
    end;
  finally
    LStream.Free;
  end;
end;

function TRedisCacheProvider.GetValue(const AKey: string): IDBDataSetSnapshot;
var
  LResp: TRedisResponse;
  LStream: TMemoryStream;
begin
  try
    LResp := FTransport.ExecuteCommand('GET', [_GetDataKey(AKey)]);
    if LResp.RawValue = nil then
    begin
      Inc(FMissCount);
      Exit(nil);
    end;

    LStream := TMemoryStream.Create;
    try
      LStream.Write(LResp.RawValue[0], Length(LResp.RawValue));
      LStream.Position := 0;
      Result := TCacheManager.DeserializeFromStream(LStream);
      if Assigned(Result) then
        Inc(FHitCount)
      else
        Inc(FMissCount);
    finally
      LStream.Free;
    end;
  except
    Inc(FMissCount);
    Result := nil;
  end;
end;

procedure TRedisCacheProvider.Clear;
begin
  // FLUSHDB is dangerous, maybe better to loop and delete prefix keys?
  // For simplicity and matching current local providers:
  FTransport.ExecuteCommand('FLUSHDB', []);
  FHitCount := 0;
  FMissCount := 0;
end;

procedure TRedisCacheProvider.Evict(const AKey: string);
begin
  FTransport.ExecuteCommand('DEL', [_GetDataKey(AKey)]);
end;

procedure TRedisCacheProvider.InvalidateByTable(const ATableName: string);
var
  LResp: TRedisResponse;
  LKey: string;
begin
  // 1. Get all queries for this table
  LResp := FTransport.ExecuteCommand('SMEMBERS', [_GetTagKey(ATableName)]);

  // 2. Delete each query data
  if Assigned(LResp.ArrayValue) then
  begin
    for LKey in LResp.ArrayValue do
      FTransport.ExecuteCommand('DEL', [_GetDataKey(LKey)]);
  end;

  // 3. Clear the tag set
  FTransport.ExecuteCommand('DEL', [_GetTagKey(ATableName)]);
end;

function TRedisCacheProvider.Count: Integer;
begin
  // DBSYZE returns total keys in DB
  Result := FTransport.ExecuteCommand('DBSIZE', []).IntegerValue;
end;

function TRedisCacheProvider.HitCount: Integer;
begin
  Result := FHitCount;
end;

function TRedisCacheProvider.MissCount: Integer;
begin
  Result := FMissCount;
end;

procedure TRedisCacheProvider.Prune;
begin
  // Redis handles TTL automatically
end;

procedure TRedisCacheProvider.SetMetadata(const AKey: string; const AJSON: string;
  const ATTL: Integer);
var
  LTTLSeconds: Integer;
begin
  FTransport.ExecuteCommand('SET', [_GetMetaKey(AKey), AJSON]);
  
  LTTLSeconds := ATTL * 60;
  if LTTLSeconds <= 0 then
    LTTLSeconds := 1440 * 60; // 24h default for metadata

  FTransport.ExecuteCommand('EXPIRE', [_GetMetaKey(AKey), IntToStr(LTTLSeconds)]);
end;

function TRedisCacheProvider.GetMetadata(const AKey: string): string;
var
  LResp: TRedisResponse;
begin
  LResp := FTransport.ExecuteCommand('GET', [_GetMetaKey(AKey)]);
  Result := LResp.StringValue;
  if Result <> '' then
    Inc(FHitCount)
  else
    Inc(FMissCount);
end;

end.
