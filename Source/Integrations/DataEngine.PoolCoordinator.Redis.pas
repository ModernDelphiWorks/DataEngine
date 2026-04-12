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

unit DataEngine.PoolCoordinator.Redis;

interface

uses
  SysUtils,
  Classes,
  DataEngine.FactoryInterfaces;

type
  { Generic interface to abstract the Redis client.
    Implement this interface to bridge your favorite Redis library (e.g., Redlock-delphi, DelphiRedisClient) }
  IRedisClient = interface
    ['{8AB25F68-91E0-469A-B8ED-0FE6AF4A2034}']
    function Eval(const AScript: string; const AKeys: array of string; const AArgs: array of string): string;
  end;

  { TRedisPoolCoordinator: Distributed pooling coordinator using Redis ZSET strategy.
    Reference: ADR-053 }
  TRedisPoolCoordinator = class(TInterfacedObject, IDBPoolCoordinator)
  private
    FRedis: IRedisClient;
    FNodeID: string;
    FTTL: Integer;
    function GetNowUnix: string;
  public
    constructor Create(const ARedis: IRedisClient; const ANodeID: string = ''; const ATTL: Integer = 60);
    function AcquireSlot(const ATenantID: string; const AMaxSlots: Integer; const ATimeout: Integer; out ASlotToken: string): Boolean;
    procedure ReleaseSlot(const ATenantID: string; const ASlotToken: string);
  end;

implementation

uses
  DateUtils;

{ TRedisPoolCoordinator }

constructor TRedisPoolCoordinator.Create(const ARedis: IRedisClient; const ANodeID: string; const ATTL: Integer);
begin
  inherited Create;
  FRedis := ARedis;
  FNodeID := ANodeID;
  if FNodeID = '' then
    FNodeID := TGuid.NewGuid.ToString.Replace('{', '').Replace('}', '');
  FTTL := ATTL;
end;

function TRedisPoolCoordinator.GetNowUnix: string;
begin
  Result := IntToStr(DateTimeToUnix(Now));
end;

function TRedisPoolCoordinator.AcquireSlot(const ATenantID: string; const AMaxSlots: Integer; const ATimeout: Integer; out ASlotToken: string): Boolean;
const
  LScript = 
    'local key = KEYS[1] ' +
    'local member = ARGV[1] ' +
    'local now = tonumber(ARGV[2]) ' +
    'local ttl = tonumber(ARGV[3]) ' +
    'local max = tonumber(ARGV[4]) ' +
    'redis.call("ZREMRANGEBYSCORE", key, "-inf", now - ttl) ' +
    'local count = redis.call("ZCARD", key) ' +
    'if count < max then ' +
    '  redis.call("ZADD", key, now, member) ' +
    '  return "OK" ' +
    'else ' +
    '  if redis.call("ZSCORE", key, member) then ' +
    '    redis.call("ZADD", key, now, member) ' +
    '    return "OK" ' +
    '  end ' +
    '  return "BUSY" ' +
    'end';
var
  LKey: string;
  LRes: string;
begin
  LKey := 'DataEngine:Pool:Slots:' + ATenantID;
  ASlotToken := FNodeID + ':' + TGuid.NewGuid.ToString.Replace('{', '').Replace('}', '');
  
  try
    LRes := FRedis.Eval(LScript, [LKey], [ASlotToken, GetNowUnix, IntToStr(FTTL), IntToStr(AMaxSlots)]);
    Result := LRes = 'OK';
  except
    // Fallback: If Redis fails, we should decide whether to block or allow.
    // Following "Graceful Degradation" rule, but TPoolConnection loop handles timeout.
    Result := False; 
  end;
end;

procedure TRedisPoolCoordinator.ReleaseSlot(const ATenantID: string; const ASlotToken: string);
const
  LScript = 'redis.call("ZREM", KEYS[1], ARGV[1]); return "OK"';
begin
  try
    FRedis.Eval(LScript, ['DataEngine:Pool:Slots:' + ATenantID], [ASlotToken]);
  except
    // Silent fail on release is acceptable for distributed pools due to TTL cleanup
  end;
end;

end.
