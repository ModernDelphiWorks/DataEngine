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
    function Execute(const ACommand: string; const AArgs: array of string): TArray<string>;
    procedure SetEndpoint(const AHost: string; const APort: Integer);
  end;

  { TRedisPoolCoordinator: Distributed pooling coordinator using Redis ZSET strategy.
    Reference: ADR-053, ADR-054, ADR-014 }
  TRedisPoolCoordinator = class(TInterfacedObject, IDBPoolCoordinator)
  private
    FRedis: IRedisClient;
    FNodeID: string;
    FTTL: Integer;
    FMaxRetries: Integer;
    FEndpoints: TStrings;
    FServiceName: string;
    FCurrentMasterHost: string;
    FCurrentMasterPort: Integer;
    function GetNowUnix: string;
    function ExecuteWithRetry(const AFunc: TFunc<string>): string;
    procedure _DiscoverMaster;
    procedure _SetCurrentEndpoint(const AHost: string; const APort: Integer);
  public
    constructor Create(const ARedis: IRedisClient; const ANodeID: string = ''; const ATTL: Integer = 60);
    constructor CreateSentinel(const ARedis: IRedisClient; const AServiceName: string; const AEndpoints: TStrings; const ANodeID: string = ''; const ATTL: Integer = 60);
    destructor Destroy; override;
    function AcquireSlot(const ATenantID: string; const AMaxSlots: Integer; const ATimeout: Integer; out ASlotToken: string): Boolean;
    function RefreshSlot(const ATenantID: string; const ASlotToken: string): Boolean;
    procedure ReleaseSlot(const ATenantID: string; const ASlotToken: string);
    function GetGlobalMetrics(const ATenantID: string): TPoolMetrics;
    function GetSlotTTL: Integer;
    property MaxRetries: Integer read FMaxRetries write FMaxRetries;
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
  FMaxRetries := 3;
  FEndpoints := TStringList.Create;
end;

constructor TRedisPoolCoordinator.CreateSentinel(const ARedis: IRedisClient; const AServiceName: string; const AEndpoints: TStrings; const ANodeID: string; const ATTL: Integer);
begin
  Create(ARedis, ANodeID, ATTL);
  FServiceName := AServiceName;
  FEndpoints.Assign(AEndpoints);
end;

destructor TRedisPoolCoordinator.Destroy;
begin
  FEndpoints.Free;
  inherited;
end;

function TRedisPoolCoordinator.GetNowUnix: string;
begin
  Result := IntToStr(DateTimeToUnix(Now));
end;

procedure TRedisPoolCoordinator._DiscoverMaster;
var
  LAddress: TArray<string>;
  I: Integer;
  LEndpoint: string;
  LParts: TArray<string>;
begin
  if (FServiceName = '') or (FEndpoints.Count = 0) then
    Exit;

  for I := 0 to FEndpoints.Count - 1 do
  begin
    LEndpoint := FEndpoints[I];
    LParts := LEndpoint.Split([':']);
    try
      FRedis.SetEndpoint(LParts[0], StrToIntDef(LParts[1], 26379));
      LAddress := FRedis.Execute('SENTINEL', ['get-master-addr-by-name', FServiceName]);
      if Length(LAddress) >= 2 then
      begin
        _SetCurrentEndpoint(LAddress[0], StrToInt(LAddress[1]));
        Exit;
      end;
    except
      // Try next sentinel
    end;
  end;
  raise Exception.Create('Could not discover Redis Master via Sentinels');
end;

procedure TRedisPoolCoordinator._SetCurrentEndpoint(const AHost: string; const APort: Integer);
begin
  FCurrentMasterHost := AHost;
  FCurrentMasterPort := APort;
  FRedis.SetEndpoint(FCurrentMasterHost, FCurrentMasterPort);
end;

function TRedisPoolCoordinator.ExecuteWithRetry(const AFunc: TFunc<string>): string;
var
  LRetry: Integer;
  LExc: Exception;
begin
  LExc := nil;
  for LRetry := 0 to FMaxRetries do
  begin
    try
      // If we use Sentinels and don't have a master yet, discover it
      if (FEndpoints.Count > 0) and (FCurrentMasterHost = '') then
        _DiscoverMaster;
        
      Result := AFunc();
      Exit;
    except
      on E: Exception do
      begin
        LExc := E;
        // ADR-014: Trigger re-discovery on connection error if Sentinels are configured
        if (FEndpoints.Count > 0) then
        begin
          FCurrentMasterHost := ''; // Invalidate cache
          try
            _DiscoverMaster;
          except
            // Ignore discovery error here, will retry or fail in the next loop iteration
          end;
        end;

        if LRetry < FMaxRetries then
          Sleep(100 * (LRetry + 1))
        else
          raise;
      end;
    end;
  end;
  if Assigned(LExc) then
    raise LExc;
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
  LToken: string;
begin
  LKey := 'DataEngine:Pool:Slots:' + ATenantID;
  LToken := FNodeID + ':' + TGuid.NewGuid.ToString.Replace('{', '').Replace('}', '');
  
  try
    LRes := ExecuteWithRetry(function: string
      begin
        Result := FRedis.Eval(LScript, [LKey], [LToken, GetNowUnix, IntToStr(FTTL), IntToStr(AMaxSlots)]);
      end);
    if LRes = 'OK' then
    begin
      ASlotToken := LToken;
      Result := True;
    end
    else
      Result := False;
  except
    Result := False; 
  end;
end;

function TRedisPoolCoordinator.RefreshSlot(const ATenantID: string; const ASlotToken: string): Boolean;
const
  LScript = 
    'local key = KEYS[1] ' +
    'local member = ARGV[1] ' +
    'local now = tonumber(ARGV[2]) ' +
    'local ttl = tonumber(ARGV[3]) ' +
    'redis.call("ZREMRANGEBYSCORE", key, "-inf", now - ttl) ' +
    'if redis.call("ZSCORE", key, member) then ' +
    '  redis.call("ZADD", key, now, member) ' +
    '  return "OK" ' +
    'else ' +
    '  return "EXPIRED" ' +
    'end';
var
  LRes: string;
  LKey: string;
  LToken: string;
begin
  LKey := 'DataEngine:Pool:Slots:' + ATenantID;
  LToken := ASlotToken;
  try
    LRes := ExecuteWithRetry(function: string
      begin
        Result := FRedis.Eval(LScript, [LKey], [LToken, GetNowUnix, IntToStr(FTTL)]);
      end);
    Result := LRes = 'OK';
  except
    Result := False;
  end;
end;

procedure TRedisPoolCoordinator.ReleaseSlot(const ATenantID: string; const ASlotToken: string);
const
  LScript = 'redis.call("ZREM", KEYS[1], ARGV[1]); return "OK"';
var
  LKey: string;
  LToken: string;
begin
  LKey := 'DataEngine:Pool:Slots:' + ATenantID;
  LToken := ASlotToken;
  try
    ExecuteWithRetry(function: string
      begin
        Result := FRedis.Eval(LScript, [LKey], [LToken]);
      end);
  except
    // Silent fail on release is acceptable
  end;
end;

function TRedisPoolCoordinator.GetGlobalMetrics(const ATenantID: string): TPoolMetrics;
const
  LScript = 
    'local key = KEYS[1] ' +
    'local now = tonumber(ARGV[1]) ' +
    'local ttl = tonumber(ARGV[2]) ' +
    'redis.call("ZREMRANGEBYSCORE", key, "-inf", now - ttl) ' +
    'return tostring(redis.call("ZCARD", key))';
var
  LCountStr: string;
  LKey: string;
begin
  LKey := 'DataEngine:Pool:Slots:' + ATenantID;
  FillChar(Result, SizeOf(Result), 0);
  try
    LCountStr := ExecuteWithRetry(function: string
      begin
        Result := FRedis.Eval(LScript, [LKey], ['', GetNowUnix, IntToStr(FTTL)]);
      end);
    Result.DistributedSlotsBusy := StrToIntDef(LCountStr, 0);
  except
    // Return empty metrics on failure
  end;
end;

function TRedisPoolCoordinator.GetSlotTTL: Integer;
begin
  Result := FTTL;
end;

end.
