program Tests.Pool.Distributed;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.Threading,
  System.Diagnostics,
  System.DateUtils,
  System.Generics.Collections,
  System.SyncObjs,
  DataEngine.FactoryInterfaces,
  DataEngine.PoolConnection,
  DataEngine.PoolCoordinator.Redis,
  Tests.MultiTenant.Mocks;

type
  { Mock Redis Client: Simulates ZSET and Lua script execution }
  TMockRedisClient = class(TInterfacedObject, IRedisClient)
  private
    FSlots: TDictionary<string, TDictionary<string, Int64>>; // Key -> { Member -> Score }
    FLock: TCriticalSection;
    FCurrentHost: string;
    FCurrentPort: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    function Eval(const AScript: string; const AKeys: array of string; const AArgs: array of string): string;
    function Execute(const ACommand: string; const AArgs: array of string): TArray<string>;
    procedure SetEndpoint(const AHost: string; const APort: Integer);
    property CurrentHost: string read FCurrentHost;
    property CurrentPort: Integer read FCurrentPort;
  end;

constructor TMockRedisClient.Create;
begin
  inherited Create;
  FSlots := TObjectDictionary<string, TDictionary<string, Int64>>.Create([doOwnsValues]);
  FLock := TCriticalSection.Create;
end;

destructor TMockRedisClient.Destroy;
begin
  FSlots.Free;
  FLock.Free;
  inherited;
end;

procedure TMockRedisClient.SetEndpoint(const AHost: string; const APort: Integer);
begin
  FCurrentHost := AHost;
  FCurrentPort := APort;
end;

function TMockRedisClient.Execute(const ACommand: string; const AArgs: array of string): TArray<string>;
begin
  if (ACommand = 'SENTINEL') and (AArgs[0] = 'get-master-addr-by-name') then
  begin
    if AArgs[1] = 'mymaster' then
    begin
      SetLength(Result, 2);
      Result[0] := '127.0.0.1';
      Result[1] := '6379';
    end
    else
      raise Exception.Create('Sentinel: master not found');
  end
  else
    raise Exception.Create('Mock Redis: Command not implemented: ' + ACommand);
end;

function TMockRedisClient.Eval(const AScript: string; const AKeys: array of string; const AArgs: array of string): string;
var
  LKey, LMember: string;
  LNow, LTTL, LMax: Int64;
  LDict: TDictionary<string, Int64>;
  LCount: Integer;
  LM: string;
begin
  FLock.Acquire;
  try
    LKey := AKeys[0];
    if not FSlots.TryGetValue(LKey, LDict) then
    begin
      LDict := TDictionary<string, Int64>.Create;
      FSlots.Add(LKey, LDict);
    end;

    if Pos('ZREMRANGEBYSCORE', AScript) > 0 then
    begin
      LNow := StrToInt64(AArgs[1]);
      LTTL := StrToInt64(AArgs[2]);

      var LToExpire := TList<string>.Create;
      try
        for LM in LDict.Keys do
          if LDict[LM] <= (LNow - LTTL) then
            LToExpire.Add(LM);
        for LM in LToExpire do
          LDict.Remove(LM);
      finally
        LToExpire.Free;
      end;
    end;

    if (Pos('ZADD', AScript) > 0) or (Pos('PREFILL', AScript) > 0) then
    begin
      LMember := AArgs[0];
      LMax := StrToInt64(AArgs[3]);
      LCount := LDict.Count;
      if (LCount < LMax) or LDict.ContainsKey(LMember) then
      begin
        LDict.AddOrSetValue(LMember, StrToInt64(AArgs[1]));
        Result := 'OK';
      end
      else
      begin
        Result := 'BUSY';
      end;
      Exit;
    end;

    if (Pos('ZCARD', AScript) > 0) and (Pos('return tostring', AScript) > 0) then
    begin
       Result := IntToStr(LDict.Count);
       Exit;
    end;

    if Pos('ZSCORE', AScript) > 0 then
    begin
      LMember := AArgs[0];
      if LDict.ContainsKey(LMember) then
      begin
        LDict.AddOrSetValue(LMember, StrToInt64(AArgs[1]));
        Result := 'OK';
      end
      else
        Result := 'EXPIRED';
      Exit;
    end;

    if Pos('ZREM', AScript) > 0 then
    begin
      LMember := AArgs[0];
      LDict.Remove(LMember);
      Result := 'OK';
      Exit;
    end;
  finally
    FLock.Release;
  end;
end;

procedure TestSentinelDiscovery;
var
  LRedis: TMockRedisClient;
  LCoordinator: TRedisPoolCoordinator;
  LEndpoints: TStrings;
  LToken: string;
begin
  Writeln('Testing Sentinel Discovery...');
  LRedis := TMockRedisClient.Create;
  LEndpoints := TStringList.Create;
  try
    LEndpoints.Add('192.168.1.100:26379');
    LEndpoints.Add('192.168.1.101:26379');
    
    LCoordinator := TRedisPoolCoordinator.CreateSentinel(LRedis, 'mymaster', LEndpoints, 'NodeS', 60);
    try
      // The first call should trigger discovery
      if LCoordinator.AcquireSlot('TenantS', 10, 1000, LToken) then
      begin
        Writeln('Slot acquired through Sentinel-discovered master.');
        if LRedis.CurrentHost <> '127.0.0.1' then
          raise Exception.Create('Test failed: Current host should be 127.0.0.1 (discovered), but was ' + LRedis.CurrentHost);
      end
      else
        raise Exception.Create('Test failed: Could not acquire slot');
    finally
      LCoordinator.Free;
    end;
  finally
    LEndpoints.Free;
  end;
  Writeln('Sentinel test passed.');
end;

procedure TestDistributedLimit;
var
  LRedis: IRedisClient;
  LCoordinator: IDBPoolCoordinator;
  LTasks: TArray<ITask>;
  LFailedCount: Integer;
begin
  Writeln('Testing Distributed Global Limit...');
  PoolManager.Clear;
  PoolManager.SetDefaultTimeout(1000);
  LRedis := TMockRedisClient.Create;
  LCoordinator := TRedisPoolCoordinator.Create(LRedis, 'Node1', 10);
  
  // Pre-fill 5 slots
  for var K := 1 to 5 do
    LRedis.Eval('ZREMRANGEBYSCORE_PREFILL', ['DataEngine:Pool:Slots:GlobalTenant'], ['Node2:Ext' + IntToStr(K), IntToStr(DateTimeToUnix(Now)), '10', '10']);

  PoolManager.SetCoordinator(LCoordinator);
  
  LFailedCount := 0;
  SetLength(LTasks, 10);
  
  for var I := 0 to 9 do
  begin
    LTasks[I] := TTask.Run(
      procedure
      var
        LConn: IDBConnection;
      begin
        try
          try
            LConn := PoolManager.AcquireConnection(
              'GlobalTenant',
              function: IDBConnection begin Result := TStubConnection.Create; end,
              10
            );
            if Assigned(LConn) then
            begin
              Sleep(2000); // Hold for 2s, longer than the 1s timeout of other tasks
              PoolManager.ReleaseConnection('GlobalTenant', LConn);
            end;
          except
            on E: Exception do
            begin
              TInterlocked.Increment(LFailedCount);
            end;
          end;
        except
          // Catch all to avoid thread termination
        end;
      end
    );
  end;

  TTask.WaitForAll(LTasks);
  
  Writeln(Format('Successful: %d, Failed: %d (Expected: 5)', [10 - LFailedCount, LFailedCount]));
  
  if LFailedCount <> 5 then
    raise Exception.Create(Format('Test failed: Limit was not enforced correctly (Expected 5 failures, got %d).', [LFailedCount]));
end;

procedure TestDistributedHeartbeat;
var
  LRedis: TMockRedisClient;
  LCoordinator: IDBPoolCoordinator;
  LConn: IDBConnection;
  LMetrics: TPoolMetrics;
begin
  Writeln('Testing Distributed Heartbeat (Resiliency)...');
  LRedis := TMockRedisClient.Create;
  // TTL of 6 seconds. Renewal should happen at 3s.
  LCoordinator := TRedisPoolCoordinator.Create(LRedis, 'NodeH', 6);
  PoolManager.SetCoordinator(LCoordinator);

  LConn := PoolManager.AcquireConnection(
    'HeartbeatTenant',
    function: IDBConnection begin Result := TStubConnection.Create; end,
    1,
    1000
  );

  Writeln('Connection acquired. Holding for 10 seconds (TTL is 6s)...');
  // Hold connection for 10 seconds. Without heartbeat, slot would expire in 6s.
  // With dynamic logic (TTL/2 = 3s), it should renew at 3s and 6s and 9s.
  Sleep(10000); 
  
  LMetrics := PoolManager.GetMetrics('HeartbeatTenant');
  Writeln(Format('Metrics after 10s: Busy=%d, GlobalBusy=%d', [LMetrics.LocalBusyConnections, LMetrics.DistributedSlotsBusy]));
  
  if LMetrics.DistributedSlotsBusy = 0 then
    raise Exception.Create('Test failed: Slot expired despite heartbeat');

  PoolManager.ReleaseConnection('HeartbeatTenant', LConn);
  Writeln('Heartbeat test passed.');
end;

procedure TestDistributedMetrics;
var
  LRedis: TMockRedisClient;
  LCoordinator: IDBPoolCoordinator;
  LMetrics: TPoolMetrics;
begin
  Writeln('Testing Distributed Metrics (Observability)...');
  LRedis := TMockRedisClient.Create;
  LCoordinator := TRedisPoolCoordinator.Create(LRedis, 'NodeM', 60);
  PoolManager.SetCoordinator(LCoordinator);
  
  var LConn1 := PoolManager.AcquireConnection('MetricTenant', function: IDBConnection begin Result := TStubConnection.Create; end, 5);
  var LConn2 := PoolManager.AcquireConnection('MetricTenant', function: IDBConnection begin Result := TStubConnection.Create; end, 5);
  
  LMetrics := PoolManager.GetMetrics('MetricTenant');
  Writeln(Format('Hits: %d, LocalBusy: %d, GlobalBusy: %d', [LMetrics.PoolHits, LMetrics.LocalBusyConnections, LMetrics.DistributedSlotsBusy]));
  
  if (LMetrics.LocalBusyConnections <> 2) or (LMetrics.DistributedSlotsBusy <> 2) then
    raise Exception.Create('Test failed: Metrics are incorrect');

  PoolManager.ReleaseConnection('MetricTenant', LConn1);
  PoolManager.ReleaseConnection('MetricTenant', LConn2);
  
  LMetrics := PoolManager.GetMetrics('MetricTenant');
  if LMetrics.LocalBusyConnections <> 0 then
    raise Exception.Create('Test failed: LocalBusy should be 0');

  Writeln('Metrics test passed.');
end;

begin
  try
    TestDistributedLimit;
    Writeln('');
    TestDistributedMetrics;
    Writeln('');
    TestDistributedHeartbeat;
    Writeln('');
    TestSentinelDiscovery;
    Writeln('Validation: SUCCESS');
  except
    on E: Exception do
    begin
      Writeln('Validation: FAILED');
      Writeln(E.ClassName + ': ' + E.Message);
      System.ExitCode := 1;
    end;
  end;
end.
