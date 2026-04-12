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
  public
    constructor Create;
    destructor Destroy; override;
    function Eval(const AScript: string; const AKeys: array of string; const AArgs: array of string): string;
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
    if Pos('ZREMRANGEBYSCORE', AScript) > 0 then
    begin
      LKey := AKeys[0];
      LMember := AArgs[0];
      LNow := StrToInt64(AArgs[1]);
      LTTL := StrToInt64(AArgs[2]);
      LMax := StrToInt64(AArgs[3]);

      if not FSlots.TryGetValue(LKey, LDict) then
      begin
        LDict := TDictionary<string, Int64>.Create;
        FSlots.Add(LKey, LDict);
      end;

      // Simulate ZREMRANGEBYSCORE key -inf now-ttl
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

      LCount := LDict.Count;

      if (LCount < LMax) or LDict.ContainsKey(LMember) then
      begin
        LDict.AddOrSetValue(LMember, LNow);
        Result := 'OK';
      end
      else
        Result := 'BUSY';
    end
    else if Pos('ZREM', AScript) > 0 then
    begin
      LKey := AKeys[0];
      LMember := AArgs[0];
      if FSlots.TryGetValue(LKey, LDict) then
        LDict.Remove(LMember);
      Result := 'OK';
    end
    else
      Result := 'ERROR';
  finally
    FLock.Release;
  end;
end;

procedure TestDistributedLimit;
var
  LRedis: IRedisClient;
  LCoordinator: IDBPoolCoordinator;
  LTasks: TArray<ITask>;
  LFailedCount: Integer;
begin
  Writeln('Testing Distributed Global Limit...');
  LRedis := TMockRedisClient.Create;
  // TTL of 10 seconds for testing
  LCoordinator := TRedisPoolCoordinator.Create(LRedis, 'Node1', 10);
  
  // Pre-fill 5 slots in Redis for "Node2" to simulate distributed usage
  var LDummyToken: string;
  for var K := 1 to 5 do
    LRedis.Eval('ZREMRANGEBYSCORE_PREFILL', ['DataEngine:Pool:Slots:GlobalTenant'], ['Node2:Ext' + IntToStr(K), IntToStr(DateTimeToUnix(Now)), '10', '10']);

  PoolManager.SetCoordinator(LCoordinator);
  
  LFailedCount := 0;
  SetLength(LTasks, 10);
  
  // Try to acquire 10 connections locally. Total global will be 5 (Node2) + 10 (Node1) = 15.
  // Since Global Limit is 10, only 5 more should be allowed.
  for var I := 0 to 9 do
  begin
    LTasks[I] := TTask.Run(
      procedure
      var
        LConn: IDBConnection;
      begin
        try
          LConn := PoolManager.AcquireConnection(
            'GlobalTenant',
            function: IDBConnection begin Result := TStubConnection.Create; end,
            10, // Max Connections locally
            1000 // 1 second timeout
          );
          if Assigned(LConn) then
          begin
            Sleep(500); // Hold for 500ms
            PoolManager.ReleaseConnection('GlobalTenant', LConn);
          end;
        except
          on E: Exception do
          begin
            TInterlocked.Increment(LFailedCount);
          end;
        end;
      end
    );
  end;

  TTask.WaitForAll(LTasks);
  
  Writeln(Format('Successful: %d, Failed: %d (Expected: 5)', [10 - LFailedCount, LFailedCount]));
  
  if LFailedCount <> 5 then
    raise Exception.Create(Format('Test failed: Limit was not enforced correctly (Expected 5 failures, got %d).', [LFailedCount]));
end;

begin
  try
    TestDistributedLimit;
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
