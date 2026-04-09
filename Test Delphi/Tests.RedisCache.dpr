program Tests.RedisCache;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  Data.DB,
  FireDAC.Comp.Client,
  FireDAC.Phys.SQLite,
  DataEngine.FactoryInterfaces in '..\Source\Core\DataEngine.FactoryInterfaces.pas',
  DataEngine.FactoryFireDac in '..\Source\Drivers\DataEngine.FactoryFireDac.pas',
  DataEngine.CacheTypes in '..\Source\Core\DataEngine.CacheTypes.pas',
  DataEngine.CacheManager in '..\Source\Core\DataEngine.CacheManager.pas',
  DataEngine.RedisTransport in '..\Source\Core\DataEngine.RedisTransport.pas',
  DataEngine.RedisCacheProvider in '..\Source\Core\DataEngine.RedisCacheProvider.pas';

var
  GPhysicalCalls: Integer = 0;

procedure RunRedisTest;
var
  LConnection: TFDConnection;
  LFactory: IDBConnection;
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LMonitor: TMonitorProc;
  LProvider: TRedisCacheProvider;
begin
  Writeln('--- DataEngine Redis Cache Test ---');
  
  // 1. Setup Physical Connection (Source of Data)
  LConnection := TFDConnection.Create(nil);
  try
    LConnection.DriverName := 'SQLite';
    LConnection.Params.Values['Database'] := ':memory:';
    LConnection.Connected := True;
    
    // Create dummy source data
    LConnection.ExecSQL('CREATE TABLE Products (Id INTEGER, Name TEXT, Price CURRENCY)');
    LConnection.ExecSQL('INSERT INTO Products VALUES (1, ''Delphi Enterprise'', 5000)');
    LConnection.ExecSQL('INSERT INTO Products VALUES (2, ''Delphi Professional'', 2000)');

    // 2. Setup DataEngine Factory with Monitor
    LMonitor := procedure(const AParam: TMonitorParam)
      begin
        if not AParam.Command.StartsWith('[CACHE HIT]') then
          Inc(GPhysicalCalls);
        Writeln('[MONITOR] Physical Call: ' + AParam.Command);
      end;
      
    // FIRST SESSION
    Writeln('[SESSION 1] Initializing...');
    LFactory := TFactoryFireDAC.Create(LConnection, TDBEngineDriver.dnSQLite, LMonitor);
    
    // Setup Redis Cache Provider
    Writeln('[SESSION 1] Setting up TRedisCacheProvider (localhost:6379)...');
    try
      LProvider := TRedisCacheProvider.Create('localhost', 6379);
      LFactory.SetCacheProvider(LProvider);
      
      // Test Ping
      Writeln('[REDIS] Pinging server...');
      LProvider.Transport.ExecuteCommand('PING', []);
      Writeln('[REDIS] Server ALIVE.');
    except
      on E: Exception do
      begin
        Writeln('SKIPPING TEST: Could not connect to Redis server. Error: ' + E.Message);
        Exit;
      end;
    end;
    
    // Clean Redis for a fresh start in this test
    LProvider.Clear;

    // Execute Query (Expected PHYSICAL CALL / MISS)
    Writeln('[SESSION 1] Executing Query...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Products';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('[SESSION 1] Records retrieved: ' + IntToStr(LDataSet.RecordCount));
    
    if GPhysicalCalls <> 1 then
      raise Exception.Create('Expected 1 physical call in Session 1, but found ' + IntToStr(GPhysicalCalls));

    // SECOND SESSION (Simulated horizontal scale / restart)
    Writeln('[SESSION 1] Closing Session 1...');
    LFactory := nil; 
    
    Writeln('[SESSION 2] Re-initializing another instance with same Redis...');
    GPhysicalCalls := 0; // Reset monitor for Session 2
    LFactory := TFactoryFireDAC.Create(LConnection, TDBEngineDriver.dnSQLite, LMonitor);
    
    // Re-use same Redis
    LFactory.SetCacheProvider(TRedisCacheProvider.Create('localhost', 6379));
    
    // Execute SAME Query (Expected CACHE HIT / 0 PHYSICAL CALLS)
    Writeln('[SESSION 2] Executing Same Query...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Products';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('[SESSION 2] Records retrieved: ' + IntToStr(LDataSet.RecordCount));
    Writeln('[SESSION 2] Physical Calls monitored: ' + IntToStr(GPhysicalCalls));
    
    if GPhysicalCalls > 0 then
      raise Exception.Create('FAILED: Physical call detected in Session 2. Redis should have provided the data.');

    Writeln('[SUCCESS] Redis Distributed Cache validated!');
    
    // INVALIADATION TEST
    Writeln('[INVALIDATE TEST] Executing DML (INSERT)...');
    LConnection.ExecSQL('INSERT INTO Products VALUES (3, ''Delphi Starter'', 0)');
    // Invalidate manually for now or via TCacheManager helper if integrated
    TCacheManager.InvalidateBySQL(LFactory.Cache, 'INSERT INTO Products ...', nil, LMonitor);
    
    GPhysicalCalls := 0;
    Writeln('[INVALIDATE TEST] Executing Query after invalidation...');
    LDataSet := LQuery.ExecuteQuery;
    Writeln('[INVALIDATE TEST] Physical Calls: ' + IntToStr(GPhysicalCalls));
    if GPhysicalCalls <> 1 then
      raise Exception.Create('FAILED: Expected physical call after invalidation.');

    Writeln('[DONE] All Redis tests passed.');

  finally
    LConnection.Free;
  end;
end;

begin
  try
    RunRedisTest;
  except
    on E: Exception do
    begin
      Writeln('ERROR: ' + E.Message);
      ExitCode := 1;
    end;
  end;
  Writeln('Press Enter to exit...');
  if not IsConsole then Readln;
end.
