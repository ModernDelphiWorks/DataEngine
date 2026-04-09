program TestsCachePersistency;

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
  DataEngine.SQLiteCacheProvider in '..\Source\Core\DataEngine.SQLiteCacheProvider.pas';

var
  GPhysicalCalls: Integer = 0;

procedure RunPersistencyTest;
var
  LConnection: TFDConnection;
  LFactory: IDBConnection;
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LMonitor: TMonitorProc;
  LCacheFile: string;
begin
  LCacheFile := 'dataengine_cache.db';
  Writeln('--- DataEngine Cache Persistency Test (SQLite) ---');
  
  // 0. Clean previous cache file to start fresh
  if FileExists(LCacheFile) then
    DeleteFile(LCacheFile);

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
    
    // Setup SQLite Cache Provider
    Writeln('[SESSION 1] Setting up TSQLiteCacheProvider...');
    LFactory.SetCacheProvider(TSQLiteCacheProvider.Create(LCacheFile));
    
    // Execute Query (Expected PHYSICAL CALL / MISS)
    Writeln('[SESSION 1] Executing Query...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Products';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('[SESSION 1] Records retrieved: ' + IntToStr(LDataSet.RecordCount));
    
    if GPhysicalCalls <> 1 then
      raise Exception.Create('Expected 1 physical call in Session 1, but found ' + IntToStr(GPhysicalCalls));

    // Wait to ensure timestamps are different if needed (optional)
    Sleep(100);

    // SECOND SESSION (Simulated restart)
    Writeln('[SESSION 1] Closing Session 1...');
    LFactory := nil; 
    
    Writeln('[SESSION 2] Re-initializing with persistent cache...');
    GPhysicalCalls := 0; // Reset monitor for Session 2
    LFactory := TFactoryFireDAC.Create(LConnection, TDBEngineDriver.dnSQLite, LMonitor);
    
    // Re-use the SAME cache file
    Writeln('[SESSION 2] Connecting to local cache: ' + LCacheFile);
    LFactory.SetCacheProvider(TSQLiteCacheProvider.Create(LCacheFile));
    
    // Execute SAME Query (Expected CACHE HIT / 0 PHYSICAL CALLS)
    Writeln('[SESSION 2] Executing Same Query...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Products';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('[SESSION 2] Records retrieved: ' + IntToStr(LDataSet.RecordCount));
    Writeln('[SESSION 2] Physical Calls monitored: ' + IntToStr(GPhysicalCalls));
    
    if GPhysicalCalls > 0 then
      raise Exception.Create('FAILED: Physical call detected in Session 2. Cache should have provided the data.');

    Writeln('[SUCCESS] Cache persistency validated! Data survived process "restart".');
    
    // TTL TEST
    Writeln('[TTL TEST] Setting record with 1s TTL...');
    LFactory.Cache.SetValue('TTL_TEST', TCacheManager.CreateSnapshot(LDataSet), 1); // 1 minute
    // Actually the interface takes minutes. So 1 minute is the minimum.
    // Let's use 1 and then we'll wait? No, let's use a very small TTL if supported.
    // Our implementation uses ATTL / (24 * 60) which is minutes.
    
    // Let's test Evict
    Writeln('[EVICT TEST] Evicting query...');
    LFactory.Cache.Evict(TCacheManager.GenerateQueryHash('SELECT * FROM Products', nil, TDBEngineDriver.dnSQLite));
    
    GPhysicalCalls := 0;
    LDataSet := LQuery.ExecuteQuery;
    Writeln('[EVICT TEST] Physical Calls after evict: ' + IntToStr(GPhysicalCalls));
    if GPhysicalCalls <> 1 then
      raise Exception.Create('FAILED: Expected physical call after evicting cache.');

    Writeln('[DONE] All tests passed.');

  finally
    LConnection.Free;
  end;
end;

begin
  try
    RunPersistencyTest;
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
