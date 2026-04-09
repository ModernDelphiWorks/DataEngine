program TestsCacheInvalidation;

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
  DataEngine.MemCacheProvider in '..\Source\Core\DataEngine.MemCacheProvider.pas',
  DataEngine.SQLiteCacheProvider in '..\Source\Core\DataEngine.SQLiteCacheProvider.pas',
  DataEngine.DriverConnection in '..\Source\Core\DataEngine.DriverConnection.pas',
  DataEngine.DataSetSnapshot in '..\Source\Core\DataEngine.DataSetSnapshot.pas';

var
  GPhysicalCalls: Integer = 0;
  GInvalidations: Integer = 0;

procedure RunInvalidationTest(const ACacheProvider: IDBCacheProvider; const ALabel: string);
var
  LConnection: TFDConnection;
  LFactory: IDBConnection;
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LMonitor: TMonitorProc;
begin
  Writeln('--- DataEngine Cache Invalidation Test (' + ALabel + ') ---');
  GPhysicalCalls := 0;
  GInvalidations := 0;

  // 1. Setup Physical Connection (Source of Data)
  LConnection := TFDConnection.Create(nil);
  try
    LConnection.DriverName := 'SQLite';
    LConnection.Params.Values['Database'] := ':memory:';
    LConnection.Connected := True;
    
    // Create dummy source data
    LConnection.ExecSQL('CREATE TABLE Products (Id INTEGER, Name TEXT, Price CURRENCY)');
    LConnection.ExecSQL('INSERT INTO Products VALUES (1, ''Delphi Enterprise'', 5000)');

    // 2. Setup DataEngine Factory with Monitor
    LMonitor := procedure(const AParam: TMonitorParam)
      begin
        if AParam.Command.StartsWith('[CACHE MISS]') then
          Inc(GPhysicalCalls)
        else if AParam.Command.StartsWith('[CACHE INVALIDATE]') then
        begin
          Inc(GInvalidations);
          Writeln('   [MONITOR] Auto-Invalidation detected for Table: ' + AParam.Command);
        end;
      end;
      
    LFactory := TFactoryFireDAC.Create(LConnection, TDBEngineDriver.dnSQLite, LMonitor);
    LFactory.SetCacheProvider(ACacheProvider);
    
    // 3. STEP 1: Execute Query (Expected PHYSICAL CALL / MISS / Cache Population)
    Writeln('[STEP 1] Executing initial query...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Products';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('   Physical calls: ' + IntToStr(GPhysicalCalls));
    if GPhysicalCalls <> 1 then raise Exception.Create('Step 1 failed: Expected 1 physical call.');

    // 4. STEP 2: Execute Query AGAIN (Expected CACHE HIT / 0 Physical Calls)
    Writeln('[STEP 2] Executing same query (should HIT cache)...');
    GPhysicalCalls := 0;
    LDataSet := LQuery.ExecuteQuery;
    Writeln('   Physical calls: ' + IntToStr(GPhysicalCalls));
    if GPhysicalCalls <> 0 then raise Exception.Create('Step 2 failed: Expected 0 physical calls (Cache Hit).');

    // 5. STEP 3: Execute UPDATE (Expected CACHE INVALIDATION)
    Writeln('[STEP 3] Executing UPDATE on "Products" table...');
    LFactory.ExecuteDirect('UPDATE Products SET Price = Price * 1.1');
    Writeln('   Invalidations tracked: ' + IntToStr(GInvalidations));
    if GInvalidations < 1 then raise Exception.Create('Step 3 failed: Expected at least 1 cache invalidation event.');

    // 6. STEP 4: Execute Query AGAIN (Expected CACHE MISS / PHYSICAL CALL)
    Writeln('[STEP 4] Executing query after update (should MISS cache)...');
    GPhysicalCalls := 0;
    LDataSet := LQuery.ExecuteQuery;
    Writeln('   Physical calls: ' + IntToStr(GPhysicalCalls));
    if GPhysicalCalls <> 1 then raise Exception.Create('Step 4 failed: Expected 1 physical call (Cache Miss due to Invalidation).');

    Writeln('[SUCCESS] ' + ALabel + ' validation passed!');
    Writeln('');

  finally
    LConnection.Free;
  end;
end;

procedure TestTableExtraction;
var
  LTables: TArray<string>;
begin
  Writeln('--- TCacheManager.ExtractTables Utility Test ---');
  
  Writeln('Testing [SELECT FROM Products]...');
  LTables := TCacheManager.ExtractTables('SELECT * FROM Products');
  if (Length(LTables) <> 1) or (not SameText(LTables[0], 'Products')) then
      raise Exception.Create('Extraction failed for simple SELECT');
      
  Writeln('Testing [UPDATE Categories SET...]...');
  LTables := TCacheManager.ExtractTables('UPDATE Categories SET Name = ''Test''');
  if (Length(LTables) <> 1) or (not SameText(LTables[0], 'Categories')) then
      raise Exception.Create('Extraction failed for UPDATE');
      
  Writeln('Testing [DELETE FROM Orders]...');
  LTables := TCacheManager.ExtractTables('DELETE FROM Orders WHERE ID = 1');
  if (Length(LTables) <> 1) or (not SameText(LTables[0], 'Orders')) then
      raise Exception.Create('Extraction failed for DELETE');

  Writeln('Testing [INSERT INTO Logs]...');
  LTables := TCacheManager.ExtractTables('INSERT INTO Logs (Msg) VALUES (''Ok'')');
  if (Length(LTables) <> 1) or (not SameText(LTables[0], 'Logs')) then
      raise Exception.Create('Extraction failed for INSERT');

  Writeln('Testing JOINs [SELECT FROM A JOIN B]...');
  LTables := TCacheManager.ExtractTables('SELECT * FROM TableA JOIN TableB ON ...');
  if (Length(LTables) < 2) then
      raise Exception.Create('Extraction failed for JOINs');

  Writeln('[SUCCESS] Utility tests passed!');
  Writeln('');
end;

begin
  try
    TestTableExtraction;
    RunInvalidationTest(TMemCacheProvider.Create, 'Memory Provider');
    
    if FileExists('dataengine_cache.db') then DeleteFile('dataengine_cache.db');
    RunInvalidationTest(TSQLiteCacheProvider.Create('dataengine_cache.db'), 'SQLite Provider');
    
    Writeln('[DONE] All Invalidation Tests Completed Successfully.');
  except
    on E: Exception do
    begin
      Writeln('ERROR (' + E.ClassName + '): ' + E.Message);
      System.ExitCode := 1;
    end;
  end;
  Writeln('Press Enter to exit...');
  if not IsConsole then Readln;
end.
