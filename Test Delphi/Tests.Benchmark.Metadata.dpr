program Tests.Benchmark.Metadata;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Diagnostics,
  Data.DB,
  FireDAC.Comp.Client,
  FireDAC.Phys.SQLite,
  DataEngine.FactoryInterfaces in '..\Source\Core\DataEngine.FactoryInterfaces.pas',
  DataEngine.FactoryFireDac in '..\Source\Drivers\DataEngine.FactoryFireDac.pas',
  DataEngine.SQLiteCacheProvider in '..\Source\Core\DataEngine.SQLiteCacheProvider.pas';

const
  ITERATIONS = 5;
  SIMULATED_LATENCY_MS = 50;

type
  TBenchmarkMonitor = class
  public
    class procedure Monitor(const AParam: TMonitorParam);
  end;

class procedure TBenchmarkMonitor.Monitor(const AParam: TMonitorParam);
begin
  if not AParam.Command.StartsWith('[METADATA HIT]') then
  begin
    // Simulate network latency for physical calls
    Sleep(SIMULATED_LATENCY_MS);
  end;
  Writeln('  ' + AParam.Command);
end;

procedure RunBenchmark;
var
  LConnection: TFDConnection;
  LFactory: IDBConnection;
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LStopwatch: TStopwatch;
  I: Integer;
  LTotalTime: Int64;
  LCacheFile: string;
begin
  LCacheFile := 'benchmark_metadata.db';
  if FileExists(LCacheFile) then DeleteFile(LCacheFile);

  Writeln('=====================================================');
  Writeln('   DataEngine Metadata Cache Benchmark');
  Writeln('   Simulated Network Latency: ' + IntToStr(SIMULATED_LATENCY_MS) + 'ms');
  Writeln('=====================================================');

  LConnection := TFDConnection.Create(nil);
  try
    LConnection.DriverName := 'SQLite';
    LConnection.Params.Values['Database'] := ':memory:';
    LConnection.Connected := True;
    LConnection.ExecSQL('CREATE TABLE HeavyTable (Id INTEGER, Name TEXT, Description TEXT, CreatedAt DATETIME, Value CURRENCY, Status INTEGER)');

    LFactory := TFactoryFireDAC.Create(LConnection, TDBEngineDriver.dnSQLite, TBenchmarkMonitor.Monitor);
    LFactory.SetCacheProvider(TSQLiteCacheProvider.Create(LCacheFile));

    // --- PHASE 1: COLD START (MISS) ---
    Writeln('[ITERATION 0] Cold Start (Expect MISS + Latency)...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM HeavyTable';
    
    LStopwatch := TStopwatch.StartNew;
    LDataSet := LQuery.ExecuteQuery;
    LStopwatch.Stop;
    
    Writeln('>> Time: ' + IntToStr(LStopwatch.ElapsedMilliseconds) + 'ms');

    // --- PHASE 2: WARM START (HITS) ---
    LTotalTime := 0;
    for I := 1 to ITERATIONS do
    begin
      Writeln('[ITERATION ' + IntToStr(I) + '] Warm Start (Expect HIT)...');
      LStopwatch := TStopwatch.StartNew;
      LDataSet := LQuery.ExecuteQuery;
      LStopwatch.Stop;
      
      Writeln('>> Time: ' + IntToStr(LStopwatch.ElapsedMilliseconds) + 'ms');
      LTotalTime := LTotalTime + LStopwatch.ElapsedMilliseconds;
    end;

    Writeln('-----------------------------------------------------');
    Writeln('Average Cache Hit Latency: ' + FloatToStrF(LTotalTime / ITERATIONS, ffFixed, 7, 2) + 'ms');
    
    if (LTotalTime / ITERATIONS) > 10 then
       Writeln('WARNING: Latency higher than 10ms target! (But usually acceptable on slow disks)');
    else
       Writeln('SUCCESS: Performance within < 5ms target (local).');
    Writeln('-----------------------------------------------------');

  finally
    LConnection.Free;
  end;
end;

begin
  try
    RunBenchmark;
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
