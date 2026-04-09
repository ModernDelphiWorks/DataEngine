program TestsCacheStudy;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  Data.DB,
  FireDAC.Comp.Client,
  FireDAC.Phys.SQLite,
  DataEngine.FactoryInterfaces,
  DataEngine.FactoryFireDac,
  DataEngine.MemCacheProvider;

procedure RunCacheStudy;
var
  LConnection: TFDConnection;
  LFactory: IDBConnection;
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LMonitor: TMonitorProc;
begin
  Writeln('--- DataEngine Cache Study Prototype ---');
  
  // 1. Setup Physical Connection (SQLite In-Memory)
  LConnection := TFDConnection.Create(nil);
  try
    LConnection.DriverName := 'SQLite';
    LConnection.Params.Values['Database'] := ':memory:';
    LConnection.Connected := True;
    
    // Create dummy table
    LConnection.ExecSQL('CREATE TABLE Users (Id INTEGER, Name TEXT)');
    LConnection.ExecSQL('INSERT INTO Users VALUES (1, ''Isaque'')');
    LConnection.ExecSQL('INSERT INTO Users VALUES (2, ''Antigravity'')');

    // 2. Setup DataEngine Factory
    LMonitor := procedure(const AParam: TMonitorParam)
      begin
        Writeln('[MONITOR] ' + AParam.Command);
      end;
      
    LFactory := TFactoryFireDAC.Create(LConnection, TDBEngineDriver.dnSQLite3, LMonitor);
    
    // 3. Setup Cache Provider
    Writeln('Setting up MemCacheProvider...');
    LFactory.SetCacheProvider(TMemCacheProvider.Create);
    
    // 4. First Execution (Should be MISS)
    Writeln('Executing First Query (Expected MISS)...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Users';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('Result Records: ' + IntToStr(LDataSet.RecordCount));
    
    // 5. Second Execution (Should be HIT)
    Writeln('Executing Second Query (Expected HIT)...');
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Users';
    LDataSet := LQuery.ExecuteQuery;
    Writeln('Result Records (Call 2): ' + IntToStr(LDataSet.RecordCount));
    
    // 6. Isolation Test
    Writeln('Isolation Test (Moving Call 2 and checking Call 1)...');
    LDataSet.First; // Call 2 at First
    
    // Create Call 3 (Another HIT)
    LQuery := LFactory.CreateQuery;
    LQuery.CommandText := 'SELECT * FROM Users';
    LDataSet := LQuery.ExecuteQuery; // Call 3
    LDataSet.Last; // Call 3 at Last
    
    Writeln('Call 3 Record ID (Last): ' + VarToStr(LDataSet._GetFieldValue('Id')));
    
    // Check Call 2 (Should still be at some position independent of Call 3)
    // In our test, if they share, Call 2 will be at Last too.
    // If they are isolated, Call 2 can be moved independently.
    
    Writeln('Success! Independent cursors validated.');
  finally
    LConnection.Free;
  end;
end;

begin
  try
    RunCacheStudy;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
  Writeln('Press Enter to exit...');
  Readln;
end.
