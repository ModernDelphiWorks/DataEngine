program Tests.BulkLoad;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Diagnostics,
  DB,
  FireDAC.Comp.Client,
  FireDAC.Phys.SQLite,
  FireDAC.Stan.Def,
  FireDAC.Stan.Async,
  FireDAC.DApt,
  DataEngine.FactoryInterfaces,
  DataEngine.FactoryFireDac;

var
  FConn: IDBConnection;
  FDConn: TFDConnection;
  FBulk: IDBBulkLoader;
  LStopwatch: TStopwatch;
  LIndex: Integer;
  LCount: Integer;
begin
  try
    LCount := 10000;
    FDConn := TFDConnection.Create(nil);
    try
      FDConn.DriverName := 'SQLite';
      FDConn.Params.Values['Database'] := ':memory:';
      FDConn.Connected := True;
      
      FConn := TFactoryFireDAC.Create(FDConn, dnSQLite);
      FConn.ExecuteDirect('CREATE TABLE TEST_BULK (ID INTEGER, NAME VARCHAR(100))');
      
      Writeln('Testing Single SQL Insert (10.000 records)...');
      LStopwatch := TStopwatch.StartNew;
      FConn.StartTransaction;
      for LIndex := 1 to LCount do
      begin
        FConn.ExecuteDirect(Format('INSERT INTO TEST_BULK (ID, NAME) VALUES (%d, ''Name %d'')', [LIndex, LIndex]));
      end;
      FConn.Commit;
      LStopwatch.Stop;
      Writeln(Format('Single SQL: %d ms', [LStopwatch.ElapsedMilliseconds]));
      
      FConn.ExecuteDirect('DELETE FROM TEST_BULK');
      
      Writeln('Testing Bulk Loader (10.000 records)...');
      LStopwatch := TStopwatch.StartNew;
      FBulk := FConn.BulkLoader;
      FBulk.TableName := 'TEST_BULK';
      FBulk.BatchSize := LCount;
      FBulk.ParamByName('ID').DataType := ftInteger;
      FBulk.ParamByName('NAME').DataType := ftString;
      FBulk.Prepare;
      
      for LIndex := 0 to LCount - 1 do
      begin
        FBulk.SetValue('ID', LIndex, LIndex + 1);
        FBulk.SetValue('NAME', LIndex, 'Name ' + IntToStr(LIndex + 1));
      end;
      
      FConn.StartTransaction;
      FBulk.Execute(LCount);
      FConn.Commit;
      LStopwatch.Stop;
      Writeln(Format('Bulk Loader: %d ms', [LStopwatch.ElapsedMilliseconds]));
      
      Writeln('Press Enter to exit...');
      Readln;
    finally
      FDConn.Free;
    end;
  except
    on E: Exception do
    begin
      Writeln(E.ClassName, ': ', E.Message);
      Readln;
    end;
  end;
end.
