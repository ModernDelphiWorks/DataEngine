program Tests.IsolationLevels;

{$APPTYPE CONSOLE}

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  FireDAC.Comp.Client,
  FireDAC.Phys.SQLite,
  DataEngine.FactoryInterfaces,
  DataEngine.FactoryFireDac;

var
  FConn: IDBConnection;
  FDConn: TFDConnection;
begin
  try
    Writeln('Testing Transaction Isolation Levels...');
    FDConn := TFDConnection.Create(nil);
    try
      FDConn.DriverName := 'SQLite';
      FDConn.Params.Values['Database'] := ':memory:';
      FDConn.Connected := True;
      
      FConn := TFactoryFireDAC.Create(FDConn, dnSQLite);
      
      Writeln('Testing ilReadCommitted...');
      FConn.UseTransaction('DEFAULT');
      FConn.StartTransaction(ilReadCommitted);
      Writeln('Success.');
      FConn.Commit;

      Writeln('Testing ilSerializable...');
      FConn.StartTransaction(ilSerializable);
      Writeln('Success.');
      FConn.Commit;

      Writeln('Testing ilSnapshot (SQLite fallback)...');
      FConn.StartTransaction(ilSnapshot);
      Writeln('Success.');
      FConn.Commit;
      
      Writeln('All basic Core tests passed.');
    finally
      FDConn.Free;
    end;
  except
    on E: Exception do
    begin
      Writeln(E.ClassName, ': ', E.Message);
      Halt(1);
    end;
  end;
end.
