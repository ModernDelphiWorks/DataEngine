program HorseSample;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  Horse,
  Horse.DataEngine,
  DataEngine.FactoryInterfaces,
  DataEngine.PoolConnection,
  DataEngine.FactoryFireDac;

begin
  try
    { 
      Register the HorseDataEngine middleware.
      We pass the PoolManager instance and a connection factory.
      The middleware will handle acquiring and releasing connections 
      back to the pool for every request.
    }
    THorse.Use(HorseDataEngine(PoolManager, 
      function: IDBConnection
      begin
        {
          For this sample, we use an in-memory SQLite database via FireDAC.
          In a real production environment, you would configure your real database connection here.
        }
        Result := TFactoryFireDac.Create(nil, dnSQLite);
        Result.Connect;
        
        { Initialize a simple schema for the sample }
        Result.ExecuteDirect('CREATE TABLE IF NOT EXISTS USERS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME VARCHAR(100))');
      end));

    { Route to list users }
    THorse.Get('/users',
      procedure(Req: THorseRequest; Res: THorseResponse; Next: TNextProc)
      var
        LConn: IDBConnection;
        LDataSet: IDBDataSet;
      begin
        { Access the connection injected by the middleware }
        LConn := Req.SessionIDBConnection;
        
        LDataSet := LConn.CreateDataSet('SELECT * FROM USERS');
        LDataSet.Open;
        
        Res.Send('Total users in database: ' + LDataSet.RecordCount.ToString);
      end);

    { Route to add a user }
    THorse.Post('/users',
      procedure(Req: THorseRequest; Res: THorseResponse; Next: TNextProc)
      var
        LConn: IDBConnection;
        LName: string;
      begin
        LConn := Req.SessionIDBConnection;
        LName := 'User ' + FormatDateTime('hh:nn:ss', Now);
        
        with LConn.CreateQuery do
        begin
          CommandText := 'INSERT INTO USERS (NAME) VALUES (:NAME)';
          ParamByName('NAME').AsString := LName;
          ExecuteDirect;
        end;
        
        Res.Status(201).Send('User created: ' + LName);
      end);

    Writeln('Horse server started on http://localhost:9000');
    THorse.Listen(9000);
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
