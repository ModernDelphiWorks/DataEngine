unit Tests.AsyncLoading;

interface

uses
  DUnitX.TestFramework,
  DataEngine.FactoryInterfaces,
  DataEngine.FactoryFireDac,
  FireDAC.Comp.Client,
  System.SysUtils,
  System.Threading;

type
  [TestFixture]
  TTestsAsyncLoading = class
  private
    FConn: IDBConnection;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure TestFireDACStreaming;
    [Test]
    procedure TestParallelStreaming;
  end;

implementation

uses
  DataEngine.Consts;

procedure TTestsAsyncLoading.Setup;
var
  LFD: TFDConnection;
begin
  LFD := TFDConnection.Create(nil);
  LFD.DriverName := 'SQLite';
  LFD.Params.Values['Database'] := ':memory:';
  FConn := TFactoryFireDAC.Create(LFD, dnSQLite);
  FConn.Connect;
  
  // Create dummy data
  FConn.ExecuteDirect('CREATE TABLE TEST_STREAM (ID INTEGER PRIMARY KEY, NAME TEXT)');
  FConn.StartTransaction;
  try
    for var i := 1 to 1000 do
      FConn.ExecuteDirect(Format('INSERT INTO TEST_STREAM (ID, NAME) VALUES (%d, "NAME %d")', [i, i]));
    FConn.Commit;
  except
    FConn.Rollback;
    raise;
  end;
end;

procedure TTestsAsyncLoading.TearDown;
begin
  FConn := nil;
end;

procedure TTestsAsyncLoading.TestFireDACStreaming;
var
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LCount: Integer;
begin
  LQuery := FConn.CreateQuery;
  LQuery.CommandText := 'SELECT * FROM TEST_STREAM';
  LQuery.FetchOptions := TFetchOptions.Create(fmOnDemand, 50);
  
  LDataSet := LQuery.ExecuteQuery;
  
  Assert.IsFalse(LDataSet.FetchingAll, 'Should be in streaming mode');
  
  LCount := 0;
  while not LDataSet.Eof do
  begin
    Inc(LCount);
    LDataSet.Next;
  end;
  
  Assert.AreEqual(1000, LCount, 'Should have fetched all records via streaming');
end;

procedure TTestsAsyncLoading.TestParallelStreaming;
const
  NUM_THREADS = 5;
var
  LTasks: TArray<ITask>;
begin
  SetLength(LTasks, NUM_THREADS);
  for var i := 0 to NUM_THREADS - 1 do
  begin
    LTasks[i] := TTask.Run(
      procedure
      var
        LQuery: IDBQuery;
        LDataSet: IDBDataSet;
        LCount: Integer;
      begin
        LQuery := FConn.CreateQuery;
        LQuery.CommandText := 'SELECT * FROM TEST_STREAM';
        LQuery.FetchOptions := TFetchOptions.Create(fmOnDemand, 10);
        LDataSet := LQuery.ExecuteQuery;
        
        LCount := 0;
        while not LDataSet.Eof do
        begin
          Inc(LCount);
          LDataSet.Next;
          TThread.Sleep(1); // Simulate work
        end;
        Assert.AreEqual(1000, LCount);
      end
    );
  end;
  
  TTask.WaitForAll(LTasks);
end;

initialization
  TDUnitX.RegisterTestFixture(TTestsAsyncLoading);

end.
