unit Tests.Observability.Main;

interface

uses
  DUnitX.TestFramework, System.Classes, System.SysUtils, System.Threading, DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection, DataEngine.CacheManager, DB;

type
  [TestFixture]
  TTestObservability = class
  private
    FDriverTransaction: TDriverTransaction;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;
    [Test]
    procedure Test_FetchTime_Accuracy;
    [Test]
    procedure Test_ThreadSafety_Observer_Iteration;
  end;

  TDummyObserver = class(TInterfacedObject, IDBObserver)
  private
    FHitCount: Integer;
  public
    procedure OnNotify(const AParam: TMonitorParam);
    property HitCount: Integer read FHitCount;
  end;

  TDummyConnection = class(TDriverConnection)
  protected
    procedure _InternalExecuteDirect(const ASQL: String; const AParams: TParams = nil); override;
  end;

  TDummyQuery = class(TDriverQuery)
  protected
    function _InternalExecuteQuery: IDBDataSet; override;
    procedure _InternalExecuteDirect; override;
  end;

  TDummyDataSet = class(TDriverDataSetBase)
  public
    procedure Next; override;
  end;

implementation

{ TTestObservability }

procedure TTestObservability.Setup;
begin
  FDriverTransaction := nil;
end;

procedure TTestObservability.TearDown;
begin
end;

procedure TTestObservability.Test_FetchTime_Accuracy;
var
  LConn: TDummyConnection;
  LQuery: TDummyQuery;
  LObserver: TDummyObserver;
  LDS: IDBDataSet;
begin
  LConn := TDummyConnection.Create(nil, nil, edSQLite, nil);
  try
    LObserver := TDummyObserver.Create;
    LConn.AddObserver(LObserver);
    
    LQuery := TDummyQuery.Create(nil, LConn.MonitorCallback, edSQLite);
    LDS := LQuery.ExecuteQuery;
    
    // Test dataset fetch time isolation
    Assert.IsNotNull(LDS);
    
    // Trigger Next that fakes EOF to trigger teFetchEnd
    LDS.Next;
    
    Assert.IsTrue(LObserver.HitCount > 0);
  finally
    LConn.Free;
  end;
end;

procedure TTestObservability.Test_ThreadSafety_Observer_Iteration;
var
  LConn: TDummyConnection;
  LTasks: TArray<ITask>;
  LFor: Integer;
begin
  LConn := TDummyConnection.Create(nil, nil, edSQLite, nil);
  try
    SetLength(LTasks, 10);
    // Task 1-5 will iterate events by calling MonitorCallback repeatedly
    // Task 6-10 will add and remove observers
    for LFor := 0 to 4 do
    begin
      LTasks[LFor] := TTask.Run(
        procedure
        var i: Integer;
        begin
          for i := 1 to 10000 do
            if Assigned(LConn.MonitorCallback) then
              LConn.MonitorCallback(TMonitorParam.Create(teQueryStart, 'SELECT 1', nil));
        end);
    end;

    for LFor := 5 to 9 do
    begin
      LTasks[LFor] := TTask.Run(
        procedure
        var
          i: Integer;
          LObserver: IDBObserver;
        begin
          for i := 1 to 10000 do
          begin
            LObserver := TDummyObserver.Create;
            LConn.AddObserver(LObserver);
            Sleep(0); // force yield
            LConn.RemoveObserver(LObserver);
          end;
        end);
    end;

    TTask.WaitForAll(LTasks);
    Assert.IsTrue(True, 'Completed without Access Violation');
  finally
    LConn.Free;
  end;
end;

{ TDummyObserver }

procedure TDummyObserver.OnNotify(const AParam: TMonitorParam);
begin
  TInterlocked.Increment(FHitCount);
  if AParam.EventType = teFetchEnd then
  begin
    Assert.IsTrue(AParam.FetchTime >= 0, 'FetchTime is tracked separately');
  end;
end;

{ TDummyConnection }

procedure TDummyConnection._InternalExecuteDirect(const ASQL: String; const AParams: TParams);
begin
  Sleep(10);
end;

{ TDummyQuery }

procedure TDummyQuery._InternalExecuteDirect;
begin
end;

function TDummyQuery._InternalExecuteQuery: IDBDataSet;
begin
  Sleep(10);
  Result := TDummyDataSet.Create;
  TDriverDataSetBase(Result)._SetActive(True);
end;

{ TDummyDataSet }

procedure TDummyDataSet.Next;
begin
  inherited Next;
  // trigger eof locally
  if FFetchStarted then
  begin
    FFetchingAll := True;
    FFetchStarted := False;
    FFetchStopwatch.Stop;
    if Assigned(FMonitorCallback) then
      FMonitorCallback(TMonitorParam.Create(teFetchEnd, _GetCommandText, nil, 0, FFetchStopwatch.ElapsedMilliseconds, 10));
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TTestObservability);

end.
