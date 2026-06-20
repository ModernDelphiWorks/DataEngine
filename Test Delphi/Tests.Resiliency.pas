unit Tests.Resiliency;

interface

uses
  DUnitX.TestFramework,
  SysUtils,
  Classes,
  DB,
  DataEngine.FactoryInterfaces,
  DataEngine.GuardConnection,
  DataEngine.DriverConnection;

type
  [TestFixture]
  TResiliencyTests = class
  public
    [Test]
    procedure TestRetryMechanism;
    [Test]
    procedure TestHealthCheckInPool;
  end;

  TMockConnection = class(TInterfacedObject, IDBConnection, IDBTransaction)
  private
    FShouldFailCount: Integer;
    FCurrentFailures: Integer;
    FIsAlive: Boolean;
    FResiliencePolicy: IDBResiliencePolicy;
  public
    constructor Create(AFailCount: Integer);
    // IDBConnection
    procedure Connect; virtual;
    procedure Disconnect; virtual;
    procedure ExecuteDirect(const ASQL: String); overload; virtual;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); overload; virtual;
    procedure ExecuteScript(const AScript: String); virtual;
    procedure AddScript(const AScript: String); virtual;
    procedure ExecuteScripts; virtual;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); virtual;
    function IsConnected: Boolean; virtual;
    function CreateQuery: IDBQuery; virtual;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; virtual;
    function GetSQLScripts: String; virtual;
    function RowsAffected: UInt32; virtual;
    function GetDriver: TDriverName; virtual;
    function CommandMonitor: ICommandMonitor; virtual;
    procedure SetCommandMonitor(AMonitor: ICommandMonitor); virtual;
    function MonitorCallback: TMonitorProc; virtual;
    function Options: IOptions; virtual;
    function Cache: IDBCacheProvider; virtual;
    function MetadataCache: IDBMetadataCache; virtual;
    procedure SetCacheProvider(ACache: IDBCacheProvider); virtual;
    procedure SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache); virtual;
    procedure RefreshMetadata(const ATableName: string); virtual;
    function BulkLoader: IDBBulkLoader; virtual;
    function IsAlive: Boolean; virtual;

    function ResiliencePolicy: IDBResiliencePolicy; virtual;
    procedure SetResiliencePolicy(APolicy: IDBResiliencePolicy); virtual;
    
    // IDBTransaction
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); virtual;
    procedure Commit; virtual;
    procedure Rollback; virtual;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); virtual;
    procedure UseTransaction(const AKey: String); virtual;
    function TransactionActive: TComponent; virtual;
    function InTransaction: Boolean; virtual;
    function _GetTransaction(const AKey: String): TComponent; virtual;
    
    // Test specific
    property IsAliveStatus: Boolean read FIsAlive write FIsAlive;
  end;

implementation

{ TMockConnection }

constructor TMockConnection.Create(AFailCount: Integer);
begin
  inherited Create;
  FShouldFailCount := AFailCount;
  FCurrentFailures := 0;
  FIsAlive := True;
end;

procedure TMockConnection.Connect;
begin
  if FCurrentFailures < FShouldFailCount then
  begin
    Inc(FCurrentFailures);
    raise Exception.Create('Transient connection error');
  end;
end;

procedure TMockConnection.Disconnect; begin end;

procedure TMockConnection.ExecuteDirect(const ASQL: String);
begin
  if FCurrentFailures < FShouldFailCount then
  begin
    Inc(FCurrentFailures);
    raise Exception.Create('Transient execution error');
  end;
end;

procedure TMockConnection.ExecuteDirect(const ASQL: String; const AParams: TParams);
begin
  ExecuteDirect(ASQL);
end;

procedure TMockConnection.ExecuteScript(const AScript: String); begin end;
procedure TMockConnection.AddScript(const AScript: String); begin end;
procedure TMockConnection.ExecuteScripts; begin end;
procedure TMockConnection.ApplyUpdates(const ADataSets: array of IDBDataSet); begin end;

function TMockConnection.IsConnected: Boolean;
begin
  Result := FCurrentFailures >= FShouldFailCount;
end;

function TMockConnection.CreateQuery: IDBQuery; begin Result := nil; end;
function TMockConnection.CreateDataSet(const ASQL: String): IDBDataSet; begin Result := nil; end;
function TMockConnection.GetSQLScripts: String; begin Result := ''; end;
function TMockConnection.RowsAffected: UInt32; begin Result := 0; end;
function TMockConnection.GetDriver: TDriverName; begin Result := TDriverName.dnSQLite; end;
function TMockConnection.CommandMonitor: ICommandMonitor; begin Result := nil; end;
procedure TMockConnection.SetCommandMonitor(AMonitor: ICommandMonitor); begin end;
function TMockConnection.MonitorCallback: TMonitorProc; begin Result := nil; end;
function TMockConnection.Options: IOptions; begin Result := nil; end;
function TMockConnection.Cache: IDBCacheProvider; begin Result := nil; end;
function TMockConnection.MetadataCache: IDBMetadataCache; begin Result := nil; end;
procedure TMockConnection.SetCacheProvider(ACache: IDBCacheProvider); begin end;
procedure TMockConnection.SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache); begin end;
procedure TMockConnection.RefreshMetadata(const ATableName: string); begin end;

function TMockConnection.BulkLoader: IDBBulkLoader; begin Result := nil; end;
function TMockConnection.IsAlive: Boolean;

begin
  Result := FIsAlive;
end;

function TMockConnection.ResiliencePolicy: IDBResiliencePolicy;
begin
  Result := FResiliencePolicy;
end;

procedure TMockConnection.SetResiliencePolicy(APolicy: IDBResiliencePolicy);
begin
  FResiliencePolicy := APolicy;
end;

procedure TMockConnection.StartTransaction(const ALevel: TDBIsolationLevel); begin end;
procedure TMockConnection.Commit; begin end;
procedure TMockConnection.Rollback; begin end;
procedure TMockConnection.AddTransaction(const AKey: String; const ATransaction: TComponent); begin end;
procedure TMockConnection.UseTransaction(const AKey: String); begin end;
function TMockConnection.TransactionActive: TComponent; begin Result := nil; end;
function TMockConnection.InTransaction: Boolean; begin Result := False; end;
function TMockConnection._GetTransaction(const AKey: String): TComponent; begin Result := nil; end;

{ TResiliencyTests }

procedure TResiliencyTests.TestRetryMechanism;
var
  LGuard: TGuardConnection;
begin
  LGuard := TConnectionGuardBuilder.Create
    .Limit(1)
    .WithResiliencePolicy(TDefaultResiliencePolicy.Create)
    .WithFactory(function: IDBConnection
                 begin
                   Result := TMockConnection.Create(2); // Fail twice
                 end)
    .Build;
  try

    LGuard.UseConnection(procedure(AConn: IDBConnection)
                         begin
                           AConn.ExecuteDirect('SELECT 1');
                         end);
    // If we reached here, it succeeded after retries
    Assert.Pass;
  finally
    LGuard.Free;
  end;
end;

procedure TResiliencyTests.TestHealthCheckInPool;
var
  LGuard: TGuardConnection;
  LMock: TMockConnection;
begin
  LGuard := TConnectionGuardBuilder.Create
    .Limit(1)
    .WithFactory(function: IDBConnection
                 begin
                   Result := TMockConnection.Create(0);
                 end)
    .Build;
  try
    // First use to populate pool
    LGuard.UseConnection(procedure(AConn: IDBConnection)
                         begin
                           LMock := AConn as TMockConnection;
                           Assert.IsTrue(AConn.IsAlive);
                         end);
    
    // Sabotage the connection in pool
    LMock.IsAliveStatus := False;

    // Second use should detect it's not alive and create a new one
    LGuard.UseConnection(procedure(AConn: IDBConnection)
                         begin
                           Assert.AreNotEqual(Pointer(LMock), Pointer(AConn as TMockConnection));
                           Assert.IsTrue(AConn.IsAlive);
                         end);
  finally
    LGuard.Free;
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TResiliencyTests);

end.
