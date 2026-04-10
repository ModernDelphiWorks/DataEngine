program Tests.Horse.Integration;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.Generics.Collections,
  Horse,
  Horse.DataEngine,
  DataEngine.FactoryInterfaces,
  DataEngine.PoolConnection,
  DB;

type
  TMockConnection = class(TInterfacedObject, IDBConnection)
  public
    Committed: Boolean;
    RolledBack: Boolean;
    Started: Boolean;
    Released: Boolean;
    InTx: Boolean;
    function InTransaction: Boolean;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault);
    procedure Commit;
    procedure Rollback;
    procedure Connect;
    procedure Disconnect;
    procedure ExecuteDirect(const ASQL: String); overload;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); overload;
    procedure ExecuteScript(const AScript: String);
    procedure AddScript(const AScript: String);
    procedure ExecuteScripts;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet);
    function IsConnected: Boolean;
    function CreateQuery: IDBQuery;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet;
    function BulkLoader: IDBBulkLoader;
    function GetSQLScripts: String;
    function RowsAffected: UInt32;
    function GetDriver: TDBEngineDriver;
    function CommandMonitor: ICommandMonitor;
    function MonitorCallback: TMonitorProc;
    function Options: IOptions;
    function Cache: IDBCacheProvider;
    function MetadataCache: IDBMetadataCache;
    procedure SetCacheProvider(ACache: IDBCacheProvider);
    procedure SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache);
    procedure SetCommandMonitor(AMonitor: ICommandMonitor);
    procedure RefreshMetadata(const ATableName: string);
    function IsAlive: Boolean;
    function ResiliencePolicy: IDBResiliencePolicy;
    procedure SetResiliencePolicy(APolicy: IDBResiliencePolicy);
    procedure AddObserver(const AObserver: IDBObserver);
    procedure RemoveObserver(const AObserver: IDBObserver);
    function SlowQueryThreshold: Integer;
    procedure SetSlowQueryThreshold(const AValue: Integer);
    function _GetTransaction(const AKey: String): TComponent;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent);
    procedure UseTransaction(const AKey: String);
    function TransactionActive: TComponent;
  end;

  TMockPoolManager = class(TInterfacedObject, IDBPoolManager)
  public
    LastTenant: string;
    Connection: TMockConnection;
    function AcquireConnection(const ATenantID: string; const AFactory: TFunc<IDBConnection>;
      const AMaxConnections: Integer = 10; const ALifetime: Integer = 600): IDBConnection;
    procedure ReleaseConnection(const ATenantID: string; const AConnection: IDBConnection);
    procedure Clear;
    procedure Cleanup;
  end;

{ TMockConnection implementations }

function TMockConnection.InTransaction: Boolean;
begin
  Result := InTx;
end;

procedure TMockConnection.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  Started := True;
  InTx := True;
end;

procedure TMockConnection.Commit;
begin
  Committed := True;
  InTx := False;
end;

procedure TMockConnection.Rollback;
begin
  RolledBack := True;
  InTx := False;
end;

procedure TMockConnection.Connect; begin end;
procedure TMockConnection.Disconnect; begin end;
procedure TMockConnection.ExecuteDirect(const ASQL: String); begin end;
procedure TMockConnection.ExecuteDirect(const ASQL: String; const AParams: TParams); begin end;
procedure TMockConnection.ExecuteScript(const AScript: String); begin end;
procedure TMockConnection.AddScript(const AScript: String); begin end;
procedure TMockConnection.ExecuteScripts; begin end;
procedure TMockConnection.ApplyUpdates(const ADataSets: array of IDBDataSet); begin end;
function TMockConnection.IsConnected: Boolean; begin Result := True; end;
function TMockConnection.CreateQuery: IDBQuery; begin Result := nil; end;
function TMockConnection.CreateDataSet(const ASQL: String): IDBDataSet; begin Result := nil; end;
function TMockConnection.BulkLoader: IDBBulkLoader; begin Result := nil; end;
function TMockConnection.GetSQLScripts: String; begin Result := ''; end;
function TMockConnection.RowsAffected: UInt32; begin Result := 0; end;
function TMockConnection.GetDriver: TDBEngineDriver; begin Result := dnSQLite; end;
function TMockConnection.CommandMonitor: ICommandMonitor; begin Result := nil; end;
function TMockConnection.MonitorCallback: TMonitorProc; begin Result := nil; end;
function TMockConnection.Options: IOptions; begin Result := nil; end;
function TMockConnection.Cache: IDBCacheProvider; begin Result := nil; end;
function TMockConnection.MetadataCache: IDBMetadataCache; begin Result := nil; end;
procedure TMockConnection.SetCacheProvider(ACache: IDBCacheProvider); begin end;
procedure TMockConnection.SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache); begin end;
procedure TMockConnection.SetCommandMonitor(AMonitor: ICommandMonitor); begin end;
procedure TMockConnection.RefreshMetadata(const ATableName: string); begin end;
function TMockConnection.IsAlive: Boolean; begin Result := True; end;
function TMockConnection.ResiliencePolicy: IDBResiliencePolicy; begin Result := nil; end;
procedure TMockConnection.SetResiliencePolicy(APolicy: IDBResiliencePolicy); begin end;
procedure TMockConnection.AddObserver(const AObserver: IDBObserver); begin end;
procedure TMockConnection.RemoveObserver(const AObserver: IDBObserver); begin end;
function TMockConnection.SlowQueryThreshold: Integer; begin Result := 0; end;
procedure TMockConnection.SetSlowQueryThreshold(const AValue: Integer); begin end;
function TMockConnection._GetTransaction(const AKey: String): TComponent; begin Result := nil; end;
procedure TMockConnection.AddTransaction(const AKey: String; const ATransaction: TComponent); begin end;
procedure TMockConnection.UseTransaction(const AKey: String); begin end;
function TMockConnection.TransactionActive: TComponent; begin Result := nil; end;

{ TMockPoolManager implementations }

function TMockPoolManager.AcquireConnection(const ATenantID: string; const AFactory: TFunc<IDBConnection>;
  const AMaxConnections: Integer; const ALifetime: Integer): IDBConnection;
begin
  LastTenant := ATenantID;
  Result := Connection;
end;

procedure TMockPoolManager.ReleaseConnection(const ATenantID: string; const AConnection: IDBConnection);
begin
  Connection.Released := True;
end;

procedure TMockPoolManager.Clear; begin end;
procedure TMockPoolManager.Cleanup; begin end;

var
  VSuccess: Boolean = False;

procedure TestMiddlewareLogic;
var
  LPool: TMockPoolManager;
  LConn: TMockConnection;
  LMiddleware: THorseCallback;
  LReq: THorseRequest;
  LRes: THorseResponse;
begin
  Writeln('Testing Middleware Transaction Logic...');
  
  LPool := TMockPoolManager.Create;
  LConn := TMockConnection.Create;
  LPool.Connection := LConn;
  
  LMiddleware := HorseDataEngine(LPool);
  
  { Case 1: Status 200 -> Commit }
  LReq := THorseRequest.Create;
  LRes := THorseResponse.Create;
  try
    LRes.Status(200);
    LMiddleware(LReq, LRes, procedure begin end);
    
    if not LConn.Started then raise Exception.Create('Transaction not started');
    if not LConn.Committed then raise Exception.Create('Transaction not committed for status 200');
    if not LConn.Released then raise Exception.Create('Connection not released');
  finally
    LReq.Free;
    LRes.Free;
  end;
  Writeln('  - Status 200 (Commit) OK');

  { Case 2: Status 400 -> Rollback }
  LConn.Started := False; LConn.Committed := False; LConn.RolledBack := False; LConn.Released := False;
  LReq := THorseRequest.Create;
  LRes := THorseResponse.Create;
  try
    LRes.Status(400);
    LMiddleware(LReq, LRes, procedure begin end);
    
    if not LConn.Started then raise Exception.Create('Transaction not started');
    if not LConn.RolledBack then raise Exception.Create('Transaction not rolled back for status 400');
    if not LConn.Released then raise Exception.Create('Connection not released');
  finally
    LReq.Free;
    LRes.Free;
  end;
  Writeln('  - Status 400 (Rollback) OK');
end;

procedure TestMultiTenancy;
var
  LPool: TMockPoolManager;
  LConn: TMockConnection;
  LConfig: IHorseDataEngineConfig;
  LMiddleware: THorseCallback;
  LReq: THorseRequest;
  LRes: THorseResponse;
begin
  Writeln('Testing Multi-tenancy logic...');
  LPool := TMockPoolManager.Create;
  LConn := TMockConnection.Create;
  LPool.Connection := LConn;
  
  LConfig := THorseDataEngineConfig.New(LPool);
  LConfig.TenantProvider := function(Req: THorseRequest): string
    begin
       Result := Req.Headers['X-Tenant'];
    end;
    
  LMiddleware := HorseDataEngine(LConfig);
  
  LReq := THorseRequest.Create;
  LReq.Headers.Add('X-Tenant', 'Tenant-A');
  LRes := THorseResponse.Create;
  try
    LMiddleware(LReq, LRes, procedure begin end);
    if LPool.LastTenant <> 'Tenant-A' then
      raise Exception.Create('Incorrect tenant id: ' + LPool.LastTenant);
  finally
    LReq.Free;
    LRes.Free;
  end;
  Writeln('  - Tenant Provider OK');
end;

begin
  try
    Writeln('--- DataEngine Horse Integration Tests ---');
    TestMiddlewareLogic;
    TestMultiTenancy;
    Writeln('--- All Tests Passed ---');
    VSuccess := True;
  except
    on E: Exception do
    begin
      Writeln('ERROR: ', E.Message);
      VSuccess := False;
    end;
  end;
  
  if not VSuccess then
    Halt(1);
end.
