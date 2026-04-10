unit Tests.MultiTenant.Mocks;

interface

uses
  System.Classes, System.SysUtils, Data.DB, DataEngine.FactoryInterfaces;

type
  TStubConnection = class(TInterfacedObject, IDBConnection, IDBTransaction)
  private
    function _GetTransaction(const AKey: String): TComponent;
  public
    // IDBTransaction
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault);
    procedure Commit;
    procedure Rollback;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent);
    procedure UseTransaction(const AKey: String);
    function TransactionActive: TComponent;
    function InTransaction: Boolean;
    // IDBConnection
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
  end;

implementation

{ TStubConnection }

procedure TStubConnection.AddObserver(const AObserver: IDBObserver); begin end;
procedure TStubConnection.AddScript(const AScript: String); begin end;
procedure TStubConnection.AddTransaction(const AKey: String; const ATransaction: TComponent); begin end;
procedure TStubConnection.ApplyUpdates(const ADataSets: array of IDBDataSet); begin end;
function TStubConnection.BulkLoader: IDBBulkLoader; begin Result := nil; end;
function TStubConnection.Cache: IDBCacheProvider; begin Result := nil; end;
procedure TStubConnection.Commit; begin end;
function TStubConnection.CommandMonitor: ICommandMonitor; begin Result := nil; end;
procedure TStubConnection.Connect; begin end;
function TStubConnection.CreateDataSet(const ASQL: String): IDBDataSet; begin Result := nil; end;
function TStubConnection.CreateQuery: IDBQuery; begin Result := nil; end;
procedure TStubConnection.Disconnect; begin end;
procedure TStubConnection.ExecuteDirect(const ASQL: String); begin end;
procedure TStubConnection.ExecuteDirect(const ASQL: String; const AParams: TParams); begin end;
procedure TStubConnection.ExecuteScript(const AScript: String); begin end;
procedure TStubConnection.ExecuteScripts; begin end;
function TStubConnection.GetDriver: TDBEngineDriver; begin Result := dnMemory; end;
function TStubConnection.GetSQLScripts: String; begin Result := ''; end;
function TStubConnection.InTransaction: Boolean; begin Result := False; end;
function TStubConnection.IsAlive: Boolean; begin Result := True; end;
function TStubConnection.IsConnected: Boolean; begin Result := True; end;
function TStubConnection.MetadataCache: IDBMetadataCache; begin Result := nil; end;
function TStubConnection.MonitorCallback: TMonitorProc; begin Result := nil; end;
function TStubConnection.Options: IOptions; begin Result := nil; end;
procedure TStubConnection.RefreshMetadata(const ATableName: string); begin end;
procedure TStubConnection.RemoveObserver(const AObserver: IDBObserver); begin end;
function TStubConnection.ResiliencePolicy: IDBResiliencePolicy; begin Result := nil; end;
procedure TStubConnection.Rollback; begin end;
function TStubConnection.RowsAffected: UInt32; begin Result := 0; end;
procedure TStubConnection.SetCacheProvider(ACache: IDBCacheProvider); begin end;
procedure TStubConnection.SetCommandMonitor(AMonitor: ICommandMonitor); begin end;
procedure TStubConnection.SetMetadataCacheProvider(AMetadataCache: IDBMetadataCache); begin end;
procedure TStubConnection.SetResiliencePolicy(APolicy: IDBResiliencePolicy); begin end;
procedure TStubConnection.SetSlowQueryThreshold(const AValue: Integer); begin end;
function TStubConnection.SlowQueryThreshold: Integer; begin Result := 0; end;
procedure TStubConnection.StartTransaction(const ALevel: TDBIsolationLevel); begin end;
function TStubConnection.TransactionActive: TComponent; begin Result := nil; end;
procedure TStubConnection.UseTransaction(const AKey: String); begin end;
function TStubConnection._GetTransaction(const AKey: String): TComponent; begin Result := nil; end;

end.
