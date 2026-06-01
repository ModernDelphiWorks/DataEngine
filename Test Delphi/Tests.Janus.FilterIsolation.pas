unit Tests.Janus.FilterIsolation;

{
  ESP-006 (scope extension v2) — Triage by layer isolation using the SAME
  DataEngine native primitives the Janus REST stack ultimately invokes.

  Scope correction against the v1 bootstrap:
    v1 used IDBQuery.ExecuteQuery with literal SQL. That primitive is NOT
    what the Janus REST handler calls. The REST call chain resolves to:

      GET  $filter=name eq 'X'      (Janus.Server.Resource.ParseFind)
        -> TRESTObjectSet.FindWhere
        -> TRESTObjectSetSession.FindWhere
        -> TRESTObjectManager.FindWhere
        -> TDMLCommandFactory.GeneratorSelect(ASQL, APageSize)
        -> IDBConnection.CreateDataSet(ASQL)   <-- DataEngine native

      DELETE $filter=name eq 'X'    (Janus.Server.Resource.ParseDelete)
        -> TRESTObjectSet.FindOne(AWhere)      (uses CreateDataSet)
        -> TRESTObjectSet.Delete(LObject)
        -> TCommandDeleter.GenerateDelete(AObject) ->
             'DELETE FROM t WHERE id = :id' + TParams
        -> IDBConnection.ExecuteDirect(ASQL, AParams) <-- DataEngine native

    The OData-to-SQL mapping performed by TRESTQueryParse is literal:
      'name eq ''Alice'''            -> 'name = ''Alice'''
      'name eq ''Alice'' or name eq ''Bob''' -> 'name = ''Alice'' OR name = ''Bob'''

    Therefore the three tests below execute byte-equivalent DataEngine
    native calls to those that the failing REST endpoints dispatch, with
    zero Janus / Horse / MetaDbDiff / RestHorseTest dependency.

  Progression rule (BR-002, ADR-002): only one [Test] attribute active per
  slice. T2 and T3 stay under //[Test] until the previous slice PASSES.
}

interface

uses
  Data.DB,
  System.SysUtils,
  System.Classes,
  System.IOUtils,
  DUnitX.TestFramework,
  FireDAC.Comp.Client,
  FireDAC.Stan.Def,
  FireDAC.Stan.Intf,
  FireDAC.Stan.Option,
  FireDAC.Stan.Error,
  FireDAC.Stan.Pool,
  FireDAC.Stan.Async,
  FireDAC.UI.Intf,
  FireDAC.Phys.Intf,
  FireDAC.Phys,
  FireDAC.Phys.SQLite,
  FireDAC.Phys.SQLiteDef,
  FireDAC.DApt,
  DataEngine.FactoryInterfaces;

const
  cTEST_DB_PATH = 'dataengine_filter_triage.db';
  { Exact SELECT column list produced by Janus's SQLite generator for the
    customer_test entity. Column order and names mirror the REST path. }
  cSELECT_COLUMNS = 'id, name, email, active';

type
  [TestFixture]
  TTestJanusFilterIsolation = class
  private
    FDConnection: TFDConnection;
    FConnection: IDBConnection;
    procedure _CreateSchema;
    procedure _SeedCustomers;
    function _FindIdByName(const AName: String): Integer;
  public
    [SetupFixture]
    procedure SetupFixture;
    [TearDownFixture]
    procedure TearDownFixture;
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure RawFireDAC_GetFilterEq_Baseline;

    [Test]
    procedure GetFilterEq_ReturnsAlice_DataEngine;

    [Test]
    procedure GetFilterOr_ReturnsAliceAndBob_DataEngine;

    [Test]
    procedure DeleteFilter_RemovesTemp_DataEngine;
  end;

implementation

uses
  DataEngine.FactoryFireDac;

{ TTestJanusFilterIsolation }

procedure TTestJanusFilterIsolation._CreateSchema;
const
  LDDL_CUSTOMER =
    'CREATE TABLE IF NOT EXISTS customer_test (' +
    '  id      INTEGER PRIMARY KEY AUTOINCREMENT,' +
    '  name    VARCHAR(100) NOT NULL,' +
    '  email   VARCHAR(200),' +
    '  active  INTEGER DEFAULT 1' +
    ')';
begin
  FConnection.ExecuteDirect('DROP TABLE IF EXISTS customer_test');
  FConnection.ExecuteDirect(LDDL_CUSTOMER);
end;

procedure TTestJanusFilterIsolation._SeedCustomers;
begin
  FConnection.ExecuteDirect(
    'INSERT INTO customer_test (name, email, active) VALUES (''Alice'', ''alice@test.com'', 1)');
  FConnection.ExecuteDirect(
    'INSERT INTO customer_test (name, email, active) VALUES (''Bob'', ''bob@test.com'', 1)');
  FConnection.ExecuteDirect(
    'INSERT INTO customer_test (name, email, active) VALUES (''Carol'', ''carol@test.com'', 0)');
end;

function TTestJanusFilterIsolation._FindIdByName(const AName: String): Integer;
var
  LDataSet: IDBDataSet;
  LSQL: String;
begin
  { Mirrors TRESTObjectSet.FindOne -> TRESTObjectManager.FindOne path:
    SelectInternal(SelectInternalWhere(AWhere, '')) -> CreateDataSet(sql). }
  LSQL := 'SELECT ' + cSELECT_COLUMNS + ' FROM customer_test WHERE name = ''' + AName + '''';
  LDataSet := FConnection.CreateDataSet(LSQL);
  Assert.IsTrue(Assigned(LDataSet), 'CreateDataSet returned nil for FindOne SQL: ' + LSQL);
  if LDataSet.Eof then
  begin
    Result := 0;
    Exit;
  end;
  Result := LDataSet.FieldByName('id').AsInteger;
end;

procedure TTestJanusFilterIsolation.SetupFixture;
begin
  if TFile.Exists(cTEST_DB_PATH) then
    TFile.Delete(cTEST_DB_PATH);

  FDConnection := TFDConnection.Create(nil);
  FDConnection.Params.DriverID := 'SQLite';
  FDConnection.Params.Database := cTEST_DB_PATH;
  FDConnection.Params.Values['OpenMode'] := 'CreateUTF8';
  FDConnection.LoginPrompt := False;
  FDConnection.TxOptions.Isolation := xiReadCommitted;
  FDConnection.TxOptions.AutoCommit := False;
  FDConnection.Connected := True;

  FConnection := TFactoryFireDAC.Create(FDConnection, dnSQLite);
end;

procedure TTestJanusFilterIsolation.TearDownFixture;
begin
  FConnection := nil;
  if Assigned(FDConnection) then
  begin
    FDConnection.Connected := False;
    FreeAndNil(FDConnection);
  end;
  if TFile.Exists(cTEST_DB_PATH) then
    TFile.Delete(cTEST_DB_PATH);
end;

procedure TTestJanusFilterIsolation.Setup;
begin
  _CreateSchema;
  _SeedCustomers;
end;

procedure TTestJanusFilterIsolation.TearDown;
begin
end;

procedure TTestJanusFilterIsolation.RawFireDAC_GetFilterEq_Baseline;
const
  LSQL = 'SELECT id, name, email, active FROM customer_test WHERE name = ''Alice''';
var
  LQuery: TFDQuery;
  LCount: Integer;
begin
  { Bypass DataEngine entirely — raw TFDQuery on same FDConnection.
    If this passes and GetFilterEq_ReturnsAlice_DataEngine fails,
    the bug is inside DataEngine's CreateDataSet wrapper. }
  LQuery := TFDQuery.Create(nil);
  try
    LQuery.Connection := FDConnection;
    LQuery.SQL.Text := LSQL;
    LQuery.Open;
    LCount := 0;
    while not LQuery.Eof do
    begin
      Inc(LCount);
      LQuery.Next;
    end;
    Assert.AreEqual(1, LCount,
      Format('RAW: Expected 1 row for Alice but got %d', [LCount]));
  finally
    LQuery.Free;
  end;
end;

procedure TTestJanusFilterIsolation.GetFilterEq_ReturnsAlice_DataEngine;
const
  { Exact SQL produced by Janus REST for:
      GET /CustomerTest?$filter=name eq 'Alice'
    via TDMLGeneratorSQLite.GeneratorSelectWhere(AClass, 'name = ''Alice''', '', -1)
    which appends 'WHERE' + AWhere to the entity's base SELECT. }
  LSQL = 'SELECT ' + cSELECT_COLUMNS + ' FROM customer_test WHERE name = ''Alice''';
var
  LDataSet: IDBDataSet;
  LCount: Integer;
  LName: String;
begin
  { Same native DataEngine call the REST stack makes:
    TDMLCommandFactory.GeneratorSelect(ASQL, APageSize) -> FConnection.CreateDataSet(ASQL). }
  LDataSet := FConnection.CreateDataSet(LSQL);
  Assert.IsTrue(Assigned(LDataSet), 'CreateDataSet returned nil for: ' + LSQL);

  LCount := 0;
  LName := '';
  LDataSet.First;
  while not LDataSet.Eof do
  begin
    LName := LDataSet.FieldByName('name').AsString;
    Inc(LCount);
    LDataSet.Next;
  end;

  Assert.AreEqual(1, LCount,
    Format('Expected 1 row for "%s" but got %d', [LSQL, LCount]));
  Assert.AreEqual('Alice', LName,
    Format('Expected name "Alice" but got "%s" for: %s', [LName, LSQL]));
end;

procedure TTestJanusFilterIsolation.GetFilterOr_ReturnsAliceAndBob_DataEngine;
const
  { Exact SQL produced by Janus REST for:
      GET /CustomerTest?$filter=name eq 'Alice' or name eq 'Bob'
    The OData 'or' is mapped to SQL 'OR' by TRESTQueryParse.ParseOperator
    (cLogOData[1]='or' -> cLogSQL[1]='OR'). }
  LSQL = 'SELECT ' + cSELECT_COLUMNS +
         ' FROM customer_test WHERE name = ''Alice'' OR name = ''Bob''';
var
  LDataSet: IDBDataSet;
  LCount: Integer;
  LHasAlice: Boolean;
  LHasBob: Boolean;
  LName: String;
begin
  LDataSet := FConnection.CreateDataSet(LSQL);
  Assert.IsTrue(Assigned(LDataSet), 'CreateDataSet returned nil for: ' + LSQL);

  LCount := 0;
  LHasAlice := False;
  LHasBob := False;
  LDataSet.First;
  while not LDataSet.Eof do
  begin
    LName := LDataSet.FieldByName('name').AsString;
    if LName = 'Alice' then
      LHasAlice := True;
    if LName = 'Bob' then
      LHasBob := True;
    Inc(LCount);
    LDataSet.Next;
  end;

  Assert.AreEqual(2, LCount,
    Format('Expected 2 rows for "%s" but got %d', [LSQL, LCount]));
  Assert.IsTrue(LHasAlice, 'Alice missing from result set for: ' + LSQL);
  Assert.IsTrue(LHasBob, 'Bob missing from result set for: ' + LSQL);
end;

procedure TTestJanusFilterIsolation.DeleteFilter_RemovesTemp_DataEngine;
const
  LSQL_INSERT =
    'INSERT INTO customer_test (name, email, active) VALUES (''Temp'', ''temp@test.com'', 0)';
  { DELETE SQL shape produced by TCommandDeleter.GenerateDelete(AObject):
    always 'DELETE FROM <table> WHERE <pk> = :<pk>' + TParams (ftInteger).
    REST DELETE $filter flow: FindOne(WHERE name='Temp') -> load Object ->
    GenerateDelete by primary key -> ExecuteDirect(sql, params). }
  LSQL_DELETE_BY_PK = 'DELETE FROM customer_test WHERE id = :id';
  LSQL_COUNT =
    'SELECT count(*) AS row_count FROM customer_test WHERE name = ''Temp''';
var
  LTempId: Integer;
  LParams: TParams;
  LParam: TParam;
  LDataSet: IDBDataSet;
  LRemaining: Integer;
begin
  { Step 1 — seed Temp, mirroring the POST the REST test does before DELETE. }
  FConnection.ExecuteDirect(LSQL_INSERT);

  { Step 2 — FindOne via CreateDataSet (exactly what ParseDelete/FindOne does). }
  LTempId := _FindIdByName('Temp');
  Assert.AreNotEqual(0, LTempId, 'FindOne(name=''Temp'') returned no row before DELETE');

  { Step 3 — parameterized DELETE by PK, exactly what
    TDMLCommandFactory.GeneratorDelete -> FConnection.ExecuteDirect(sql, params) does. }
  LParams := TParams.Create(nil);
  try
    LParam := LParams.Add as TParam;
    LParam.Name := 'id';
    LParam.DataType := ftInteger;
    LParam.Value := LTempId;
    FConnection.ExecuteDirect(LSQL_DELETE_BY_PK, LParams);
  finally
    LParams.Free;
  end;

  { Step 4 — verify removal using CreateDataSet (same primitive as the rest). }
  LDataSet := FConnection.CreateDataSet(LSQL_COUNT);
  Assert.IsTrue(Assigned(LDataSet), 'CreateDataSet returned nil for: ' + LSQL_COUNT);
  Assert.IsFalse(LDataSet.Eof, 'count(*) returned empty result set for: ' + LSQL_COUNT);
  LRemaining := LDataSet.FieldByName('row_count').AsInteger;

  Assert.AreEqual(0, LRemaining,
    Format('Expected 0 rows after DELETE id=%d but got %d (sql=%s)',
           [LTempId, LRemaining, LSQL_DELETE_BY_PK]));
end;

initialization
  TDUnitX.RegisterTestFixture(TTestJanusFilterIsolation);

end.
