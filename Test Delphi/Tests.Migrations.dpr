program Tests.Migrations;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  DataEngine.FactoryInterfaces,
  DataEngine.Migration.Interfaces,
  DataEngine.Migration.Manager,
  DataEngine.FactorySQLite3,
  DataEngine.FluentSQL,
  SQLiteTable3;

type
  { Migrations using FluentSQL Bridge }
  TMigrationV1 = class(TInterfacedObject, IDBMigration)
  public
    function GetVersion: Int64;
    function GetName: string;
    procedure Up(const AConnection: IDBConnection);
    procedure Down(const AConnection: IDBConnection);
  end;

  TMigrationV2 = class(TInterfacedObject, IDBMigration)
  public
    function GetVersion: Int64;
    function GetName: string;
    procedure Up(const AConnection: IDBConnection);
    procedure Down(const AConnection: IDBConnection);
  end;
  
  TMigrationLargeGap = class(TInterfacedObject, IDBMigration)
  public
    function GetVersion: Int64;
    function GetName: string;
    procedure Up(const AConnection: IDBConnection);
    procedure Down(const AConnection: IDBConnection);
  end;

{ TMigrationV1 }

procedure TMigrationV1.Down(const AConnection: IDBConnection);
begin
  AConnection.ExecuteDirect('DROP TABLE Users');
end;

function TMigrationV1.GetName: string;
begin
  Result := 'Initial Create Users';
end;

function TMigrationV1.GetVersion: Int64;
begin
  Result := 20260410001;
end;

procedure TMigrationV1.Up(const AConnection: IDBConnection);
begin
  TDataEngineFluentSQL.CreateTable(AConnection, 'Users',
    procedure(Builder: IFluentTableBuilder)
    begin
      Builder.AddColumn('Id', 'INTEGER', True, True);
      Builder.AddColumn('Name', 'VARCHAR(100)', False, True);
    end);
end;

{ TMigrationV2 }

procedure TMigrationV2.Down(const AConnection: IDBConnection);
begin
  AConnection.ExecuteDirect('ALTER TABLE Users DROP COLUMN Email');
end;

function TMigrationV2.GetName: string;
begin
  Result := 'Add Email to Users and Index';
end;

function TMigrationV2.GetVersion: Int64;
begin
  Result := 20260410002;
end;

procedure TMigrationV2.Up(const AConnection: IDBConnection);
begin
  TDataEngineFluentSQL.AddColumn(AConnection, 'Users', 'Email', 'VARCHAR(200)');
  TDataEngineFluentSQL.CreateIndex(AConnection, 'IDX_Users_Name', 'Users', ['Name']);
end;

{ TMigrationLargeGap }

procedure TMigrationLargeGap.Down(const AConnection: IDBConnection);
begin
end;

function TMigrationLargeGap.GetName: string;
begin
  Result := 'Large Gap Test';
end;

function TMigrationLargeGap.GetVersion: Int64;
begin
  // Versão propositalmente grande para testar o sorting
  Result := 99991231235959; 
end;

procedure TMigrationLargeGap.Up(const AConnection: IDBConnection);
begin
  Writeln('Large gap migration applied successfully.');
end;

procedure RunTest;
var
  LDB: TSQLiteDatabase;
  LConn: IDBConnection;
  LManager: IDBMigrationEngine;
  LApplied: TArray<Int64>;
begin
  Writeln('--- Starting Migrations Integration Test ---');
  
  LDB := TSQLiteDatabase.Create(nil);
  try
    LDB.Filename := ':memory:';
    LDB.Connected := True;
    
    LConn := TFactorySQLite3.Create(LDB, dnSQLite);
    
    LManager := TMigrationManager.Create(LConn);
    LManager.RegisterMigration(TMigrationLargeGap.Create); // Registrada fora de ordem (grande)
    LManager.RegisterMigration(TMigrationV1.Create);
    LManager.RegisterMigration(TMigrationV2.Create);
    
    Writeln('Applying migrations...');
    LManager.RunMigrations;
    
    LApplied := LManager.GetAppliedMigrations;
    Writeln('Applied versions: ' + Length(LApplied).ToString);
    
    if Length(LApplied) = 3 then
      Writeln('SUCCESS: All migrations applied correctly (including large gap).')
    else
      raise Exception.Create('FAILURE: Expected 3 migrations, but applied ' + Length(LApplied).ToString);

    { Verify order }
    if LApplied[2] <> 99991231235959 then
       raise Exception.Create('FAILURE: Sorting of Int64 versions failed. Last version should be 99991231235959.');

    { Verify if table exists }
    LConn.ExecuteDirect('SELECT * FROM Users');
    Writeln('SUCCESS: Table "Users" is accessible.');

    { Verify if migration is idempotent }
    Writeln('Verifying idempotency...');
    LManager.RunMigrations;
    if Length(LManager.GetAppliedMigrations) = 3 then
      Writeln('SUCCESS: Migrations are idempotent.')
    else
      raise Exception.Create('FAILURE: Idempotency check failed.');
      
  finally
    LConn.Disconnect;
  end;
end;

begin
  try
    RunTest;
    Writeln('--- Test completed with success ---');
  except
    on E: Exception do
    begin
      Writeln('--- TEST FAILED ---');
      Writeln(E.ClassName, ': ', E.Message);
      Halt(1);
    end;
  end;
  if DebugHook <> 0 then
    Readln;
end.
