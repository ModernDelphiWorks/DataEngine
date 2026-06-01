{
  ------------------------------------------------------------------------------
  DataEngine
  High-performance database engine abstraction framework for Delphi and Lazarus.

  SPDX-License-Identifier: MIT
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the MIT License.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.Migration.Manager;

interface

uses
  System.SysUtils,
  System.Generics.Collections,
  System.Generics.Defaults,
  DataEngine.FactoryInterfaces,
  DataEngine.Migration.Interfaces,
  DataEngine.FluentSQL;

type
  TMigrationManager = class(TInterfacedObject, IDBMigrationEngine)
  private
    FConnection: IDBConnection;
    FMigrations: TList<IDBMigration>;
    procedure CreateHistoryTable;
    procedure RecordMigration(const AMigration: IDBMigration);
  public
    constructor Create(const AConnection: IDBConnection);
    destructor Destroy; override;
    procedure RunMigrations;
    procedure RegisterMigration(const AMigration: IDBMigration);
    function GetAppliedMigrations: TArray<Int64>;
  end;

implementation

{ TMigrationManager }

constructor TMigrationManager.Create(const AConnection: IDBConnection);
begin
  inherited Create;
  FConnection := AConnection;
  FMigrations := TList<IDBMigration>.Create;
end;

destructor TMigrationManager.Destroy;
begin
  FMigrations.Free;
  inherited;
end;

procedure TMigrationManager.CreateHistoryTable;
begin
  TDataEngineFluentSQL.CreateHistoryTable(FConnection);
end;

function TMigrationManager.GetAppliedMigrations: TArray<Int64>;
var
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LList: TList<Int64>;
begin
  CreateHistoryTable;
  LList := TList<Int64>.Create;
  try
    LQuery := FConnection.CreateQuery;
    LQuery.CommandText := 'SELECT Version FROM _DataEngineMigrations ORDER BY Version';
    LDataSet := LQuery.ExecuteQuery;
    while not LDataSet.Eof do
    begin
      LList.Add(LDataSet.FieldByName('Version').AsLargeInt);
      LDataSet.Next;
    end;
    Result := LList.ToArray;
  finally
    LList.Free;
  end;
end;


procedure TMigrationManager.RecordMigration(const AMigration: IDBMigration);
var
  LQuery: IDBQuery;
begin
  LQuery := FConnection.CreateQuery;
  LQuery.CommandText := 'INSERT INTO _DataEngineMigrations (Version, Name) VALUES (:Version, :Name)';
  LQuery.ParamByName('Version').AsLargeInt := AMigration.GetVersion;
  LQuery.ParamByName('Name').AsString := AMigration.GetName;
  LQuery.ExecuteDirect;
end;

procedure TMigrationManager.RegisterMigration(const AMigration: IDBMigration);
begin
  FMigrations.Add(AMigration);
end;

procedure TMigrationManager.RunMigrations;
var
  LMigration: IDBMigration;
  LApplied: TArray<Int64>;
  function Applied(const AVersion: Int64): Boolean;
  var
    LV: Int64;
  begin
    for LV in LApplied do
      if LV = AVersion then
        Exit(True);
    Result := False;
  end;
begin
  // Sort migrations by version
  FMigrations.Sort(TComparer<IDBMigration>.Construct(
    function(const Left, Right: IDBMigration): Integer
    var
      L, R: Int64;
    begin
      L := Left.GetVersion;
      R := Right.GetVersion;
      if L < R then Result := -1
      else if L > R then Result := 1
      else Result := 0;
    end));

  CreateHistoryTable;
  LApplied := GetAppliedMigrations;

  for LMigration in FMigrations do
  begin
    if not Applied(LMigration.GetVersion) then
    begin
      FConnection.StartTransaction;
      try
        LMigration.Up(FConnection);
        RecordMigration(LMigration);
        FConnection.Commit;
      except
        FConnection.Rollback;
        raise;
      end;
    end;
  end;
end;

end.
