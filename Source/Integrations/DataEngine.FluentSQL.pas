{
  ------------------------------------------------------------------------------
  DataEngine
  Modular and extensible database engine framework for Delphi.

  SPDX-License-Identifier: Apache-2.0
  Copyright (c) 2025-2026 Isaque Pinheiro

  Licensed under the Apache License, Version 2.0.
  See the LICENSE file in the project root for full license information.
  ------------------------------------------------------------------------------
}

unit DataEngine.FluentSQL;

interface

uses
  System.SysUtils,
  System.Classes,
  DataEngine.FactoryInterfaces;

type
  { FluentSQL Column Definition Hub }
  IFluentColumn = interface
    ['{A1B2C3D4-E5F6-4A5B-8C9D-0E1F2A3B4C5D}']
    function GetName: string;
    function GetDataType: string;
    function IsPrimaryKey: Boolean;
    function IsNotNull: Boolean;
  end;

  IFluentTableBuilder = interface
    ['{B2C3D4E5-F6A1-5B6C-9D0E-1F2A3B4C5D6E}']
    procedure AddColumn(const AName: string; const AType: string; const AIsPK: Boolean = False; const AIsNotNull: Boolean = False);
    function BuildSQL(const ADriverName: string; const ATableName: string): string;
  end;

  { Concrete Simple Builder for Bridge }
  TFluentTableBuilder = class(TInterfacedObject, IFluentTableBuilder)
  private
    FColumns: TStringList;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AddColumn(const AName: string; const AType: string; const AIsPK: Boolean = False; const AIsNotNull: Boolean = False);
    function BuildSQL(const ADriverName: string; const ATableName: string): string;
  end;

  { FluentSQL Bridge }
  TDataEngineFluentSQL = class
  public
    class procedure CreateTable(const AConnection: IDBConnection; const ATableName: string; const AColumns: TProc<IFluentTableBuilder>);
    class procedure CreateIndex(const AConnection: IDBConnection; const AIndexName: string; const ATableName: string; const AFields: array of string);
    class procedure CreateHistoryTable(const AConnection: IDBConnection);
    class procedure AddColumn(const AConnection: IDBConnection; const ATableName: string; const AColumnName: string; const AType: string);
  end;

implementation

{ TFluentTableBuilder }

constructor TFluentTableBuilder.Create;
begin
  inherited Create;
  FColumns := TStringList.Create;
end;

destructor TFluentTableBuilder.Destroy;
begin
  FColumns.Free;
  inherited;
end;

procedure TFluentTableBuilder.AddColumn(const AName: string; const AType: string;
  const AIsPK, AIsNotNull: Boolean);
var
  LCol: string;
begin
  LCol := AName + ' ' + AType;
  if AIsPK then LCol := LCol + ' PRIMARY KEY';
  if AIsNotNull then LCol := LCol + ' NOT NULL';
  FColumns.Add(LCol);
end;

function TFluentTableBuilder.BuildSQL(const ADriverName, ATableName: string): string;
var
  LCols: string;
begin
  LCols := FColumns.Text.TrimRight;
  LCols := StringReplace(LCols, sLineBreak, ',' + sLineBreak + '  ', [rfReplaceAll]);
  Result := 'CREATE TABLE ' + ATableName + ' (' + sLineBreak +
            '  ' + LCols + sLineBreak +
            ')';
end;

{ TDataEngineFluentSQL }

class procedure TDataEngineFluentSQL.CreateIndex(const AConnection: IDBConnection;
  const AIndexName, ATableName: string; const AFields: array of string);
var
  LSQL: string;
  LFields: string;
  LIndex: Integer;
begin
  LFields := '';
  for LIndex := Low(AFields) to High(AFields) do
  begin
    if LIndex > Low(AFields) then LFields := LFields + ', ';
    LFields := LFields + AFields[LIndex];
  end;
  LSQL := Format('CREATE INDEX %s ON %s (%s)', [AIndexName, ATableName, LFields]);
  AConnection.ExecuteDirect(LSQL);
end;

class procedure TDataEngineFluentSQL.CreateTable(const AConnection: IDBConnection;
  const ATableName: string; const AColumns: TProc<IFluentTableBuilder>);
var
  LBuilder: IFluentTableBuilder;
  LSQL: string;
begin
  LBuilder := TFluentTableBuilder.Create;
  if Assigned(AColumns) then
    AColumns(LBuilder);
  
  LSQL := LBuilder.BuildSQL(TStrDBEngineDriver[AConnection.GetDriver], ATableName);
  AConnection.ExecuteDirect(LSQL);
end;

class procedure TDataEngineFluentSQL.CreateHistoryTable(const AConnection: IDBConnection);
var
  LSQL: string;
begin
  case AConnection.GetDriver of
    dnSQLite, dnPostgreSQL, dnMariaDB, dnMySQL:
      LSQL := 'CREATE TABLE IF NOT EXISTS _DataEngineMigrations (' +
              '  Version BIGINT NOT NULL,' +
              '  Name VARCHAR(255) NOT NULL,' +
              '  AppliedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
              '  PRIMARY KEY (Version)' +
              ')';
    dnMSSQL:
      LSQL := 'IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N''[_DataEngineMigrations]'') AND type in (N''U'')) ' +
              'CREATE TABLE _DataEngineMigrations (' +
              '  Version BIGINT NOT NULL,' +
              '  Name VARCHAR(255) NOT NULL,' +
              '  AppliedAt DATETIME DEFAULT GETDATE(),' +
              '  PRIMARY KEY (Version)' +
              ')';
    dnFirebird, dnFirebird3:
      LSQL := 'CREATE TABLE _DataEngineMigrations (' +
              '  Version BIGINT NOT NULL,' +
              '  Name VARCHAR(255) NOT NULL,' +
              '  AppliedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
              '  PRIMARY KEY (Version)' +
              ')';
    else
      LSQL := 'CREATE TABLE _DataEngineMigrations (' +
              '  Version BIGINT NOT NULL,' +
              '  Name VARCHAR(255) NOT NULL,' +
              '  AppliedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,' +
              '  PRIMARY KEY (Version)' +
              ')';
  end;

  try
    AConnection.ExecuteDirect(LSQL);
  except
    on E: Exception do
    begin
      if not (E.Message.ToLower.Contains('already exists') or 
              E.Message.ToLower.Contains('já existe') or
              E.Message.ToLower.Contains('table base or view already exists')) then
        raise;
    end;
  end;
end;

class procedure TDataEngineFluentSQL.AddColumn(const AConnection: IDBConnection;
  const ATableName, AColumnName, AType: string);
var
  LSQL: string;
begin
  case AConnection.GetDriver of
    dnMSSQL, dnFirebird, dnFirebird3:
      LSQL := Format('ALTER TABLE %s ADD %s %s', [ATableName, AColumnName, AType]);
    else
      LSQL := Format('ALTER TABLE %s ADD COLUMN %s %s', [ATableName, AColumnName, AType]);
  end;
  AConnection.ExecuteDirect(LSQL);
end;

end.
