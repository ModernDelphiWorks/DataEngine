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

unit DataEngine.CacheManager;

interface

uses
  DB,
  Classes,
  SysUtils,
  Variants,
  System.Hash,
  Generics.Collections,
  DataEngine.FactoryInterfaces,
  DataEngine.CacheTypes;

type
  TCacheManager = class
  public
    class function GenerateQueryHash(const ASQL: string; AParams: TParams; const ADriver: TDBEngineDriver): string; static;
    class function CreateSnapshot(const ASource: IDBDataSet): IDBDataSetSnapshot; static;
    class procedure SerializeToStream(const ASnapshot: IDBDataSetSnapshot; AStream: TStream); static;
    class function DeserializeFromStream(AStream: TStream): IDBDataSetSnapshot; static;
    class function ExtractTables(const ASQL: string): TArray<string>; static;
    class procedure InvalidateBySQL(ACache: IDBCacheProvider; const ASQL: string; AParams: TParams; AMonitor: TMonitorProc); static;
  end;

implementation

uses
  DataEngine.DataSetSnapshot,
  DataEngine.InMemoryDataFactory,
  MidasLib,
  System.RegularExpressions;

{ TCacheManager }

class function TCacheManager.GenerateQueryHash(const ASQL: string; AParams: TParams; const ADriver: TDBEngineDriver): string;
var
  LInput: string;
  LIndex: Integer;
begin
  LInput := 'DRIVER:' + IntToStr(Ord(ADriver)) + '|SQL:' + ASQL;
  
  if Assigned(AParams) and (AParams.Count > 0) then
  begin
    LInput := LInput + '|PARAMS:';
    for LIndex := 0 to AParams.Count - 1 do
    begin
      LInput := LInput + AParams[LIndex].Name + '=' + VarToStr(AParams[LIndex].Value) + ';';
    end;
  end;

  Result := THashMD5.GetHashString(LInput);
end;

class function TCacheManager.CreateSnapshot(const ASource: IDBDataSet): IDBDataSetSnapshot;
var
  LData: TDataSet;
begin
  if not Assigned(ASource) then
    Exit(nil);

  LData := TInMemoryDataFactory.CreateFromDataSet(ASource.AsDataSet);
  Result := TDataSetSnapshot.Create(LData);
end;

class procedure TCacheManager.SerializeToStream(const ASnapshot: IDBDataSetSnapshot; AStream: TStream);
begin
  if not Assigned(ASnapshot) then Exit;
  TInMemoryDataFactory.SaveToStream(ASnapshot.AsDataSet, AStream);
end;

class function TCacheManager.DeserializeFromStream(AStream: TStream): IDBDataSetSnapshot;
var
  LData: TDataSet;
begin
  LData := TInMemoryDataFactory.CreateDataSet(nil);
  try
    AStream.Position := 0;
    TInMemoryDataFactory.LoadFromStream(LData, AStream);
    Result := TDataSetSnapshot.Create(LData);
  except
    LData.Free;
    Result := nil;
  end;
end;

class function TCacheManager.ExtractTables(const ASQL: string): TArray<string>;
var
  LMatches: TMatchCollection;
  LMatch: TMatch;
  LTables: TStringList;
  LPattern: string;
begin
  LTables := TStringList.Create;
  try
    LTables.CaseSensitive := False;
    LTables.Duplicates := dupIgnore;
    LTables.Sorted := True;

    // Pattern to catch tables after FROM, INTO, UPDATE, JOIN
    LPattern := '\b(FROM|INTO|UPDATE|JOIN)\s+([\[\]\w\."]+)(\s+|,|$)';
    LMatches := TRegEx.Matches(ASQL, LPattern, [roIgnoreCase, roMultiline]);
    
    for LMatch in LMatches do
    begin
      if LMatch.Groups.Count >= 3 then
      begin
        // Group 2 is the table name
        LTables.Add(LMatch.Groups.Item[2].Value.Trim(['[', ']', '"']));
      end;
    end;

    Result := LTables.ToStringArray;
  finally
    LTables.Free;
  end;
end;

class procedure TCacheManager.InvalidateBySQL(ACache: IDBCacheProvider; const ASQL: string; AParams: TParams; AMonitor: TMonitorProc);
var
  LTables: TArray<string>;
  LTable: string;
begin
  if not Assigned(ACache) then
    Exit;
    
  LTables := ExtractTables(ASQL);
  for LTable in LTables do
  begin
    if Assigned(AMonitor) then
      AMonitor(TMonitorParam.Create('[CACHE INVALIDATE] ' + LTable, AParams));
    ACache.InvalidateByTable(LTable);
  end;
end;

end.
