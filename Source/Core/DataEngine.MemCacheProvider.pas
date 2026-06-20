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

unit DataEngine.MemCacheProvider;

interface

uses
  SysUtils,
  Classes,
  Generics.Collections,
  Generics.Defaults,
  DataEngine.FactoryInterfaces,
  DataEngine.CacheTypes;

type
  TMemCacheProvider = class(TInterfacedObject, IDBCacheProvider)
  private
    FItems: TObjectDictionary<string, IDBDataSetSnapshot>;
    FTableMap: TObjectDictionary<string, TList<string>>;
    FHitCount: Integer;
    FMissCount: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    
    procedure SetValue(const AKey: string; const ADataSet: IDBDataSetSnapshot; const ATTL: Integer = 0; const ATables: TArray<string> = nil);
    function GetValue(const AKey: string): IDBDataSetSnapshot;
    procedure Clear;
    procedure Evict(const AKey: string);
    procedure InvalidateByTable(const ATableName: string);
    function Count: Integer;
    function HitCount: Integer;
    function MissCount: Integer;
    procedure Prune;
  end;

implementation

{ TMemCacheProvider }

constructor TMemCacheProvider.Create;
begin
  inherited Create;
  FItems := TObjectDictionary<string, IDBDataSetSnapshot>.Create([doOwnsValues]);
  FTableMap := TObjectDictionary<string, TList<string>>.Create([doOwnsValues],
    TEqualityComparer<string>.Construct(
      function(const ALeft, ARight: string): Boolean
      begin
        Result := SameText(ALeft, ARight);
      end,
      function(const AValue: string): Integer
      begin
        Result := TEqualityComparer<string>.Default.GetHashCode(UpperCase(AValue));
      end));
  FHitCount := 0;
  FMissCount := 0;
end;

destructor TMemCacheProvider.Destroy;
begin
  FTableMap.Free;
  FItems.Free;
  inherited;
end;

procedure TMemCacheProvider.SetValue(const AKey: string; const ADataSet: IDBDataSetSnapshot; const ATTL: Integer; const ATables: TArray<string>);
var
  LTable: string;
begin
  FItems.AddOrSetValue(AKey, ADataSet);
  
  if Length(ATables) > 0 then
  begin
    for LTable in ATables do
    begin
      if not FTableMap.ContainsKey(LTable) then
        FTableMap.Add(LTable, TList<string>.Create);
      
      if not FTableMap[LTable].Contains(AKey) then
        FTableMap[LTable].Add(AKey);
    end;
  end;
end;

function TMemCacheProvider.GetValue(const AKey: string): IDBDataSetSnapshot;
var
  LSnapshot: IDBDataSetSnapshot;
begin
  if FItems.TryGetValue(AKey, LSnapshot) then
  begin
    Inc(FHitCount);
    // Return a new isolated view (independent cursor) for each HIT
    Result := LSnapshot.CreateView as IDBDataSetSnapshot;
  end
  else
  begin
    Inc(FMissCount);
    Result := nil;
  end;
end;

procedure TMemCacheProvider.Clear;
begin
  FTableMap.Clear;
  FItems.Clear;
  FHitCount := 0;
  FMissCount := 0;
end;

procedure TMemCacheProvider.Evict(const AKey: string);
begin
  FItems.Remove(AKey);
end;

procedure TMemCacheProvider.InvalidateByTable(const ATableName: string);
var
  LKeys: TList<string>;
  LKey: string;
begin
  if FTableMap.TryGetValue(ATableName, LKeys) then
  begin
    // We create a copy of the list because removing items might affect iteration if not careful,
    // though here we are removing from FItems, but we also want to clear the mapping for this table.
    for LKey in LKeys do
      FItems.Remove(LKey);
    
    LKeys.Clear;
  end;
end;

function TMemCacheProvider.Count: Integer;
begin
  Result := FItems.Count;
end;

function TMemCacheProvider.HitCount: Integer;
begin
  Result := FHitCount;
end;

function TMemCacheProvider.MissCount: Integer;
begin
  Result := FMissCount;
end;

procedure TMemCacheProvider.Prune;
begin
  // Memory cache could implement LRU or just clear if too many, but for now it's simple
end;

end.
