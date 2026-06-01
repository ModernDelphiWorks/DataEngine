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

unit DataEngine.Metadata.Manager;

interface

uses
  System.SysUtils,
  System.Classes,
  DB,
  System.Hash,
  DataEngine.FactoryInterfaces,
  DataEngine.Metadata.Serializer;

type
  TMetadataCacheManager = class
  public
    class function GenerateKey(const ATableName: string; const ADriver: TDBEngineDriver): string; static;
    class function TryGetMetadata(const AMetadataCache: IDBMetadataCache; const ADriver: TDBEngineDriver; const ATableName: string; ADestination: TFieldDefs): Boolean; static;
    class procedure SaveMetadata(const AMetadataCache: IDBMetadataCache; const ADriver: TDBEngineDriver; const ATableName: string; AFieldDefs: TFieldDefs); static;
  end;

implementation

{ TMetadataCacheManager }

class function TMetadataCacheManager.GenerateKey(const ATableName: string; const ADriver: TDBEngineDriver): string;
begin
  Result := THashMD5.GetHashString(IntToStr(Ord(ADriver)) + ':' + UpperCase(ATableName));
end;

class function TMetadataCacheManager.TryGetMetadata(const AMetadataCache: IDBMetadataCache; const ADriver: TDBEngineDriver; const ATableName: string; ADestination: TFieldDefs): Boolean;
var
  LJSON: string;
  LKey: string;
begin
  Result := False;
  if not Assigned(AMetadataCache) then
    Exit;

  LKey := GenerateKey(ATableName, ADriver);
  LJSON := AMetadataCache.GetMetadata(LKey);
  
  if not LJSON.IsEmpty then
  begin
    TMetadataSerializer.FromJSON(LJSON, ADestination);
    Result := True;
  end;
end;

class procedure TMetadataCacheManager.SaveMetadata(const AMetadataCache: IDBMetadataCache; const ADriver: TDBEngineDriver; const ATableName: string; AFieldDefs: TFieldDefs);
var
  LJSON: string;
  LKey: string;
begin
  if not Assigned(AMetadataCache) or not Assigned(AFieldDefs) then
    Exit;

  LKey := GenerateKey(ATableName, ADriver);
  LJSON := TMetadataSerializer.ToJSON(AFieldDefs);
  AMetadataCache.SetMetadata(LKey, LJSON);
end;

end.
