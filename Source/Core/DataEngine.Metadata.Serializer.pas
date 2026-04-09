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

unit DataEngine.Metadata.Serializer;

interface

uses
  DB,
  Classes,
  SysUtils,
  System.JSON;

type
  TMetadataSerializer = class
  public
    class function ToJSON(AFieldDefs: TFieldDefs): string; static;
    class procedure FromJSON(const AJSON: string; ADestination: TFieldDefs); static;
  end;

implementation

{ TMetadataSerializer }

class function TMetadataSerializer.ToJSON(AFieldDefs: TFieldDefs): string;
var
  LArray: TJSONArray;
  LObject: TJSONObject;
  LIndex: Integer;
begin
  LArray := TJSONArray.Create;
  try
    for LIndex := 0 to AFieldDefs.Count - 1 do
    begin
      LObject := TJSONObject.Create;
      LObject.AddPair('Name', AFieldDefs[LIndex].Name);
      LObject.AddPair('DataType', TJSONNumber.Create(Integer(AFieldDefs[LIndex].DataType)));
      LObject.AddPair('Size', TJSONNumber.Create(AFieldDefs[LIndex].Size));
      LObject.AddPair('Precision', TJSONNumber.Create(AFieldDefs[LIndex].Precision));
      LObject.AddPair('Required', TJSONBool.Create(AFieldDefs[LIndex].Required));
      LObject.AddPair('Attributes', 0);

      LArray.AddElement(LObject);
    end;
    Result := LArray.ToJSON;
  finally
    LArray.Free;
  end;
end;

class procedure TMetadataSerializer.FromJSON(const AJSON: string; ADestination: TFieldDefs);
var
  LValue: TJSONValue;
  LArray: TJSONArray;
  LObject: TJSONObject;
  LIndex: Integer;
  LFieldDef: TFieldDef;
begin
  LValue := TJSONObject.ParseJSONValue(AJSON);
  if not Assigned(LValue) or not (LValue is TJSONArray) then
  begin
    LValue.Free;
    Exit;
  end;

  LArray := TJSONArray(LValue);
  try
    ADestination.Clear;
    for LIndex := 0 to LArray.Count - 1 do
    begin
      LObject := TJSONObject(LArray.Items[LIndex]);
      LFieldDef := ADestination.AddFieldDef;
      LFieldDef.Name := LObject.GetValue<string>('Name');
      LFieldDef.DataType := TFieldType(LObject.GetValue<Integer>('DataType'));
      LFieldDef.Size := LObject.GetValue<Integer>('Size');
      LFieldDef.Precision := LObject.GetValue<Integer>('Precision');
      LFieldDef.Required := LObject.GetValue<Boolean>('Required');
      LFieldDef.Attributes := [];

    end;
  finally
    LArray.Free;
  end;
end;

end.
