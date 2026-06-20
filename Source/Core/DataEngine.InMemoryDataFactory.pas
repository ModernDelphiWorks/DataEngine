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

unit DataEngine.InMemoryDataFactory;

interface

uses
  DB, Classes, SysUtils;

type
  TInMemoryDataFactory = class
  public
    class function CreateDataSet(AOwner: TComponent = nil): TDataSet; static;
    class function CloneDataSet(ASource: TDataSet): TDataSet; static;
    class function CreateFromDataSet(ASource: TDataSet): TDataSet; static;
    class procedure SaveToStream(ASource: TDataSet; AStream: TStream); static;
    class procedure LoadFromStream(ATarget: TDataSet; AStream: TStream); static;
  end;

implementation

uses
  FireDAC.Comp.Client,
  FireDAC.Comp.DataSet;

class function TInMemoryDataFactory.CreateDataSet(AOwner: TComponent): TDataSet;
begin
  Result := TFDMemTable.Create(AOwner);
end;

class function TInMemoryDataFactory.CloneDataSet(ASource: TDataSet): TDataSet;
var
  LStream: TMemoryStream;
begin
  Result := CreateDataSet(nil);
  LStream := TMemoryStream.Create;
  try
    SaveToStream(ASource, LStream);
    LStream.Position := 0;
    LoadFromStream(Result, LStream);
  finally
    LStream.Free;
  end;
end;

class function TInMemoryDataFactory.CreateFromDataSet(ASource: TDataSet): TDataSet;
var
  LTarget: TFDMemTable;
begin
  LTarget := TFDMemTable.Create(nil);
  try
    LTarget.CopyDataSet(ASource, [coStructure, coRestart, coAppend]);
    Result := LTarget;
  except
    LTarget.Free;
    raise;
  end;
end;

class procedure TInMemoryDataFactory.SaveToStream(ASource: TDataSet; AStream: TStream);
begin
  if ASource is TFDMemTable then
    TFDMemTable(ASource).SaveToStream(AStream);
end;

class procedure TInMemoryDataFactory.LoadFromStream(ATarget: TDataSet; AStream: TStream);
begin
  if ATarget is TFDMemTable then
    TFDMemTable(ATarget).LoadFromStream(AStream);
end;

end.
