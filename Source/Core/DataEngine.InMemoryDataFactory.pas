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
  Datasnap.DBClient, Datasnap.Provider;

class function TInMemoryDataFactory.CreateDataSet(AOwner: TComponent): TDataSet;
begin
  Result := TClientDataSet.Create(AOwner);
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
  LProvider: TDataSetProvider;
  LTargetCDS: TClientDataSet;
begin
  LTargetCDS := TClientDataSet.Create(nil);
  LProvider := TDataSetProvider.Create(nil);
  try
    LProvider.DataSet := ASource;
    LTargetCDS.Data := LProvider.Data;
    Result := LTargetCDS;
  finally
    LProvider.Free;
  end;
end;

class procedure TInMemoryDataFactory.SaveToStream(ASource: TDataSet; AStream: TStream);
begin
  if ASource is TClientDataSet then
    TClientDataSet(ASource).SaveToStream(AStream);
end;

class procedure TInMemoryDataFactory.LoadFromStream(ATarget: TDataSet; AStream: TStream);
begin
  if ATarget is TClientDataSet then
    TClientDataSet(ATarget).LoadFromStream(AStream);
end;

end.
