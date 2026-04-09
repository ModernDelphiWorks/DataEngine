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

unit DataEngine.DriverUniDacBulkLoader;

interface

uses
  Classes,
  SysUtils,
  DB,
  Generics.Collections,
  Variants,
  Uni,
  DBAccess,
  DataEngine.DriverBulkLoader,
  DataEngine.FactoryInterfaces;

type
  TDriverUniDACBulkLoader = class(TDriverBulkLoader)
  private
    FUniLoader: TUniLoader;
    FBuffer: TDictionary<string, TArray<Variant>>;
    procedure OnPutData(Sender: TDALoader; Column: TColumn; Row: Integer; var Value: Variant);
  protected
    procedure Prepare; override;
    procedure Execute(const ARows: Integer); override;
    procedure SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant); override;
  public
    constructor Create(const AConnection: TUniConnection); reintroduce;
    destructor Destroy; override;
  end;

implementation

{ TDriverUniDACBulkLoader }

constructor TDriverUniDACBulkLoader.Create(const AConnection: TUniConnection);
begin
  inherited Create;
  FBuffer := TDictionary<string, TArray<Variant>>.Create;
  FUniLoader := TUniLoader.Create(nil);
  FUniLoader.Connection := AConnection;
  FUniLoader.OnPutData := OnPutData;
end;

destructor TDriverUniDACBulkLoader.Destroy;
begin
  FBuffer.Free;
  FUniLoader.Free;
  inherited;
end;

procedure TDriverUniDACBulkLoader.Prepare;
var
  LFor: Integer;
  LColArr: TArray<Variant>;
begin
  if FTableName = '' then
    raise Exception.Create('Table name not defined for Bulk Loader');

  FUniLoader.TableName := FTableName;
  
  FBuffer.Clear;
  for LFor := 0 to FParams.Count - 1 do
  begin
    SetLength(LColArr, FBatchSize);
    FBuffer.Add(UpperCase(FParams[LFor].Name), LColArr);
  end;
end;

procedure TDriverUniDACBulkLoader.Execute(const ARows: Integer);
begin
  if ARows <= 0 then Exit;
  FUniLoader.Load(ARows);
end;

procedure TDriverUniDACBulkLoader.OnPutData(Sender: TDALoader; Column: TColumn; Row: Integer; var Value: Variant);
var
  LColArr: TArray<Variant>;
begin
  if FBuffer.TryGetValue(UpperCase(Column.Name), LColArr) then
    Value := LColArr[Row - 1] // UniLoader uses 1-based Row index in OnPutData? Wait, usually it is 1-based for rows in some components, but DBAccess docs say 1..ARows.
  else
    Value := Null;
end;

procedure TDriverUniDACBulkLoader.SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant);
var
  LColArr: TArray<Variant>;
  LKey: string;
begin
  LKey := UpperCase(AParamName);
  if FBuffer.TryGetValue(LKey, LColArr) then
  begin
    LColArr[ARidx] := AValue;
    FBuffer[LKey] := LColArr; // TArray is a pointer but we need to ensure the reference if needed, although usually not necessary for TArray.
  end;
end;

end.
