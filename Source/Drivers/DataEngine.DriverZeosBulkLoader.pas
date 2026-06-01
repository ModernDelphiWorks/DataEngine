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

unit DataEngine.DriverZeosBulkLoader;

interface

uses
  Classes,
  SysUtils,
  DB,
  Generics.Collections,
  Variants,
  ZConnection,
  ZDataset,
  DataEngine.DriverBulkLoader,
  DataEngine.FactoryInterfaces;

type
  TDriverZeosBulkLoader = class(TDriverBulkLoader)
  private
    FZQuery: TZQuery;
    FConnection: TZConnection;
    FBuffer: TDictionary<string, TArray<Variant>>;
  protected
    procedure Prepare; override;
    procedure Execute(const ARows: Integer); override;
    procedure SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant); override;
  public
    constructor Create(const AConnection: TZConnection); reintroduce;
    destructor Destroy; override;
  end;

implementation

{ TDriverZeosBulkLoader }

constructor TDriverZeosBulkLoader.Create(const AConnection: TZConnection);
begin
  inherited Create;
  FConnection := AConnection;
  FBuffer := TDictionary<string, TArray<Variant>>.Create;
  FZQuery := TZQuery.Create(nil);
  FZQuery.Connection := FConnection;
end;

destructor TDriverZeosBulkLoader.Destroy;
begin
  FBuffer.Free;
  FZQuery.Free;
  inherited;
end;

procedure TDriverZeosBulkLoader.Prepare;
var
  LFor: Integer;
  LSql: string;
  LFields: string;
  LValues: string;
  LColArr: TArray<Variant>;
begin
  if FTableName = '' then
    raise Exception.Create('Table name not defined for Bulk Loader');
    
  if FParams.Count = 0 then
    raise Exception.Create('Parameters not defined for Bulk Loader');

  LFields := '';
  LValues := '';
  for LFor := 0 to FParams.Count - 1 do
  begin
    if LFields <> '' then
    begin
      LFields := LFields + ', ';
      LValues := LValues + ', ';
    end;
    LFields := LFields + FParams[LFor].Name;
    LValues := LValues + ':' + FParams[LFor].Name;
  end;

  LSql := Format('INSERT INTO %s (%s) VALUES (%s)', [FTableName, LFields, LValues]);
  FZQuery.SQL.Text := LSql;
  
  FBuffer.Clear;
  for LFor := 0 to FParams.Count - 1 do
  begin
    SetLength(LColArr, FBatchSize);
    FBuffer.Add(UpperCase(FParams[LFor].Name), LColArr);
    
    with FZQuery.Params.ParamByName(FParams[LFor].Name) do
      DataType := FParams[LFor].DataType;
  end;
  
  FZQuery.Prepare;
end;

procedure TDriverZeosBulkLoader.Execute(const ARows: Integer);
var
  LRow: Integer;
  LParam: Integer;
  LColArr: TArray<Variant>;
begin
  if ARows <= 0 then Exit;
  
  // Zeos fallback: loop inside current connection context
  for LRow := 0 to ARows - 1 do
  begin
    for LParam := 0 to FParams.Count - 1 do
    begin
      if FBuffer.TryGetValue(UpperCase(FParams[LParam].Name), LColArr) then
        FZQuery.Params[LParam].Value := LColArr[LRow];
    end;
    FZQuery.ExecSQL;
  end;
end;

procedure TDriverZeosBulkLoader.SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant);
var
  LColArr: TArray<Variant>;
  LKey: string;
begin
  LKey := UpperCase(AParamName);
  if FBuffer.TryGetValue(LKey, LColArr) then
  begin
    LColArr[ARidx] := AValue;
    FBuffer[LKey] := LColArr;
  end;
end;

end.
