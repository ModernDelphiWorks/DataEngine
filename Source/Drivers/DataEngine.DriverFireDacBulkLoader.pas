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

unit DataEngine.DriverFireDacBulkLoader;

interface

uses
  Classes,
  SysUtils,
  DB,
  FireDAC.Comp.Client,
  FireDAC.Stan.Param,
  DataEngine.DriverBulkLoader,
  DataEngine.FactoryInterfaces;

type
  TDriverFireDACBulkLoader = class(TDriverBulkLoader)
  private
    FFDQuery: TFDQuery;
    FConnection: TFDConnection;
    FTransaction: TFDTransaction;
  protected
    procedure Prepare; override;
    procedure Execute(const ARows: Integer); override;
    procedure SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant); override;
  public
    constructor Create(const AConnection: TFDConnection; const ATransaction: TFDTransaction); reintroduce;
    destructor Destroy; override;
  end;

implementation

{ TDriverFireDACBulkLoader }

constructor TDriverFireDACBulkLoader.Create(const AConnection: TFDConnection; const ATransaction: TFDTransaction);
begin
  inherited Create;
  FConnection := AConnection;
  FTransaction := ATransaction;
  FFDQuery := TFDQuery.Create(nil);
  FFDQuery.Connection := FConnection;
  FFDQuery.Transaction := FTransaction;
end;

destructor TDriverFireDACBulkLoader.Destroy;
begin
  FFDQuery.Free;
  inherited;
end;

procedure TDriverFireDACBulkLoader.Prepare;
var
  LFor: Integer;
  LSql: string;
  LFields: string;
  LValues: string;
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
  FFDQuery.SQL.Text := LSql;
  
  // No FireDAC o TFDQuery.Params carrega o buffer do Array DML.
  // Precisamos configurar os tipos dos parâmetros para bater com FParams.
  FFDQuery.Params.Clear;
  for LFor := 0 to FParams.Count - 1 do
  begin
    with FFDQuery.Params.Add do
    begin
      Name := FParams[LFor].Name;
      DataType := FParams[LFor].DataType;
    end;
  end;
  
  FFDQuery.Params.ArraySize := FBatchSize;
  FFDQuery.Prepare;
end;

procedure TDriverFireDACBulkLoader.Execute(const ARows: Integer);
begin
  if ARows <= 0 then Exit;
  FFDQuery.Execute(ARows, 0);
end;

procedure TDriverFireDACBulkLoader.SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant);
begin
  FFDQuery.ParamByName(AParamName).Values[ARidx] := AValue;
end;

end.
