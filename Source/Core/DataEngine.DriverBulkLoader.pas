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

unit DataEngine.DriverBulkLoader;

interface

uses
  Classes,
  SysUtils,
  DB,
  DataEngine.FactoryInterfaces;

type
  TDriverBulkLoader = class abstract(TInterfacedObject, IDBBulkLoader)
  protected
    FTableName: string;
    FBatchSize: Integer;
    FParams: TParams;
    function _GetTableName: string; virtual;
    procedure _SetTableName(const AValue: string); virtual;
    function _GetBatchSize: Integer; virtual;
    procedure _SetBatchSize(const AValue: Integer); virtual;
    function _GetParams: TParams; virtual;
  public
    procedure Prepare; virtual; abstract;
    procedure Execute(const ARows: Integer); virtual; abstract;
    procedure Clear; virtual;
    procedure SetValue(const AParamName: string; const ARidx: Integer; const AValue: Variant); virtual; abstract;
    function ParamByName(const AValue: string): TParam; virtual;
    constructor Create; virtual;
    destructor Destroy; override;
    property TableName: string read _GetTableName write _SetTableName;
    property BatchSize: Integer read _GetBatchSize write _SetBatchSize;
    property Params: TParams read _GetParams;
  end;

implementation

{ TDriverBulkLoader }

constructor TDriverBulkLoader.Create;
begin
  inherited Create;
  FParams := TParams.Create(nil);
  FBatchSize := 1000;
end;

destructor TDriverBulkLoader.Destroy;
begin
  FParams.Free;
  inherited;
end;

procedure TDriverBulkLoader.Clear;
begin
  FParams.Clear;
end;

function TDriverBulkLoader.ParamByName(const AValue: string): TParam;
begin
  Result := FParams.ParamByName(AValue);
end;

function TDriverBulkLoader._GetBatchSize: Integer;
begin
  Result := FBatchSize;
end;

function TDriverBulkLoader._GetParams: TParams;
begin
  Result := FParams;
end;

function TDriverBulkLoader._GetTableName: string;
begin
  Result := FTableName;
end;

procedure TDriverBulkLoader._SetBatchSize(const AValue: Integer);
begin
  FBatchSize := AValue;
end;

procedure TDriverBulkLoader._SetTableName(const AValue: string);
begin
  FTableName := AValue;
end;

end.
