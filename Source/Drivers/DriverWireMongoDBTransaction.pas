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

unit DriverWireMongoDBTransaction;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  MongoWireConnection,
  DriverConnection;

type
  TDriverWireMongoDBTransaction = class(TDriverTransaction)
  protected
    FConnection: TMongoWireConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverWireMongoDBTransaction }

constructor TDriverWireMongoDBTransaction.Create(const AConnection: TComponent);
begin
  inherited Create(AConnection);
  if not (AConnection is TMongoWireConnection) then
    raise Exception.Create('Invalid connection type. Expected TMongoWireConnection.');
    
  FConnection := TMongoWireConnection(AConnection);
  // MongoWire connection acts as the transaction context
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverWireMongoDBTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverWireMongoDBTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');
    
  // MongoDB doesn't strictly support multi-statement transactions in the same way as SQL,
  // or at least this wrapper implementation doesn't expose state.
  // Returning False implies no active transaction state to check against.
  Result := False; 
end;

procedure TDriverWireMongoDBTransaction.StartTransaction;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  // No-op for MongoWire wrapper
end;

procedure TDriverWireMongoDBTransaction.Commit;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  // No-op for MongoWire wrapper
end;

procedure TDriverWireMongoDBTransaction.Rollback;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined.');

  // No-op for MongoWire wrapper
end;

end.
