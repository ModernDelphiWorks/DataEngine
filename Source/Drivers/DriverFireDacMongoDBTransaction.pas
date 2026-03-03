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

unit DriverFireDacMongoDBTransaction;

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  FireDAC.Comp.Client,
  DriverConnection,
  FactoryInterfaces;

type
  TDriverMongoFireDACTransaction = class(TDriverTransaction)
  protected
    FConnection: TFDConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverMongoFireDACTransaction }

constructor TDriverMongoFireDACTransaction.Create(const AConnection: TComponent);
begin
  inherited;
  FConnection := AConnection as TFDConnection;
  // MongoDB transactions are generally managed by the connection context or session
  // For the purpose of this framework, we track the connection as the transaction object
  FTransactionList.Add('DEFAULT', FConnection);
  FTransactionActive := FConnection;
end;

destructor TDriverMongoFireDACTransaction.Destroy;
begin
  FTransactionActive := nil;
  FConnection := nil;
  inherited;
end;

function TDriverMongoFireDACTransaction.InTransaction: Boolean;
begin
  if not Assigned(FTransactionActive) then
    raise Exception.Create('The active transaction is not defined. Please make sure to start a transaction before checking if it is in progress.');

  Result := FConnection.InTransaction;
end;

procedure TDriverMongoFireDACTransaction.StartTransaction;
begin
  // MongoDB supports multi-document transactions in replica sets from v4.0
  // FireDAC supports this via StartTransaction if configured correctly.
  FConnection.StartTransaction;
end;

procedure TDriverMongoFireDACTransaction.Commit;
begin
  FConnection.Commit;
end;

procedure TDriverMongoFireDACTransaction.Rollback;
begin
  FConnection.Rollback;
end;

end.
