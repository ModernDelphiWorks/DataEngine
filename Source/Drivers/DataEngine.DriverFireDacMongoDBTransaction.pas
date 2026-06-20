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

unit DataEngine.DriverFireDacMongoDBTransaction;

{$IFDEF DATAENGINE_DRIVER_FIREDACMONGODB}

interface

uses
  DB,
  Classes,
  SysUtils,
  Generics.Collections,
  FireDAC.Comp.Client,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverMongoFireDACTransaction = class(TDriverTransaction)
  protected
    FConnection: TFDConnection;
  public
    constructor Create(const AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
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

function TDriverMongoFireDACTransaction._InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

procedure TDriverMongoFireDACTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
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

{$ELSE}
interface
implementation
{$ENDIF}

end.
