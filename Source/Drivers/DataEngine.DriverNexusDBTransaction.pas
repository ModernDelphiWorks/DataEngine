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

unit DataEngine.DriverNexusDBTransaction;

{$IFDEF DATAENGINE_DRIVER_NEXUSDB}

interface

uses
  Classes,
  DB,
  SysUtils,
  Generics.Collections,
  nxdb,
  DataEngine.DriverConnection,
  DataEngine.FactoryInterfaces;

type
  TDriverNexusDBTransaction = class(TDriverTransaction)
  protected
    FConnection: TnxDatabase;
  public
    constructor Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc = nil); override;
    destructor Destroy; override;
    procedure StartTransaction(const ALevel: TDBIsolationLevel = ilDefault); override;
    procedure Commit; override;
    procedure Rollback; override;
    function _InTransaction: Boolean; override;
  end;

implementation

{ TDriverNexusDBTransaction }

constructor TDriverNexusDBTransaction.Create(const AConnection: TComponent; const AMonitorCallback: TMonitorProc);
begin
  inherited;
  FConnection := AConnection as TnxDatabase;
  // NexusDB manages transactions on the database component itself
  // We register the connection as the default transaction object
  FTransactionList.Add('DEFAULT', FConnection); 
  FTransactionActive := FConnection;
end;

destructor TDriverNexusDBTransaction.Destroy;
begin
  FConnection := nil;
  inherited;
end;

function TDriverNexusDBTransaction._InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

procedure TDriverNexusDBTransaction.StartTransaction(const ALevel: TDBIsolationLevel);
begin
  if not FConnection.InTransaction then
    FConnection.StartTransaction;
end;

procedure TDriverNexusDBTransaction.Commit;
begin
  FConnection.Commit;
end;

procedure TDriverNexusDBTransaction.Rollback;
begin
  FConnection.Rollback;
end;


{$ELSE}

interface

implementation

{$ENDIF}

end.
