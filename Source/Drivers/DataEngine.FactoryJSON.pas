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

unit DataEngine.FactoryJSON;

// Optional experimental JSON-file driver (incomplete). Enable with DATAENGINE_DRIVER_JSON.
{$IFDEF DATAENGINE_DRIVER_JSON}

interface

uses
  DB,
  Classes,
  SysUtils,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverJSON;

type
  TFactoryJSON = class(TFactoryConnection)
  private
    FInternalConnection: TJSONConnection;
  public
    constructor Create(const AFileName: string); overload;
    constructor Create(const AFileName: string; const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
  end;

implementation

{ TFactoryJSON }

constructor TFactoryJSON.Create(const AFileName: string);
begin
  FInternalConnection := TJSONConnection.Create(nil);
  FInternalConnection.FileName := AFileName;

  FDriverTransaction := TDriverTransactionJSON.Create(FInternalConnection, FMonitorCallback);
  FDriverTransaction.AddTransaction('Default', FInternalConnection);
  FDriverTransaction.UseTransaction('Default');

  FDriverConnection := TDriverConnectionJSON.Create(FInternalConnection,
                                                   FDriverTransaction,
                                                   dnMemory, // JSON is basically memory-first
                                                   FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryJSON.Create(const AFileName: string;
  const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AFileName);
end;

destructor TFactoryJSON.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  FInternalConnection.Free;
  inherited;
end;

{$ELSE}
interface
implementation
{$ENDIF}

end.
