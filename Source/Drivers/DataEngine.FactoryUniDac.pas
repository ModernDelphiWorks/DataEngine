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

unit DataEngine.FactoryUniDac;

interface

uses
  DB,
  Classes,
  SysUtils,
  Uni,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryUniDAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TUniConnection;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TUniConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TUniConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverUniDac,
  DataEngine.DriverUniDacTransaction;

{ TFactoryUniDAC }

constructor TFactoryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverUniDACTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverUniDAC.Create(AConnection,
                                             FDriverTransaction,
                                             ADriverName,
                                             FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryUniDAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // UniDAC uses TUniTransaction
  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryUniDAC.Create(const AConnection: TUniConnection;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryUniDAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.




