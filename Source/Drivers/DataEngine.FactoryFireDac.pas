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

unit DataEngine.FactoryFireDac;

interface

uses
  DB,
  Classes,
  SysUtils,
  FireDAC.Comp.Client,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryFireDAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TFDConnection;
      const ADriver: TDriverName); overload;
    constructor Create(const AConnection: TFDConnection; const ADriver: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TFDConnection; const ADriver: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverFireDac,
  DataEngine.DriverFireDacTransaction;

{ TFactoryFireDAC }

constructor TFactoryFireDAC.Create(const AConnection: TFDConnection;
  const ADriver: TDriverName);
begin
  FDriverTransaction := TDriverFireDACTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverFireDAC.Create(AConnection,
                                              FDriverTransaction,
                                              ADriver,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryFireDAC.Create(const AConnection: TFDConnection;
  const ADriver: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriver);
end;

destructor TFactoryFireDAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

procedure TFactoryFireDAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TFDTransaction) then
    raise Exception.Create('Invalid transaction type. Expected TFDTransaction.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryFireDAC.Create(const AConnection: TFDConnection;
  const ADriver: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriver);
end;

end.







