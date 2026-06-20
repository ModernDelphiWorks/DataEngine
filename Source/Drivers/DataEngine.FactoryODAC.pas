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

unit DataEngine.FactoryODAC;

interface

uses
  DB,
  Classes,
  SysUtils,
  Ora,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryODAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TOraSession;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TOraSession;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TOraSession;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverODAC,
  DataEngine.DriverODACTransaction;

{ TFactoryODAC }

constructor TFactoryODAC.Create(const AConnection: TOraSession;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverODACTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverODAC.Create(AConnection,
                                           FDriverTransaction,
                                           ADriverName,
                                           FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryODAC.Create(const AConnection: TOraSession;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryODAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TOraSession) then
    raise Exception.Create('Invalid transaction type. Expected TOraSession.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryODAC.Create(const AConnection: TOraSession;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryODAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.





