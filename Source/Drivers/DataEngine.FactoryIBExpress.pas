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

unit DataEngine.FactoryIBExpress;

interface

uses
  DB,
  Classes,
  SysUtils,
  IBDatabase,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryIBExpress = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TIBDatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TIBDatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TIBDatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverIBExpress,
  DataEngine.DriverIBExpressTransaction;

{ TFactoryIBExpress }

constructor TFactoryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverIBExpressTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverIBExpress.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryIBExpress.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  // IBExpress uses TIBTransaction
  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryIBExpress.Create(const AConnection: TIBDatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryIBExpress.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.





