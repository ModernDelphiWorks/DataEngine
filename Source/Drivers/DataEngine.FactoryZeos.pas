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

unit DataEngine.FactoryZeos;

interface

uses
  DB,
  Classes,
  SysUtils,
  ZConnection,
  ZAbstractConnection,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryZeos = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TZConnection;
      const ADriver: TDBEngineDriver); overload;
    constructor Create(const AConnection: TZConnection;
      const ADriver: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TZConnection;
      const ADriver: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverZeos,
  DataEngine.DriverZeosTransaction;

{ TFactoryZeos }

constructor TFactoryZeos.Create(const AConnection: TZConnection;
  const ADriver: TDBEngineDriver);
begin
  FDriverTransaction := TDriverZeosTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverZeos.Create(AConnection,
                                           FDriverTransaction,
                                           ADriver,
                                           FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryZeos.Create(const AConnection: TZConnection;
  const ADriver: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriver);
end;

constructor TFactoryZeos.Create(const AConnection: TZConnection;
  const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriver);
end;

destructor TFactoryZeos.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

procedure TFactoryZeos.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  {$IFDEF ZEOS80UP}
  if not (ATransaction is TZTransaction) then
    raise Exception.Create('Invalid transaction type. Expected TZTransaction.');
  {$ENDIF}

  inherited AddTransaction(AKey, ATransaction);
end;

end.




