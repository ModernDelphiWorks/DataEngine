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

unit DataEngine.FactoryFIBPlus;

{$IFDEF DATAENGINE_DRIVER_FIBPLUS}

interface

uses
  DB,
  Classes,
  SysUtils,
  FIBDatabase,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryFIBPlus = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TFIBDatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverFIBPlus,
  DataEngine.DriverFIBPlusTransaction;

{ TFactoryFIBPlus }

constructor TFactoryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverFIBPlusTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverFIBPlus.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryFIBPlus.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TFIBDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TFIBDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryFIBPlus.Create(const AConnection: TFIBDatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryFIBPlus.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;


{$ELSE}

interface

implementation

{$ENDIF}

end.

