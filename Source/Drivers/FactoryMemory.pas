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

unit FactoryMemory;

interface

uses
  Classes,
  SysUtils,
  FactoryConnection,
  FactoryInterfaces,
  DriverMemory;

type
  TFactoryMemory = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TComponent; const ADriver: TDBEngineDriver); overload;
    constructor Create(const AConnection: TComponent; const ADriver: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TComponent; const ADriver: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DriverMemoryTransaction;

{ TFactoryMemory }

constructor TFactoryMemory.Create(const AConnection: TComponent;
  const ADriver: TDBEngineDriver);
begin
  // Create driver transaction first, which expects TMemoryConnection
  FDriverTransaction := TDriverMemoryTransaction.Create(AConnection);
  
  // Create driver connection
  FDriverConnection := TDriverMemory.Create(AConnection,
                                            FDriverTransaction,
                                            ADriver,
                                            FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryMemory.Create(const AConnection: TComponent;
  const ADriver: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriver);
end;

constructor TFactoryMemory.Create(const AConnection: TComponent;
  const ADriver: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriver);
end;

destructor TFactoryMemory.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

procedure TFactoryMemory.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  inherited AddTransaction(AKey, ATransaction);
end;

end.
