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

unit DataEngine.FactoryMemory;

// Optional in-memory driver (depends on System.Fluent, absent on minimal Linux SDK). Enable with DATAENGINE_DRIVER_MEMORY.
{$IFDEF DATAENGINE_DRIVER_MEMORY}

interface

uses
  Classes,
  SysUtils,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverMemory;

type
  TFactoryMemory = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TComponent; const ADriver: TDriverName); overload;
    constructor Create(const AConnection: TComponent; const ADriver: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TComponent; const ADriver: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverMemoryTransaction;

{ TFactoryMemory }

constructor TFactoryMemory.Create(const AConnection: TComponent;
  const ADriver: TDriverName);
begin
  // Create driver transaction first, which expects TMemoryConnection
  FDriverTransaction := TDriverMemoryTransaction.Create(AConnection, FMonitorCallback);
  
  // Create driver connection
  FDriverConnection := TDriverMemory.Create(AConnection,
                                            FDriverTransaction,
                                            ADriver,
                                            FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryMemory.Create(const AConnection: TComponent;
  const ADriver: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriver);
end;

constructor TFactoryMemory.Create(const AConnection: TComponent;
  const ADriver: TDriverName; const AMonitorCallback: TMonitorProc);
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

{$ELSE}
interface
implementation
{$ENDIF}

end.





