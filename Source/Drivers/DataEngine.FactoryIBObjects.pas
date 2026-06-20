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

unit DataEngine.FactoryIBObjects;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  IB_Components,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TFactoryIBObjects = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TIBODatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TIBODatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TIBODatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverIBObjects,
  DataEngine.DriverIBObjectsTransaction;

{ TFactoryIBObjects }

constructor TFactoryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverIBObjectsTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverIBObjects.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                nil);
  FAutoTransaction := False;
end;

constructor TFactoryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryIBObjects.Create(const AConnection: TIBODatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverIBObjectsTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverIBObjects.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryIBObjects.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryIBObjects.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TIBODatabase) then
    raise Exception.Create('Invalid transaction type. Expected TIBODatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.





