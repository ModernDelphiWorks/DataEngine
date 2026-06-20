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

unit DataEngine.FactoryElevateDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  edbcomps,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TFactoryElevateDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TEDBDatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverElevateDB,
  DataEngine.DriverElevateDBTransaction;

{ TFactoryElevateDB }

constructor TFactoryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverElevateDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverElevateDB.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                nil);
  FAutoTransaction := False;
end;

constructor TFactoryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryElevateDB.Create(const AConnection: TEDBDatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverElevateDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverElevateDB.Create(AConnection,
                                                FDriverTransaction,
                                                ADriverName,
                                                AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryElevateDB.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryElevateDB.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TEDBDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TEDBDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.





