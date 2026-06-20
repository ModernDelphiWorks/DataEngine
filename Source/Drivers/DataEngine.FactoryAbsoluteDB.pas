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

unit DataEngine.FactoryAbsoluteDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  ABSMain,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TFactoryAbsoluteDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TABSDatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TABSDatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TABSDatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverAbsoluteDB,
  DataEngine.DriverAbsoluteDBTransaction;

{ TFactoryAbsoluteDB }

constructor TFactoryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverAbsoluteDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverAbsoluteDB.Create(AConnection,
                                                 FDriverTransaction,
                                                 ADriverName,
                                                 nil);
  FAutoTransaction := False;
end;

constructor TFactoryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryAbsoluteDB.Create(const AConnection: TABSDatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverAbsoluteDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverAbsoluteDB.Create(AConnection,
                                                 FDriverTransaction,
                                                 ADriverName,
                                                 AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryAbsoluteDB.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryAbsoluteDB.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TABSDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TABSDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.





