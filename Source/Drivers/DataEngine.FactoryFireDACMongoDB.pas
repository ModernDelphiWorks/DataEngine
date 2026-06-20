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

unit DataEngine.FactoryFireDACMongoDB;

interface

uses
  DB,
  Classes,
  SysUtils,
  FireDAC.Comp.Client,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces;

type
  TFactoryMongoFireDAC = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TFDConnection;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TFDConnection;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TFDConnection;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverFireDacMongoDB,
  DataEngine.DriverFireDacMongoDBTransaction;

{ TFactoryMongoFireDAC }

constructor TFactoryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverMongoFireDACTransaction.Create(AConnection);
  FDriverConnection  := TDriverMongoFireDAC.Create(AConnection,
                                                   FDriverTransaction,
                                                   ADriverName,
                                                   FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactoryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

procedure TFactoryMongoFireDAC.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TFDConnection) then
    raise Exception.Create('Invalid transaction type. Expected TFDConnection.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactoryMongoFireDAC.Create(const AConnection: TFDConnection;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  FMonitorCallback := AMonitorCallback;
  Create(AConnection, ADriverName);
end;

destructor TFactoryMongoFireDAC.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.




