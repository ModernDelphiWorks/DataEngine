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

unit DataEngine.FactoryWireMongoDB;

interface

uses
  System.Classes,
  System.SysUtils,
  Data.DB,
  MongoWireConnection,
  DataEngine.FactoryConnection,
  DataEngine.FactoryInterfaces,
  DataEngine.DriverConnection;

type
  TFactoryWireMongoDB = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TMongoWireConnection;
      const ADriverName: TDBEngineDriver); overload;
    constructor Create(const AConnection: TMongoWireConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TMongoWireConnection;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  DataEngine.DriverWireMongoDB,
  DataEngine.DriverWireMongoDBTransaction;

{ TFactoryWireMongoDB }

constructor TFactoryWireMongoDB.Create(const AConnection: TMongoWireConnection;
  const ADriverName: TDBEngineDriver);
begin
  FDriverTransaction := TDriverWireMongoDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverWireMongoDB.Create(AConnection,
                                                  FDriverTransaction,
                                                  ADriverName,
                                                  nil);
  FAutoTransaction := False;
end;

constructor TFactoryWireMongoDB.Create(const AConnection: TMongoWireConnection;
  const ADriverName: TDBEngineDriver; const AMonitor: ICommandMonitor);
begin
  FCommandMonitor := AMonitor;
  Create(AConnection, ADriverName);
end;

constructor TFactoryWireMongoDB.Create(const AConnection: TMongoWireConnection;
  const ADriverName: TDBEngineDriver; const AMonitorCallback: TMonitorProc);
begin
  FDriverTransaction := TDriverWireMongoDBTransaction.Create(AConnection, FMonitorCallback);
  FDriverConnection  := TDriverWireMongoDB.Create(AConnection,
                                                  FDriverTransaction,
                                                  ADriverName,
                                                  AMonitorCallback);
  FMonitorCallback := AMonitorCallback;
  FAutoTransaction := False;
end;

destructor TFactoryWireMongoDB.Destroy;
begin
  FreeAndNil(FDriverConnection);
  FreeAndNil(FDriverTransaction);
  inherited;
end;

procedure TFactoryWireMongoDB.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TMongoWireConnection) then
    raise Exception.Create('Invalid transaction type. Expected TMongoWireConnection.');

  inherited AddTransaction(AKey, ATransaction);
end;

end.





