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

{$ifdef fpc}
  {$mode delphi}{$H+}
{$endif}

unit DataEngine.GuardConnection;

interface

uses
  SysUtils,
  DataEngine.FactoryInterfaces,
  DataEngine.PoolConnection;

type
  TConnectionGuardBuilder = class;

  TGuardConnection = class
  private
    FPool: TPoolConnection;
  public
    constructor Create(const ABuilder: TConnectionGuardBuilder);
    destructor Destroy; override;
    procedure UseConnection(const AAction: TProc<IDBConnection>);
  end;

  TConnectionGuardBuilder = class
  private
    FMaxConnections: Integer;
    FConnectionLifeCycle: Integer;
    FConnectionFactory: TFunc<IDBConnection>;
    class var FGuardConnection: TGuardConnection;
  public
    function Limit(const AValue: Integer): TConnectionGuardBuilder;
    function LifeCycle(const AValue: Integer): TConnectionGuardBuilder;
    function WithFactory(const AFactory: TFunc<IDBConnection>): TConnectionGuardBuilder;
    function Build: TGuardConnection;
  end;

function SetupGuard: TConnectionGuardBuilder;
procedure UseConnection(const AAction: TProc<IDBConnection>);

implementation

function SetupGuard: TConnectionGuardBuilder;
begin
  Result := TConnectionGuardBuilder.Create;
end;

procedure UseConnection(const AAction: TProc<IDBConnection>);
begin
  TConnectionGuardBuilder.FGuardConnection.UseConnection(AAction);
end;

{ TConnectionGuard }

constructor TGuardConnection.Create(const ABuilder: TConnectionGuardBuilder);
begin
  FPool := TPoolConnection.Create(ABuilder.FMaxConnections,
                                  ABuilder.FConnectionLifeCycle,
                                  ABuilder.FConnectionFactory);
end;

destructor TGuardConnection.Destroy;
begin
  FPool.Free;
  inherited;
end;

procedure TGuardConnection.UseConnection(const AAction: TProc<IDBConnection>);
var
  LConnection: IDBConnection;
begin
  LConnection := FPool.AcquireConnection;
  try
    AAction(LConnection);
  finally
    FPool.ReleaseConnection(LConnection);
  end;
end;

{ TConnectionGuardBuilder }

function TConnectionGuardBuilder.Limit(const AValue: Integer): TConnectionGuardBuilder;
begin
  FMaxConnections := AValue;
  Result := Self;
end;

function TConnectionGuardBuilder.LifeCycle(const AValue: Integer): TConnectionGuardBuilder;
begin
  FConnectionLifeCycle := AValue;
  Result := Self;
end;

function TConnectionGuardBuilder.WithFactory(const AFactory: TFunc<IDBConnection>): TConnectionGuardBuilder;
begin
  FConnectionFactory := AFactory;
  Result := Self;
end;

function TConnectionGuardBuilder.Build: TGuardConnection;
begin
  FGuardConnection := TGuardConnection.Create(Self);
  Result := FGuardConnection;
end;

end.
