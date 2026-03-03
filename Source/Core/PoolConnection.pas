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

unit PoolConnection;

interface

uses
  SysUtils,
  SyncObjs,
  DateUtils,
  Generics.Collections,
  FactoryInterfaces;

type
  TPoolConnection = class
  private
    FConnections: TList<IDBConnection>;
    FBusyConnections: TList<IDBConnection>;
    FLock: TCriticalSection;
    FMaxConnections: Integer;
    FConnectionLifeCycle: Integer;
    FConnectionFactory: TFunc<IDBConnection>;
    FCreationTimes: TDictionary<IDBConnection, TDateTime>;
    FTimeout: Integer;
    function _CreateNewConnection: IDBConnection;
    function _IsConnectionExpired(const AConnection: IDBConnection): Boolean;
  public
    constructor Create(const AMaxConnections: Integer; const AConnectionLifetime: Integer;
      const AConnectionFactory: TFunc<IDBConnection>; const ATimeout: Integer = 5000);
    destructor Destroy; override;
    function AcquireConnection: IDBConnection;
    procedure ReleaseConnection(const AConnection: IDBConnection);
    procedure CleanupExpiredConnections;
    property MaxConnections: Integer read FMaxConnections;
    property ConnectionLifeCycle: Integer read FConnectionLifeCycle;
    property Timeout: Integer read FTimeout;
  end;

implementation

constructor TPoolConnection.Create(const AMaxConnections: Integer; const AConnectionLifetime: Integer;
  const AConnectionFactory: TFunc<IDBConnection>; const ATimeout: Integer = 5000);
begin
  if AMaxConnections <= 0 then
    raise Exception.Create('The maximum number of connections must be greater than zero');
  if not Assigned(AConnectionFactory) then
    raise Exception.Create('The connection factory must be provided');

  FConnections := TList<IDBConnection>.Create;
  FBusyConnections := TList<IDBConnection>.Create;
  FCreationTimes := TDictionary<IDBConnection, TDateTime>.Create;
  FLock := TCriticalSection.Create;
  FMaxConnections := AMaxConnections;
  FConnectionLifeCycle := AConnectionLifetime;
  FConnectionFactory := AConnectionFactory;
  FTimeout := ATimeout;
end;

destructor TPoolConnection.Destroy;
begin
  FLock.Acquire;
  try
    FConnections.Clear;
    FBusyConnections.Clear;
    FCreationTimes.Clear;
    FConnections.Free;
    FBusyConnections.Free;
    FCreationTimes.Free;
  finally
    FLock.Release;
  end;
  FLock.Free;
  inherited;
end;

function TPoolConnection._CreateNewConnection: IDBConnection;
begin
  Result := FConnectionFactory();
  FCreationTimes.Add(Result, Now);
end;

function TPoolConnection._IsConnectionExpired(const AConnection: IDBConnection): Boolean;
var
  LCreationTime: TDateTime;
begin
  if FConnectionLifeCycle <= 0 then
    Exit(False);
  LCreationTime := FCreationTimes[AConnection];
  Result := SecondsBetween(Now, LCreationTime) > FConnectionLifeCycle;
end;

function TPoolConnection.AcquireConnection: IDBConnection;
var
  LStartTime: TDateTime;
begin
  LStartTime := Now;
  while True do
  begin
    FLock.Acquire;
    try
      if FConnections.Count > 0 then
      begin
        Result := FConnections.ExtractAt(0);
        if _IsConnectionExpired(Result) then
        begin
          FCreationTimes.Remove(Result);
          Result := _CreateNewConnection;
        end;
        FBusyConnections.Add(Result);
        Exit;
      end
      else if FBusyConnections.Count < FMaxConnections then
      begin
        Result := _CreateNewConnection;
        FBusyConnections.Add(Result);
        Exit;
      end;
    finally
      FLock.Release;
    end;
    if MilliSecondsBetween(Now, LStartTime) > FTimeout then
      raise Exception.Create('Timeout acquiring connection from pool');

    Sleep(50);
  end;
end;

procedure TPoolConnection.ReleaseConnection(const AConnection: IDBConnection);
begin
  FLock.Acquire;
  try
    if FBusyConnections.Remove(AConnection) >= 0 then
    begin
      if not _IsConnectionExpired(AConnection) then
        FConnections.Add(AConnection)
      else
      begin
        FCreationTimes.Remove(AConnection);
      end;
    end
    else
      raise Exception.Create('Connection not found in busy list');
  finally
    FLock.Release;
  end;
end;

procedure TPoolConnection.CleanupExpiredConnections;
var
  LFor: Integer;
  LConn: IDBConnection;
begin
  FLock.Acquire;
  try
    for LFor := FConnections.Count - 1 downto 0 do
    begin
      LConn := FConnections[LFor];
      if _IsConnectionExpired(LConn) then
      begin
        FConnections.Delete(LFor);
        FCreationTimes.Remove(LConn);
      end;
    end;
  finally
    FLock.Release;
  end;
end;

end.
