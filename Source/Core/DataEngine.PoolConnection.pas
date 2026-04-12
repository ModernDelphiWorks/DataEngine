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

unit DataEngine.PoolConnection;

interface

uses
  SysUtils,
  SyncObjs,
  DateUtils,
  Generics.Collections,
  DataEngine.FactoryInterfaces;

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
    FSlotTokens: TDictionary<IDBConnection, string>;
    FTenantID: string;
    FCoordinator: IDBPoolCoordinator;
    FTimeout: Integer;
    function _CreateNewConnection: IDBConnection;
    function _IsConnectionExpired(const AConnection: IDBConnection): Boolean;
  public
    constructor Create(const ATenantID: string; const AMaxConnections: Integer; const AConnectionLifetime: Integer;
      const AConnectionFactory: TFunc<IDBConnection>; const ATimeout: Integer = 5000; const ACoordinator: IDBPoolCoordinator = nil);
    destructor Destroy; override;
    function AcquireConnection: IDBConnection;
    procedure ReleaseConnection(const AConnection: IDBConnection);
    procedure CleanupExpiredConnections;
    property MaxConnections: Integer read FMaxConnections;
    property ConnectionLifeCycle: Integer read FConnectionLifeCycle;
    property Timeout: Integer read FTimeout;
    property TenantID: string read FTenantID;
    property Coordinator: IDBPoolCoordinator read FCoordinator write FCoordinator;
  end;

  TPoolManager = class(TInterfacedObject, IDBPoolManager)
  private
    FPools: TObjectDictionary<string, TPoolConnection>;
    FCoordinator: IDBPoolCoordinator;
    FLock: TCriticalSection;
    FDefaultTimeout: Integer;
  public
    constructor Create;
    destructor Destroy; override;
    function AcquireConnection(const ATenantID: string; const AFactory: TFunc<IDBConnection>;
      const AMaxConnections: Integer = 10; const ALifetime: Integer = 600): IDBConnection;
    procedure ReleaseConnection(const ATenantID: string; const AConnection: IDBConnection);
    procedure Clear;
    procedure Cleanup;
    procedure SetCoordinator(const ACoordinator: IDBPoolCoordinator);
    procedure SetDefaultTimeout(const ATimeout: Integer);
  end;

function PoolManager: IDBPoolManager;

implementation

constructor TPoolConnection.Create(const ATenantID: string; const AMaxConnections: Integer; const AConnectionLifetime: Integer;
  const AConnectionFactory: TFunc<IDBConnection>; const ATimeout: Integer = 5000; const ACoordinator: IDBPoolCoordinator = nil);
begin
  if ATenantID = '' then
    raise Exception.Create('TenantID cannot be empty');
  if AMaxConnections <= 0 then
    raise Exception.Create('The maximum number of connections must be greater than zero');
  if not Assigned(AConnectionFactory) then
    raise Exception.Create('The connection factory must be provided');

  FConnections := TList<IDBConnection>.Create;
  FBusyConnections := TList<IDBConnection>.Create;
  FCreationTimes := TDictionary<IDBConnection, TDateTime>.Create;
  FSlotTokens := TDictionary<IDBConnection, string>.Create;
  FLock := TCriticalSection.Create;
  FTenantID := ATenantID;
  FMaxConnections := AMaxConnections;
  FConnectionLifeCycle := AConnectionLifetime;
  FConnectionFactory := AConnectionFactory;
  FTimeout := ATimeout;
  FCoordinator := ACoordinator;
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
    FSlotTokens.Free;
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
  LCandidate: IDBConnection;
  LToken: string;
  LIsNew: Boolean;
begin
  LStartTime := Now;
  while True do
  begin
    LCandidate := nil;
    LIsNew := False;

    // Step 1: Try to get a candidate (idle or create new) while holding the lock
    FLock.Acquire;
    try
      if FConnections.Count > 0 then
      begin
        LCandidate := FConnections.ExtractAt(0);
        if _IsConnectionExpired(LCandidate) or (not LCandidate.IsAlive) then
        begin
          FCreationTimes.Remove(LCandidate);
          LCandidate := _CreateNewConnection;
          LIsNew := True;
        end;
      end
      else if FBusyConnections.Count < FMaxConnections then
      begin
        LCandidate := _CreateNewConnection;
        LIsNew := True;
      end;

      // If we have a candidate but NO coordinator, finalize locally and return
      if Assigned(LCandidate) and (not Assigned(FCoordinator)) then
      begin
        FBusyConnections.Add(LCandidate);
        Result := LCandidate;
        Exit;
      end;
    finally
      FLock.Release;
    end;

    // Step 2: If we have a candidate and a coordinator, call coordinator OUTSIDE the lock
    if Assigned(LCandidate) and Assigned(FCoordinator) then
    begin
      try
        if FCoordinator.AcquireSlot(FTenantID, FMaxConnections, FTimeout, LToken) then
        begin
          // Step 3: Success! Finalize under lock
          FLock.Acquire;
          try
            FSlotTokens.AddOrSetValue(LCandidate, LToken);
            FBusyConnections.Add(LCandidate);
            Result := LCandidate;
            Exit;
          finally
            FLock.Release;
          end;
        end;
      except
        // On hard error, treat as failed acquisition to trigger retry/timeout
      end;

      // Step 4: If we reach here, distributed acquisition failed.
      // Return candidate to idle list to avoid leak
      FLock.Acquire;
      try
        if LCandidate.IsAlive then
          FConnections.Add(LCandidate)
        else
          FCreationTimes.Remove(LCandidate);
      finally
        FLock.Release;
      end;
      LCandidate := nil;
    end;

    // Check timeout
    if MilliSecondsBetween(Now, LStartTime) > FTimeout then
      raise Exception.Create('Timeout acquiring connection from pool');

    Sleep(50);
  end;
end;

procedure TPoolConnection.ReleaseConnection(const AConnection: IDBConnection);
var
  LToken: string;
  LHasToken: Boolean;
begin
  LHasToken := False;
  FLock.Acquire;
  try
    if FBusyConnections.Remove(AConnection) >= 0 then
    begin
      if not _IsConnectionExpired(AConnection) then
        FConnections.Add(AConnection)
      else
        FCreationTimes.Remove(AConnection);

      if Assigned(FCoordinator) then
      begin
        LHasToken := FSlotTokens.TryGetValue(AConnection, LToken);
        if LHasToken then
          FSlotTokens.Remove(AConnection);
      end;
    end
    else
      raise Exception.Create('Connection not found in busy list');
  finally
    FLock.Release;
  end;

  // Release distributed slot outside the lock to avoid blocking other threads
  if LHasToken then
    FCoordinator.ReleaseSlot(FTenantID, LToken);
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

var
  vPoolManager: IDBPoolManager;
  vPoolManagerLock: TCriticalSection;

function PoolManager: IDBPoolManager;
begin
  if not Assigned(vPoolManager) then
  begin
    vPoolManagerLock.Acquire;
    try
      if not Assigned(vPoolManager) then
        vPoolManager := TPoolManager.Create;
    finally
      vPoolManagerLock.Release;
    end;
  end;
  Result := vPoolManager;
end;

{ TPoolManager }

constructor TPoolManager.Create;
begin
  FPools := TObjectDictionary<string, TPoolConnection>.Create([doOwnsValues]);
  FLock := TCriticalSection.Create;
  FDefaultTimeout := 5000;
end;

destructor TPoolManager.Destroy;
begin
  FLock.Free;
  FPools.Free;
  inherited;
end;

function TPoolManager.AcquireConnection(const ATenantID: string; const AFactory: TFunc<IDBConnection>;
  const AMaxConnections: Integer; const ALifetime: Integer): IDBConnection;
var
  LPool: TPoolConnection;
begin
  FLock.Acquire;
  try
    if not FPools.TryGetValue(ATenantID, LPool) then
    begin
      LPool := TPoolConnection.Create(ATenantID, AMaxConnections, ALifetime, AFactory, FDefaultTimeout, FCoordinator);
      FPools.Add(ATenantID, LPool);
    end;
  finally
    FLock.Release;
  end;
  Result := LPool.AcquireConnection;
end;

procedure TPoolManager.ReleaseConnection(const ATenantID: string; const AConnection: IDBConnection);
var
  LPool: TPoolConnection;
begin
  FLock.Acquire;
  try
    if FPools.TryGetValue(ATenantID, LPool) then
      LPool.ReleaseConnection(AConnection);
  finally
    FLock.Release;
  end;
end;

procedure TPoolManager.Clear;
begin
  FLock.Acquire;
  try
    FPools.Clear;
  finally
    FLock.Release;
  end;
end;

procedure TPoolManager.Cleanup;
var
  LPool: TPoolConnection;
begin
  FLock.Acquire;
  try
    for LPool in FPools.Values do
      LPool.CleanupExpiredConnections;
  finally
    FLock.Release;
  end;
end;

procedure TPoolManager.SetCoordinator(const ACoordinator: IDBPoolCoordinator);
var
  LPool: TPoolConnection;
begin
  FLock.Acquire;
  try
    FCoordinator := ACoordinator;
    for LPool in FPools.Values do
      LPool.Coordinator := FCoordinator;
  finally
    FLock.Release;
  end;
end;

procedure TPoolManager.SetDefaultTimeout(const ATimeout: Integer);
begin
  FLock.Acquire;
  try
    FDefaultTimeout := ATimeout;
  finally
    FLock.Release;
  end;
end;

initialization
  vPoolManagerLock := TCriticalSection.Create;

finalization
  vPoolManager := nil;
  vPoolManagerLock.Free;

end.

