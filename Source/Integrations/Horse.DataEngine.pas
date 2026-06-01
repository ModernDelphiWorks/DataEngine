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

unit Horse.DataEngine;

interface

uses
  Horse,
  DataEngine.FactoryInterfaces,
  DataEngine.PoolConnection,
  System.SysUtils,
  System.Classes,
  System.Generics.Collections;

type
  TTenantProvider = reference to function(Req: THorseRequest): string;

  IHorseDataEngineConfig = interface
    ['{88B2A6B3-3067-4632-BD11-2092D3EF897B}']
    function GetPoolManager: IDBPoolManager;
    function GetAutoTransaction: Boolean;
    procedure SetAutoTransaction(const Value: Boolean);
    function GetTenantProvider: TTenantProvider;
    procedure SetTenantProvider(const Value: TTenantProvider);
    function GetDefaultTenant: string;
    procedure SetDefaultTenant(const Value: string);
    function GetConnectionFactory: TFunc<IDBConnection>;
    procedure SetConnectionFactory(const Value: TFunc<IDBConnection>);
    function GetOnConnectionAcquired: TProc<THorseRequest, IDBConnection>;
    procedure SetOnConnectionAcquired(const Value: TProc<THorseRequest, IDBConnection>);

    property PoolManager: IDBPoolManager read GetPoolManager;
    property AutoTransaction: Boolean read GetAutoTransaction write SetAutoTransaction;
    property TenantProvider: TTenantProvider read GetTenantProvider write SetTenantProvider;
    property DefaultTenant: string read GetDefaultTenant write SetDefaultTenant;
    property ConnectionFactory: TFunc<IDBConnection> read GetConnectionFactory write SetConnectionFactory;
    property OnConnectionAcquired: TProc<THorseRequest, IDBConnection> read GetOnConnectionAcquired write SetOnConnectionAcquired;
  end;

  THorseDataEngineConfig = class(TInterfacedObject, IHorseDataEngineConfig)
  private
    FPoolManager: IDBPoolManager;
    FAutoTransaction: Boolean;
    FTenantProvider: TTenantProvider;
    FDefaultTenant: string;
    FConnectionFactory: TFunc<IDBConnection>;
    FOnConnectionAcquired: TProc<THorseRequest, IDBConnection>;
    
    function GetPoolManager: IDBPoolManager;
    function GetAutoTransaction: Boolean;
    procedure SetAutoTransaction(const Value: Boolean);
    function GetTenantProvider: TTenantProvider;
    procedure SetTenantProvider(const Value: TTenantProvider);
    function GetDefaultTenant: string;
    procedure SetDefaultTenant(const Value: string);
    function GetConnectionFactory: TFunc<IDBConnection>;
    procedure SetConnectionFactory(const Value: TFunc<IDBConnection>);
    function GetOnConnectionAcquired: TProc<THorseRequest, IDBConnection>;
    procedure SetOnConnectionAcquired(const Value: TProc<THorseRequest, IDBConnection>);
  public
    constructor Create(const APoolManager: IDBPoolManager);
    class function New(const APoolManager: IDBPoolManager): IHorseDataEngineConfig;
  end;

  THorseDataEngine = class
  public
    class function Middleware(const APoolManager: IDBPoolManager; const AConnectionFactory: TFunc<IDBConnection> = nil): THorseCallback; overload;
    class function Middleware(const AConfig: IHorseDataEngineConfig): THorseCallback; overload;
  end;

function HorseDataEngine(const APoolManager: IDBPoolManager; const AConnectionFactory: TFunc<IDBConnection> = nil): THorseCallback; overload;
function HorseDataEngine(const AConfig: IHorseDataEngineConfig): THorseCallback; overload;

{ Helper to avoid manual casting in routes }
type
  THorseRequestHelper = class helper for THorseRequest
  public
    function SessionIDBConnection: IDBConnection;
  end;

implementation

type
  { Internal wrapper to store Interface in TObject-based Session }
  TDataEngineSession = class
  private
    FConnection: IDBConnection;
  public
    constructor Create(const AConnection: IDBConnection);
    property Connection: IDBConnection read FConnection;
  end;

{ TDataEngineSession }

constructor TDataEngineSession.Create(const AConnection: IDBConnection);
begin
  inherited Create;
  FConnection := AConnection;
end;

{ THorseDataEngineConfig }

constructor THorseDataEngineConfig.Create(const APoolManager: IDBPoolManager);
begin
  inherited Create;
  FPoolManager := APoolManager;
  FAutoTransaction := True;
  FDefaultTenant := 'default';
end;

function THorseDataEngineConfig.GetAutoTransaction: Boolean;
begin
  Result := FAutoTransaction;
end;

function THorseDataEngineConfig.GetConnectionFactory: TFunc<IDBConnection>;
begin
  Result := FConnectionFactory;
end;

function THorseDataEngineConfig.GetDefaultTenant: string;
begin
  Result := FDefaultTenant;
end;

function THorseDataEngineConfig.GetOnConnectionAcquired: TProc<THorseRequest, IDBConnection>;
begin
  Result := FOnConnectionAcquired;
end;

function THorseDataEngineConfig.GetPoolManager: IDBPoolManager;
begin
  Result := FPoolManager;
end;

function THorseDataEngineConfig.GetTenantProvider: TTenantProvider;
begin
  Result := FTenantProvider;
end;

class function THorseDataEngineConfig.New(const APoolManager: IDBPoolManager): IHorseDataEngineConfig;
begin
  Result := THorseDataEngineConfig.Create(APoolManager);
end;

procedure THorseDataEngineConfig.SetAutoTransaction(const Value: Boolean);
begin
  FAutoTransaction := Value;
end;

procedure THorseDataEngineConfig.SetConnectionFactory(const Value: TFunc<IDBConnection>);
begin
  FConnectionFactory := Value;
end;

procedure THorseDataEngineConfig.SetDefaultTenant(const Value: string);
begin
  FDefaultTenant := Value;
end;

procedure THorseDataEngineConfig.SetOnConnectionAcquired(const Value: TProc<THorseRequest, IDBConnection>);
begin
  FOnConnectionAcquired := Value;
end;

procedure THorseDataEngineConfig.SetTenantProvider(const Value: TTenantProvider);
begin
  FTenantProvider := Value;
end;

{ THorseDataEngine }

class function THorseDataEngine.Middleware(const APoolManager: IDBPoolManager; const AConnectionFactory: TFunc<IDBConnection>): THorseCallback;
var
  LConfig: IHorseDataEngineConfig;
begin
  LConfig := THorseDataEngineConfig.New(APoolManager);
  LConfig.ConnectionFactory := AConnectionFactory;
  Result := Middleware(LConfig);
end;

class function THorseDataEngine.Middleware(const AConfig: IHorseDataEngineConfig): THorseCallback;
begin
  Result :=
    procedure(Req: THorseRequest; Res: THorseResponse; Next: TNextProc)
    var
      LTenantID: string;
      LConnection: IDBConnection;
      LSessionData: TDataEngineSession;
    begin
      LTenantID := AConfig.DefaultTenant;
      if Assigned(AConfig.TenantProvider) then
        LTenantID := AConfig.TenantProvider(Req);

      LConnection := AConfig.PoolManager.AcquireConnection(LTenantID, AConfig.ConnectionFactory);
      try
        LSessionData := TDataEngineSession.Create(LConnection);
        try
          Req.Session.AddOrSetValue('IDBConnection', LSessionData);

          if Assigned(AConfig.OnConnectionAcquired) then
            AConfig.OnConnectionAcquired(Req, LConnection);
          
          if AConfig.AutoTransaction then
          begin
            if not LConnection.InTransaction then
              LConnection.StartTransaction;
          end;

          try
            try
              Next;
              
              if AConfig.AutoTransaction and (Res.Status < 400) and LConnection.InTransaction then
                LConnection.Commit;
            except
              if AConfig.AutoTransaction and LConnection.InTransaction then
                LConnection.Rollback;
              raise;
            end;

            if AConfig.AutoTransaction and (Res.Status >= 400) and LConnection.InTransaction then
              LConnection.Rollback;
              
          finally
            Req.Session.Remove('IDBConnection');
          end;
        finally
          LSessionData.Free;
        end;

      finally
        AConfig.PoolManager.ReleaseConnection(LTenantID, LConnection);
      end;
    end;
end;

{ Global functions }

function HorseDataEngine(const APoolManager: IDBPoolManager; const AConnectionFactory: TFunc<IDBConnection>): THorseCallback;
begin
  Result := THorseDataEngine.Middleware(APoolManager, AConnectionFactory);
end;

function HorseDataEngine(const AConfig: IHorseDataEngineConfig): THorseCallback;
begin
  Result := THorseDataEngine.Middleware(AConfig);
end;

{ THorseRequestHelper }

function THorseRequestHelper.SessionIDBConnection: IDBConnection;
var
  LObj: TObject;
begin
  Result := nil;
  if Session.TryGetValue('IDBConnection', LObj) and (LObj is TDataEngineSession) then
    Result := TDataEngineSession(LObj).Connection;
end;

end.
