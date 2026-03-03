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

unit DriverDBExpress;

interface

uses
  Classes,
  SysUtils,
  StrUtils,
  Variants,
  DB,
  SqlExpr,
  DBXCommon,
  DBClient,
  Datasnap.Provider,
  DriverConnection,
  DriverDBExpressTransaction,
  FactoryInterfaces;

type
  TDriverDBExpress = class(TDriverConnection)
  private
    function _GetTransactionActive: TDBXTransaction;
  protected
    FConnection: TSQLConnection;
    FSQLScript: TSQLQuery;
  public
    constructor Create(const AConnection: TComponent;
      const ADriverTransaction: TDriverTransaction;
      const ADriverName: TDBEngineDriver;
      const AMonitorCallback: TMonitorProc); override;
    destructor Destroy; override;
    procedure Connect; override;
    procedure Disconnect; override;
    procedure ExecuteDirect(const ASQL: String); override;
    procedure ExecuteDirect(const ASQL: String; const AParams: TParams); override;
    procedure ExecuteScript(const AScript: String); override;
    procedure AddScript(const AScript: String); override;
    procedure ExecuteScripts; override;
    procedure ApplyUpdates(const ADataSets: array of IDBDataSet); override;
    function IsConnected: Boolean; override;
    function CreateQuery: IDBQuery; override;
    function CreateDataSet(const ASQL: String = ''): IDBDataSet; override;
    function GetSQLScripts: String; override;
  end;

  TDriverQueryDBExpress = class(TDriverQuery)
  private
    FSQLQuery: TSQLQuery;
    function _GetTransactionActive: TDBXTransaction;
  protected
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
  public
    constructor Create(const AConnection: TSQLConnection;
      const ADriverTransaction: TDriverTransaction;
      const AMonitorCallback: TMonitorProc);
    destructor Destroy; override;
    procedure ExecuteDirect; override;
    function ExecuteQuery: IDBDataSet; override;
    function RowsAffected: UInt32; override;
  end;

  TDriverDataSetDBExpress = class(TDriverDataSet<TClientDataSet>)
  private
    FSQLQuery: TSQLQuery;
    FProvider: TDataSetProvider;
  protected
    procedure _SetUniDirectional(const Value: Boolean); override;
    procedure _SetReadOnly(const Value: Boolean); override;
    procedure _SetCachedUpdates(const Value: Boolean); override;
    procedure _SetCommandText(const ACommandText: String); override;
    function _GetCommandText: String; override;
    function _Iso8601ToDateTime(const AValue: String): TDateTime;
    function _GetFieldValue(const FieldName: string): Variant; override;
  public
    constructor Create(const ADataSet: TClientDataSet;
      const ASQLQuery: TSQLQuery; const AProvider: TDataSetProvider;
      const AMonitorCallback: TMonitorProc); reintroduce;
    destructor Destroy; override;
    procedure Open; override;
    procedure ApplyUpdates; override;
    procedure CancelUpdates; override;
    function RowsAffected: UInt32; override;
    function IsUniDirectional: Boolean; override;
    function IsReadOnly: Boolean; override;
    function IsCachedUpdates: Boolean; override;
  end;

implementation

{ TDriverDBExpress }

constructor TDriverDBExpress.Create(const AConnection: TComponent;
  const ADriverTransaction: TDriverTransaction;
  const ADriverName: TDBEngineDriver;
  const AMonitorCallback: TMonitorProc);
begin
  FConnection := AConnection as TSQLConnection;
  FDriverTransaction := ADriverTransaction;
  FDriver := ADriverName;
  FMonitorCallback := AMonitorCallback;
  FSQLScript  := TSQLQuery.Create(nil);
  try
    FSQLScript.SQLConnection := FConnection;
    FSQLScript.SQL.Clear;
  except
    FSQLScript.Free;
    raise;
  end;
end;

destructor TDriverDBExpress.Destroy;
begin
  FDriverTransaction := nil;
  FConnection := nil;
  FSQLScript.Free;
  inherited;
end;

procedure TDriverDBExpress.Disconnect;
begin
  FConnection.Connected := False;
end;

procedure TDriverDBExpress.ExecuteDirect(const ASQL: String);
var
  LExeSQL: TSQLQuery;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TSQLQuery.Create(nil);
  try
    LExeSQL.SQLConnection := FConnection;
    LExeSQL.SQL.Text := ASQL;
    try
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

procedure TDriverDBExpress.ExecuteDirect(const ASQL: String; const AParams: TParams);
var
  LExeSQL: TSQLQuery;
  LFor: Int16;
begin
  if not Assigned(FConnection) then
    raise Exception.Create('Connection not assigned.');
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');
  if ASQL = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TSQLQuery.Create(nil);
  try
    LExeSQL.SQLConnection := FConnection;
    LExeSQL.SQL.Text := ASQL;
    for LFor := 0 to AParams.Count - 1 do
    begin
      LExeSQL.ParamByName(AParams[LFor].Name).DataType := AParams[LFor].DataType;
      LExeSQL.ParamByName(AParams[LFor].Name).Value := AParams[LFor].Value;
    end;
    try
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(ASQL, E.Message, LExeSQL.Params);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

procedure TDriverDBExpress.ExecuteScript(const AScript: String);
begin
  AddScript(AScript);
  ExecuteScripts;
end;

procedure TDriverDBExpress.ExecuteScripts;
begin
  if FSQLScript.SQL.Count = 0 then
    raise Exception.Create('No SQL scripts found to execute.');

  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  try
    try
      FSQLScript.ExecSQL;
      FRowsAffected := FSQLScript.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog('Error during script execution', E.Message, nil);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(FSQLScript.SQL.Text, 'DEFAULT', nil);
    FSQLScript.SQL.Clear;
  end;
end;

function TDriverDBExpress.GetSQLScripts: String;
begin
  Result := 'Transaction: ' + 'DEFAULT' + ' ' +  FSQLScript.SQL.Text;
end;

function TDriverDBExpress.IsConnected: Boolean;
begin
  Result := FConnection.Connected = True;
end;

procedure TDriverDBExpress.AddScript(const AScript: String);
begin
  if Self.GetDriver in [dnInterbase, dnFirebird, dnFirebird3] then
    if FSQLScript.SQL.Count = 0 then
      FSQLScript.SQL.Add('SET AUTOCOMMIT OFF');
  FSQLScript.SQL.Add(AScript);
end;

procedure TDriverDBExpress.ApplyUpdates(const ADataSets: array of IDBDataSet);
var
  LDataSet: IDBDataSet;
begin
  for LDataset in AdataSets do
    LDataset.ApplyUpdates;
end;

procedure TDriverDBExpress.Connect;
begin
  FConnection.Connected := True;
end;

function TDriverDBExpress._GetTransactionActive: TDBXTransaction;
begin
  if Assigned(FDriverTransaction.TransactionActive) and
     (FDriverTransaction.TransactionActive is TDBXTransactionWrapper) then
    Result := TDBXTransactionWrapper(FDriverTransaction.TransactionActive).Transaction
  else
    Result := nil;
end;

function TDriverDBExpress.CreateQuery: IDBQuery;
begin
  Result := TDriverQueryDBExpress.Create(FConnection,
                                         FDriverTransaction,
                                         FMonitorCallback);
end;

function TDriverDBExpress.CreateDataSet(const ASQL: String): IDBDataSet;
var
  LDBQuery: IDBQuery;
begin
  LDBQuery := TDriverQueryDBExpress.Create(FConnection,
                                           FDriverTransaction,
                                           FMonitorCallback);
  LDBQuery.CommandText := ASQL;
  Result := LDBQuery.ExecuteQuery;
end;

{ TDriverQueryDBExpress }

constructor TDriverQueryDBExpress.Create(const AConnection: TSQLConnection;
  const ADriverTransaction: TDriverTransaction;
  const AMonitorCallback: TMonitorProc);
begin
  if AConnection = nil then
    raise Exception.Create('AConnection cannot be nil');
  if ADriverTransaction = nil then
    raise Exception.Create('ADriverTransaction cannot be nil');

  FDriverTransaction := ADriverTransaction;
  FMonitorCallback := AMonitorCallback;
  FSQLQuery := TSQLQuery.Create(nil);
  try
    FSQLQuery.SQLConnection := AConnection;
  except
    FSQLQuery.Free;
    raise;
  end;
end;

destructor TDriverQueryDBExpress.Destroy;
begin
  FSQLQuery.Free;
  inherited;
end;

function TDriverQueryDBExpress.ExecuteQuery: IDBDataSet;
var
  LSQLQuery: TSQLQuery;
  LResultSet: TClientDataSet;
  LProvider: TDataSetProvider;
  LFor: Int16;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  LSQLQuery := TSQLQuery.Create(nil);
  LProvider := TDataSetProvider.Create(nil);
  LResultSet := TClientDataSet.Create(nil);
  try
    LSQLQuery.SQLConnection := FSQLQuery.SQLConnection;
    LProvider.DataSet := LSQLQuery;
    LProvider.Name := 'ProviderName';
    LResultSet.ProviderName := LProvider.Name;
    LResultSet.CommandText := FSQLQuery.SQL.Text;
    
    if LResultSet.CommandText = '' then
      raise Exception.Create('SQL statement is empty. Cannot execute the query.');

    try
      for LFor := 0 to FSQLQuery.Params.Count - 1 do
      begin
        LResultSet.Params[LFor].DataType := FSQLQuery.Params[LFor].DataType;
        LResultSet.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
      end;
      
      LResultSet.Open;
      
      Result := TDriverDataSetDBExpress.Create(LResultSet,
                                                 LSQLQuery,
                                                 LProvider,
                                                 FMonitorCallback);
      if LResultSet.Active then
      begin
        /// <summary>
        /// if LResultSet.RecordCount = 0 then
        /// Ao checar Recordcount no DBXExpress da um erro de Object Inv�lid para o SQL
        /// select name as name, ' ' as description from sys.sequences
        /// </summary>
        if LResultSet.Eof then
          Result.FetchingAll := True;
      end;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LResultSet.CommandText, E.Message, LResultSet.Params);
        raise;
      end;
    end;
  finally
    if LResultSet.CommandText <> EmptyStr then
      _SetMonitorLog(LResultSet.CommandText, 'DEFAULT', LResultSet.Params);
      
    // Note: LSQLQuery, LProvider, LResultSet ownership is complicated here.
    // If Result is created, it takes ownership. If exception raised, we free in except block below.
  end;
  except
    if Assigned(LSQLQuery) then // If Result created, these are owned by Result/TDriverDataSetDBExpress?
    begin
       // TDriverDataSetDBExpress.Create takes ownership of LSQLQuery and LProvider?
       // Looking at Create: FSQLQuery := ASQLQuery; FProvider := AProvider;
       // Looking at Destroy: FSQLQuery.Free; FProvider.Free;
       // So yes, if Result is created, we should NOT free them here.
       // But if exception happened BEFORE Result creation, we must free them.
       // The try..finally block above is tricky.
       
       if Result = nil then
       begin
         LSQLQuery.Free;
         LResultSet.Free;
         LProvider.Free;
       end;
    end;
    raise;
  end;
end;

function TDriverQueryDBExpress.RowsAffected: UInt32;
begin
  Result := FRowsAffected;
end;

function TDriverQueryDBExpress._GetCommandText: String;
begin
  Result := FSQLQuery.SQL.Text;
end;

function TDriverQueryDBExpress._GetTransactionActive: TDBXTransaction;
begin
  if Assigned(FDriverTransaction.TransactionActive) and
     (FDriverTransaction.TransactionActive is TDBXTransactionWrapper) then
    Result := TDBXTransactionWrapper(FDriverTransaction.TransactionActive).Transaction
  else
    Result := nil;
end;

procedure TDriverQueryDBExpress._SetCommandText(const ACommandText: String);
begin
  FSQLQuery.SQL.Text := ACommandText;
end;

procedure TDriverQueryDBExpress.ExecuteDirect;
var
  LExeSQL: TSQLQuery;
  LFor: Int16;
begin
  if _GetTransactionActive = nil then
    raise Exception.Create('Transaction not assigned.');

  if FSQLQuery.SQL.Text = '' then
    raise Exception.Create('SQL statement is empty. Cannot execute the query.');

  LExeSQL := TSQLQuery.Create(nil);
  try
    LExeSQL.SQLConnection := FSQLQuery.SQLConnection;
    LExeSQL.SQL.Text := FSQLQuery.SQL.Text;
    for LFor := 0 to FSQLQuery.Params.Count - 1 do
    begin
      LExeSQL.Params[LFor].DataType := FSQLQuery.Params[LFor].DataType;
      LExeSQL.Params[LFor].Value := FSQLQuery.Params[LFor].Value;
    end;
    
    try
      LExeSQL.ExecSQL;
      FRowsAffected := LExeSQL.RowsAffected;
    except
      on E: Exception do
      begin
        _SetMonitorLog(LExeSQL.SQL.Text, E.Message, LExeSQL.Params);
        raise;
      end;
    end;
  finally
    _SetMonitorLog(LExeSQL.SQL.Text, 'DEFAULT', LExeSQL.Params);
    LExeSQL.Free;
  end;
end;

{ TDriverDataSetDBExpress }

procedure TDriverDataSetDBExpress.ApplyUpdates;
begin
  FDataSet.ApplyUpdates(0);
end;

procedure TDriverDataSetDBExpress.CancelUpdates;
begin
  FDataSet.CancelUpdates;
end;

constructor TDriverDataSetDBExpress.Create(const ADataSet: TClientDataSet;
  const ASQLQuery: TSQLQuery; const AProvider: TDataSetProvider;
  const AMonitorCallback: TMonitorProc);
begin
  FSQLQuery := ASQLQuery;
  FProvider := AProvider;
  inherited Create(ADataSet, AMonitorCallback);
end;

destructor TDriverDataSetDBExpress.Destroy;
begin
  FSQLQuery.Free;
  FProvider.Free;
  inherited;
end;

function TDriverDataSetDBExpress._GetFieldValue(const FieldName: string): Variant;
var
  LValue: Variant;
  LField: TField;
begin
  LField := FDataSet.FindField(FieldName);
  if LField = nil then
    Exit(Variants.Null);
    
  if LField.IsNull then
    Result := Variants.Null
  else
  begin
    LValue := LField.Value;
    // Usando DBExpress para acessar SQLite os campos data retornam no
    // formato ISO8601 "yyyy-MM-dd e o DBExpress n�o converte para dd-MM-yyy,
    // ent�o tive que criar uma alternativa.
    if (FSQLQuery.SQLConnection.DriverName = 'Sqlite') and (VarType(LValue) = varString) then
    begin
      if (Copy(LValue,5,1) = '-') and (Copy(LValue,8,1) = '-') then
      begin
         Result := _Iso8601ToDateTime(LValue);
         Exit;
      end;
    end;
    Result := LValue;
  end;
end;

function TDriverDataSetDBExpress.IsCachedUpdates: Boolean;
begin
  Result := False; 
end;

function TDriverDataSetDBExpress.IsReadOnly: Boolean;
begin
  Result := FDataSet.ReadOnly;
end;

function TDriverDataSetDBExpress.IsUniDirectional: Boolean;
begin
  Result := FDataSet.IsUniDirectional;
end;

procedure TDriverDataSetDBExpress.Open;
begin
  try
    inherited Open;
  finally
    _SetMonitorLog(FDataSet.CommandText, 'DEFAULT', FDataSet.Params);
  end;
end;

function TDriverDataSetDBExpress.RowsAffected: UInt32;
begin
  Result := FSQLQuery.RowsAffected;
end;

function TDriverDataSetDBExpress._GetCommandText: String;
begin
  Result := FDataSet.CommandText;
end;

procedure TDriverDataSetDBExpress._SetCachedUpdates(const Value: Boolean);
begin
end;

procedure TDriverDataSetDBExpress._SetCommandText(const ACommandText: String);
begin
  FDataSet.CommandText := ACommandText;
end;

procedure TDriverDataSetDBExpress._SetReadOnly(const Value: Boolean);
begin
  FDataSet.ReadOnly := Value;
end;

procedure TDriverDataSetDBExpress._SetUniDirectional(const Value: Boolean);
begin
end;

function TDriverDataSetDBExpress._Iso8601ToDateTime(const AValue: String): TDateTime;
var
  Y, M, D, HH, MI, SS: Cardinal;
begin
  Result := StrToDateTimeDef(AValue, 0);
  case Length(AValue) of
    9:
      if (AValue[1] = 'T') and (AValue[4] = ':') and (AValue[7] = ':') then
      begin
        HH := Ord(AValue[2]) * 10 + Ord(AValue[3]) - (48 + 480);
        MI := Ord(AValue[5]) * 10 + Ord(AValue[6]) - (48 + 480);
        SS := Ord(AValue[8]) * 10 + Ord(AValue[9]) - (48 + 480);
        if (HH < 24) and (MI < 60) and (SS < 60) then
          Result := EncodeTime(HH, MI, SS, 0);
      end;
    10:
      if (AValue[5] = AValue[8]) and (Ord(AValue[8]) in [Ord('-'), Ord('/')]) then
      begin
        Y := Ord(AValue[1]) * 1000 + Ord(AValue[2]) * 100 + Ord(AValue[3]) * 10 + Ord(AValue[4]) - (48 + 480 + 4800 + 48000);
        M := Ord(AValue[6]) * 10 + Ord(AValue[7]) - (48 + 480);
        D := Ord(AValue[9]) * 10 + Ord(AValue[10]) - (48 + 480);
        if (Y <= 9999) and ((M - 1) < 12) and ((D - 1) < 31) then
          Result := EncodeDate(Y, M, D);
      end;
    19,24:
      if (AValue[5] = AValue[8]) and
         (Ord(AValue[8]) in [Ord('-'), Ord('/')]) and
         (Ord(AValue[11]) in [Ord(' '), Ord('T')]) and
         (AValue[14] = ':') and
         (AValue[17] = ':') then
      begin
        Y := Ord(AValue[1]) * 1000 + Ord(AValue[2]) * 100 + Ord(AValue[3]) * 10 + Ord(AValue[4]) - (48 + 480 + 4800 + 48000);
        M := Ord(AValue[6]) * 10 + Ord(AValue[7]) - (48 + 480);
        D := Ord(AValue[9]) * 10 + Ord(AValue[10]) - (48 + 480);
        HH := Ord(AValue[12]) * 10 + Ord(AValue[13]) - (48 + 480);
        MI := Ord(AValue[15]) * 10 + Ord(AValue[16]) - (48 + 480);
        SS := Ord(AValue[18]) * 10 + Ord(AValue[19]) - (48 + 480);
        if (Y <= 9999) and ((M - 1) < 12) and ((D - 1) < 31) and (HH < 24) and (MI < 60) and (SS < 60) then
          Result := EncodeDate(Y, M, D) + EncodeTime(HH, MI, SS, 0);
      end;
  end;
end;

end.
