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

unit DataEngine.RedisTransport;

{$IFDEF DATAENGINE_REDIS}

interface

uses
  SysUtils,
  Classes,
  System.Net.Sockets;

type
  ERedisError = class(Exception);

  TRedisResponse = record
  public
    ValueType: Char;
    StringValue: string;
    IntegerValue: Int64;
    ArrayValue: TArray<string>;
    RawValue: TBytes;
  end;

  TRedisTransport = class
  private
    FSocket: TSocket;
    FHost: string;
    FPort: Integer;
    FPassword: string;
    FDBIndex: Integer;

    procedure _Write(const AData: string); overload;
    procedure _Write(const AData: TBytes); overload;
    function _ReadLine: string;
    function _Read(ACount: Integer): TBytes;
    function _ParseResponse: TRedisResponse;
  public
    constructor Create(const AHost: string = 'localhost'; APort: Integer = 6379);
    destructor Destroy; override;

    procedure Connect;
    procedure Disconnect;
    function IsConnected: Boolean;

    function ExecuteCommand(const ACommand: string; const AArgs: array of string): TRedisResponse; overload;
    function ExecuteCommand(const ACommand: string; const AArgs: TArray<TBytes>): TRedisResponse; overload;

    property Host: string read FHost write FHost;
    property Port: Integer read FPort write FPort;
    property Password: string read FPassword write FPassword;
    property DBIndex: Integer read FDBIndex write FDBIndex;
  end;

implementation

{ TRedisTransport }

constructor TRedisTransport.Create(const AHost: string; APort: Integer);
begin
  inherited Create;
  FHost := AHost;
  FPort := APort;
  FDBIndex := 0;
end;

destructor TRedisTransport.Destroy;
begin
  Disconnect;
  inherited;
end;

procedure TRedisTransport.Connect;
begin
  if IsConnected then
    Exit;

  FSocket := TSocket.Create(TSocketType.stStream, TProtocolType.ptTCP);
  try
    FSocket.Connect('', FHost, '', FPort);
    if Password <> '' then
      ExecuteCommand('AUTH', [Password]);

    if DBIndex <> 0 then
      ExecuteCommand('SELECT', [IntToStr(DBIndex)]);
  except
    on E: Exception do
    begin
      FreeAndNil(FSocket);
      raise ERedisError.Create('Could not connect to Redis: ' + E.Message);
    end;
  end;
end;

procedure TRedisTransport.Disconnect;
begin
  if Assigned(FSocket) then
  begin
    FSocket.Close;
    FreeAndNil(FSocket);
  end;
end;

function TRedisTransport.IsConnected: Boolean;
begin
  Result := Assigned(FSocket) and (FSocket.State = TSocketState.Connected);
end;

function TRedisTransport.ExecuteCommand(const ACommand: string; const AArgs: array of string): TRedisResponse;
var
  LArgs: TArray<TBytes>;
  LIndex: Integer;
begin
  SetLength(LArgs, Length(AArgs));
  for LIndex := 0 to High(AArgs) do
    LArgs[LIndex] := TEncoding.UTF8.GetBytes(AArgs[LIndex]);
  Result := ExecuteCommand(ACommand, LArgs);
end;

function TRedisTransport.ExecuteCommand(const ACommand: string; const AArgs: TArray<TBytes>): TRedisResponse;
var
  LRESP: string;
  LIndex: Integer;
begin
  Connect;

  // Build RESP array
  LRESP := '*' + IntToStr(Length(AArgs) + 1) + #13#10;
  LRESP := LRESP + '$' + IntToStr(Length(ACommand)) + #13#10 + ACommand + #13#10;
  _Write(LRESP);

  for LIndex := 0 to High(AArgs) do
  begin
    LRESP := '$' + IntToStr(Length(AArgs[LIndex])) + #13#10;
    _Write(LRESP);
    _Write(AArgs[LIndex]);
    _Write(#13#10);
  end;

  Result := _ParseResponse;
end;

procedure TRedisTransport._Write(const AData: string);
begin
  _Write(TEncoding.UTF8.GetBytes(AData));
end;

procedure TRedisTransport._Write(const AData: TBytes);
begin
  if Length(AData) > 0 then
    FSocket.Send(AData, 0, Length(AData));
end;

function TRedisTransport._ReadLine: string;
var
  LByte: Byte;
  LBytes: TBytes;
  LCount: Integer;
begin
  Result := '';
  LCount := 0;
  SetLength(LBytes, 1024);
  while True do
  begin
    if FSocket.Receive(@LByte, 1) <= 0 then
      Break;

    if LByte = 13 then
    begin
      FSocket.Receive(@LByte, 1); // skip \n
      Break;
    end;

    if LCount >= Length(LBytes) then
      SetLength(LBytes, LCount * 2);

    LBytes[LCount] := LByte;
    Inc(LCount);
  end;
  if LCount > 0 then
    Result := TEncoding.UTF8.GetString(LBytes, 0, LCount);
end;

function TRedisTransport._Read(ACount: Integer): TBytes;
var
  LReceived: Integer;
begin
  SetLength(Result, ACount);
  LReceived := 0;
  while LReceived < ACount do
    LReceived := LReceived + FSocket.Receive(Result[LReceived], ACount - LReceived);
end;

function TRedisTransport._ParseResponse: TRedisResponse;
var
  LFirst: Byte;
  LHeader: string;
  ILen: Integer;
  LIndex: Integer;
  LRet: TRedisResponse;
begin
  if FSocket.Receive(@LFirst, 1) <= 0 then
    raise ERedisError.Create('No response from Redis');

  Result.ValueType := Char(LFirst);
  case Result.ValueType of
    '+': // Simple String
      Result.StringValue := _ReadLine;
    '-': // Error
      raise ERedisError.Create(_ReadLine);
    ':': // Integer
      Result.IntegerValue := StrToInt64(_ReadLine);
    '$': // Bulk String
      begin
        ILen := StrToInt(_ReadLine);
        if ILen = -1 then
        begin
          Result.RawValue := nil;
          Result.StringValue := '';
        end
        else
        begin
          Result.RawValue := _Read(ILen);
          _ReadLine; // skip \r\n
          Result.StringValue := TEncoding.UTF8.GetString(Result.RawValue);
        end;
      end;
    '*': // Array
      begin
        ILen := StrToInt(_ReadLine);
        if ILen = -1 then
          Result.ArrayValue := nil
        else
        begin
          SetLength(Result.ArrayValue, ILen);
          for LIndex := 0 to ILen - 1 do
          begin
            LRet := _ParseResponse;
            Result.ArrayValue[LIndex] := LRet.StringValue;
          end;
        end;
      end;
  else
    raise ERedisError.Create('Unexpected Redis response type: ' + Result.ValueType);
  end;
end;

{$ELSE}

interface

implementation

{$ENDIF}

end.
