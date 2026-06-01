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

unit DataEngine.CacheTypes;

interface

uses
  Classes,
  SysUtils;

type
  TCacheResultStatus = (csHit, csMiss, csExpired, csError);

  TCacheOptions = record
    Enabled: Boolean;
    TTLMinutes: Integer;
    AutoRefresh: Boolean;
    MaxInMemoryResults: Integer;
    class function Default: TCacheOptions; static;
  end;

  TRedisConfig = record
    Host: string;
    Port: Integer;
    Password: string;
    DBIndex: Integer;
    class function Default: TRedisConfig; static;
  end;

  TCacheEntry = record
    Key: string;
    Timestamp: TDateTime;
    TTL: Integer;
    function IsValid: Boolean;
  end;

implementation

{ TCacheOptions }

class function TCacheOptions.Default: TCacheOptions;
begin
  Result.Enabled := True;
  Result.TTLMinutes := 30;
  Result.AutoRefresh := False;
  Result.MaxInMemoryResults := 1000;
end;

{ TRedisConfig }

class function TRedisConfig.Default: TRedisConfig;
begin
  Result.Host := 'localhost';
  Result.Port := 6379;
  Result.Password := '';
  Result.DBIndex := 0;
end;

{ TCacheEntry }

function TCacheEntry.IsValid: Boolean;
begin
  Result := (Timestamp + (TTL / (24 * 60))) > Now;
end;

end.
