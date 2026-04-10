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

unit DataEngine.Migration.Interfaces;

interface

uses
  DataEngine.FactoryInterfaces;

type
  IDBMigration = interface
    ['{7B6E4A8C-2D1B-4C9D-8E0A-1F2B3C4D5E6F}']
    function GetVersion: Int64;
    function GetName: string;
    procedure Up(const AConnection: IDBConnection);
    procedure Down(const AConnection: IDBConnection);
  end;

  IDBMigrationEngine = interface
    ['{8C7F5B9D-3E2C-5D0E-9F1B-2A3C4D5E6F7F}']
    procedure RunMigrations;
    procedure RegisterMigration(const AMigration: IDBMigration);
    function GetAppliedMigrations: TArray<Int64>;
  end;

implementation

end.
