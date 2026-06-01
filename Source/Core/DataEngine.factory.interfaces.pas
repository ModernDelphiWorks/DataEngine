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

{
  DataEngine — backward-compatibility shim.
  Re-exports DataEngine.FactoryInterfaces under the legacy dotted-lowercase name
  so consumers that still reference 'DataEngine.factory.interfaces' compile
  without source changes.
}
unit DataEngine.factory.interfaces;

interface

uses
  DataEngine.FactoryInterfaces;

implementation

end.

