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

unit DataEngine.Consts;

interface

const
  ABSTRACT_METHOD_ERROR = 'Abstract method "%s" called in %s. ' +
                          'Derived classes must override this method to provide a concrete implementation.';

  DEFAULT_RETRY_COUNT = 3;
  DEFAULT_RETRY_DELAY = 500;
  DEFAULT_SLOW_QUERY_THRESHOLD = 1000;

implementation

end.

