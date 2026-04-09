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

unit DataEngine.Observability;

interface

uses
  SysUtils,
  Classes,
  DataEngine.FactoryInterfaces;

type
  TMetricCollector = class(TInterfacedObject, IDBObserver)
  protected
    procedure OnNotify(const AParam: TMonitorParam); virtual;
  end;

  TConsoleObserver = class(TMetricCollector)
  protected
    procedure OnNotify(const AParam: TMonitorParam); override;
  end;

implementation

{ TMetricCollector }

procedure TMetricCollector.OnNotify(const AParam: TMonitorParam);
begin
  // Base implementation: do nothing or aggregate basic stats in derived classes
end;

{ TConsoleObserver }

procedure TConsoleObserver.OnNotify(const AParam: TMonitorParam);
begin
  case AParam.EventType of
    teQueryStart: Writeln('[SQL START] ' + AParam.Command);
    teQueryEnd: Writeln(Format('[SQL END] %d ms - %s', [AParam.ExecutionTime, AParam.Command]));
    teFetchEnd: Writeln(Format('[FETCH END] %d ms - %d rows fetched - %s', [AParam.FetchTime, AParam.RowsAffected, AParam.Command]));
    teError: Writeln('[SQL ERROR] ' + AParam.Exception.Message);
    teMetric: Writeln('[METRIC] ' + AParam.Command);
  end;
end;

end.
