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

unit DataEngine.AsyncRunner;

interface

uses
  System.SysUtils,
  System.Threading,
  System.Classes;

type
  /// <summary>
  /// Basic orchestrator for asynchronous execution in DataEngine using PPL Tasks.
  /// </summary>
  TDataEngineAsync = class
  public
    /// <summary>
    /// Runs a heavy task in the background and notifies completion/error in the Main Thread.
    /// </summary>
    class procedure Run(const ATask: TProc; const AOnComplete: TProc; const AOnError: TProc<Exception> = nil);
  end;

implementation

{ TDataEngineAsync }

class procedure TDataEngineAsync.Run(const ATask, AOnComplete: TProc; const AOnError: TProc<Exception>);
begin
  TTask.Run(
    procedure
    begin
      try
        ATask();
        
        if Assigned(AOnComplete) then
          TThread.Queue(nil,
            procedure
            begin
              AOnComplete();
            end);
      except
        on E: Exception do
        begin
          if Assigned(AOnError) then
          begin
            var LError := E.Message;
            TThread.Queue(nil,
              procedure
              begin
                // Re-raise or pass as exception
                AOnError(Exception.Create(LError));
              end);
          end;
        end;
      end;
    end);
end;

end.
