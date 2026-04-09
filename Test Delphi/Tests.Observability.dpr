program Tests.Observability;

{$IFNDEF TESTINSIGHT}
{$APPTYPE CONSOLE}
{$ENDIF}
{$STRONGLINKTYPES ON}

uses
  SysUtils,
  {$IFDEF TESTINSIGHT}
  TestInsight.DUnitX,
  {$ELSE}
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  {$ENDIF }
  DUnitX.TestFramework,
  Tests.Observability.Main in 'Tests.Observability.Main.pas';

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
  nunitLogger : ITestLogger;
begin
{$IFDEF TESTINSIGHT}
  TestInsight.DUnitX.RunRegisteredTests;
{$ELSE}
  try
    try
      runner := TDUnitX.CreateRunner;
      runner.UseRTTI := True;
      logger := TDUnitXConsoleLogger.Create(true);
      runner.AddLogger(logger);
      nunitLogger := TDUnitXXMLNUnitFileLogger.Create(TDUnitX.Options.XMLOutputFile);
      runner.AddLogger(nunitLogger);

      results := runner.Execute;
      if not results.AllPassed then
        System.ExitCode := EXIT_ERRORS;

      {$IFNDEF CI}
      if TDUnitX.Options.ExitBehavior = TDUnitXExitBehavior.Pause then
      begin
        System.Write('Done.. press <Enter> key to quit.');
        System.Readln;
      end;
      {$ENDIF}
    except
      on E: Exception do
        System.Writeln(E.ClassName, ': ', E.Message);
    end;
  finally
  end;
{$ENDIF}
end.
