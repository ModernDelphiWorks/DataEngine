program TestResiliency;

{$IFDEF CONSOLE_TESTRUNNER}
{$APPTYPE CONSOLE}
{$ENDIF}

uses
  System.SysUtils,
  DUnitX.TestFramework,
  DUnitX.Loggers.Console,
  Tests.Resiliency in 'Tests.Resiliency.pas',
  DataEngine.GuardConnection in '..\Source\Core\DataEngine.GuardConnection.pas',
  DataEngine.PoolConnection in '..\Source\Core\DataEngine.PoolConnection.pas',
  DataEngine.DriverConnection in '..\Source\Core\DataEngine.DriverConnection.pas',
  DataEngine.FactoryInterfaces in '..\Source\Core\DataEngine.FactoryInterfaces.pas',
  DataEngine.Consts in '..\Source\Core\DataEngine.Consts.pas';

var
  runner: ITestRunner;
  results: IRunResults;
  logger: ITestLogger;
begin
  System.Writeln('Resiliency Test Suite Starting...');
  try
    runner := TDUnitX.CreateRunner;
    // runner.UseMainThread := True;
    logger := TDUnitXConsoleLogger.Create(True);
    runner.AddLogger(logger);

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
end.
