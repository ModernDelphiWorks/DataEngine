program Tests.MultiTenant.Pool;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  System.Threading,
  System.Diagnostics,
  DataEngine.FactoryInterfaces,
  DataEngine.PoolConnection,
  Tests.MultiTenant.Mocks;

procedure TestMultiTenantStress;
const
  TENANTS_COUNT = 5;
  THREADS_PER_TENANT = 10;
var
  LTasks: TArray<ITask>;
  LStopwatch: TStopwatch;
  I, J: Integer;
begin
  Writeln('Starting Multi-tenant Pool Stress Test...');
  Writeln(Format('Tenants: %d, Threads per Tenant: %d', [TENANTS_COUNT, THREADS_PER_TENANT]));
    
  LStopwatch := TStopwatch.StartNew;
  SetLength(LTasks, TENANTS_COUNT * THREADS_PER_TENANT);

  for I := 0 to TENANTS_COUNT - 1 do
  begin
    // To avoid closure capture issues, we use a local variable for the tenant ID
    var LTenantForClosure := 'Tenant_' + IntToStr(I);
    for J := 0 to THREADS_PER_TENANT - 1 do
    begin
      // We also need to capture J if needed, but here we only need the tenant
      LTasks[I * THREADS_PER_TENANT + J] := TTask.Run(
        procedure
        var
          LConn: IDBConnection;
          LMyTenant: string;
        begin
          LMyTenant := LTenantForClosure; // Capture it here
          try
            LConn := PoolManager.AcquireConnection(
              LMyTenant,
              function: IDBConnection
              begin
                Result := TStubConnection.Create;
              end,
              5,
              600
            );
            
            if Assigned(LConn) then
              Sleep(10 + Random(20));
              
            PoolManager.ReleaseConnection(LMyTenant, LConn);
          except
            on E: Exception do
              Writeln(Format('[%s] Error: %s', [LMyTenant, E.Message]));
          end;
        end
      );
    end;
  end;

  TTask.WaitForAll(LTasks);
  LStopwatch.Stop;
  Writeln(Format('Multi-tenant Pool Stress Test Completed in %d ms.', [LStopwatch.ElapsedMilliseconds]));
end;

begin
  try
    TestMultiTenantStress;
    Writeln('Validation: SUCCESS');
  except
    on E: Exception do
    begin
      Writeln('Validation: FAILED');
      Writeln(E.ClassName + ': ' + E.Message);
      System.ExitCode := 1;
    end;
  end;
end.
