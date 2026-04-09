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

program Tests.DriverJSON;

{$APPTYPE CONSOLE}

uses
  SysUtils,
  Classes,
  DB,
  System.IOUtils,
  DataEngine.FactoryInterfaces,
  DataEngine.FactoryJSON;

var
  LDB: IDBConnection;
  LQuery: IDBQuery;
  LDataSet: IDBDataSet;
  LParams: TParams;
  LFileName: string;

begin
  try
    LFileName := 'test_data.json';
    if TFile.Exists(LFileName) then
      TFile.Delete(LFileName);

    WriteLn('--- Testing JSON Store Driver ---');
    
    // 1. Setup Connection
    LDB := TFactoryJSON.Create(LFileName);
    LDB.Connect;
    WriteLn('Connected to ' + LFileName);

    // 2. Insert Data
    WriteLn('Inserting records...');
    LParams := TParams.Create(nil);
    try
      with LParams.Add as TParam do
      begin
        Name := 'ID';
        Value := 1;
      end;
      with LParams.Add as TParam do
      begin
        Name := 'Name';
        Value := 'Delphi User';
      end;
      LDB.ExecuteDirect('INSERT', LParams);
    finally
      LParams.Free;
    end;

    LParams := TParams.Create(nil);
    try
      with LParams.Add as TParam do
      begin
        Name := 'ID';
        Value := 2;
      end;
      with LParams.Add as TParam do
      begin
        Name := 'Name';
        Value := 'DataEngine Agent';
      end;
      LDB.ExecuteDirect('INSERT', LParams);
    finally
      LParams.Free;
    end;

    // 3. Select Data
    WriteLn('Selecting records...');
    LDataSet := LDB.CreateDataSet('SELECT');
    LDataSet.Open;
    
    WriteLn('RecordCount: ' + IntToStr(LDataSet.RecordCount));
    
    while not LDataSet.Eof do
    begin
      WriteLn(Format('ID: %s, Name: %s', [
        LDataSet.FieldByName('ID').AsString,
        LDataSet.FieldByName('Name').AsString
      ]));
      LDataSet.Next;
    end;
    LDataSet.Close;

    // 4. Persistence Check
    LDB.Disconnect;
    WriteLn('Disconnected.');
    
    if TFile.Exists(LFileName) then
      WriteLn(Format('File persisted successfully. Size: %d bytes', [TFile.GetSize(LFileName)]))
    else
      WriteLn('Error: File not found!');

    WriteLn('--- Test Finished Successfully ---');
    
  except
    on E: Exception do
      WriteLn(E.ClassName, ': ', E.Message);
  end;
end.

