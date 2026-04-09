program TestsMetadataSerializer;

{$APPTYPE CONSOLE}

uses
  System.SysUtils,
  System.Classes,
  Data.DB,
  FireDAC.Comp.Client,
  DataEngine.FactoryInterfaces in '..\Source\Core\DataEngine.FactoryInterfaces.pas',
  DataEngine.Metadata.Serializer in '..\Source\Core\DataEngine.Metadata.Serializer.pas';

procedure RunSerializerTest;
var
  LMemTable: TFDMemTable;
  LJSON: string;
  LRestoredMemTable: TFDMemTable;
  I: Integer;
begin
  Writeln('--- DataEngine Metadata Serializer Test ---');
  
  // 1. Create a Table with various types
  LMemTable := TFDMemTable.Create(nil);
  try
    LMemTable.FieldDefs.Add('ID', ftInteger, 0, True);
    LMemTable.FieldDefs.Add('Name', ftString, 100);
    LMemTable.FieldDefs.Add('Price', ftCurrency);
    LMemTable.FieldDefs.Add('CreatedAt', ftDateTime);
    LMemTable.FieldDefs.Add('Active', ftBoolean);
    
    Writeln('[STEP 1] Serializing original FieldDefs to JSON...');
    LJSON := TMetadataSerializer.ToJSON(LMemTable.FieldDefs);
    Writeln('[JSON] ' + LJSON);

    // 2. Restore into a new MemTable
    LRestoredMemTable := TFDMemTable.Create(nil);
    try
      Writeln('[STEP 2] Deserializing JSON into new FieldDefs...');
      TMetadataSerializer.FromJSON(LJSON, LRestoredMemTable.FieldDefs);
      
      // 3. Validation
      Writeln('[STEP 3] Validating results...');
      if LRestoredMemTable.FieldDefs.Count <> LMemTable.FieldDefs.Count then
        raise Exception.Create('Count mismatch: Expected ' + IntToStr(LMemTable.FieldDefs.Count) + ' but found ' + IntToStr(LRestoredMemTable.FieldDefs.Count));
        
      for I := 0 to LMemTable.FieldDefs.Count - 1 do
      begin
        Writeln('Validating Field [' + IntToStr(I) + ']: ' + LRestoredMemTable.FieldDefs[I].Name);
        if LRestoredMemTable.FieldDefs[I].Name <> LMemTable.FieldDefs[I].Name then
          raise Exception.Create('Name mismatch at index ' + IntToStr(I));
        if LRestoredMemTable.FieldDefs[I].DataType <> LMemTable.FieldDefs[I].DataType then
          raise Exception.Create('DataType mismatch at index ' + IntToStr(I) + ' (' + LMemTable.FieldDefs[I].Name + ')');
        if LRestoredMemTable.FieldDefs[I].Size <> LMemTable.FieldDefs[I].Size then
          raise Exception.Create('Size mismatch at index ' + IntToStr(I));
      end;
      
      Writeln('[SUCCESS] Metadata Serialization validated successfully!');
    finally
      LRestoredMemTable.Free;
    end;
  finally
    LMemTable.Free;
  end;
end;

begin
  try
    RunSerializerTest;
  except
    on E: Exception do
    begin
      Writeln('ERROR (' + E.ClassName + '): ' + E.Message);
      System.ExitCode := 1;
    end;
  end;
  Writeln('DONE.');
end.
