{
  DBE Brasil é um Engine de Conexão simples e descomplicado for Delphi/Lazarus

                   Copyright (c) 2016, Isaque Pinheiro
                          All rights reserved.

                    GNU Lesser General Public License
                      Versão 3, 29 de junho de 2007

       Copyright (C) 2007 Free Software Foundation, Inc. <http://fsf.org/>
       A todos é permitido copiar e distribuir cópias deste documento de
       licença, mas mudá-lo não é permitido.

       Esta versão da GNU Lesser General Public License incorpora
       os termos e condições da versão 3 da GNU General Public License
       Licença, complementado pelas permissões adicionais listadas no
       arquivo LICENSE na pasta principal.
}

{ @abstract(DBE Framework)
  @created(20 Jul 2016)
  @author(Isaque Pinheiro <https://www.isaquepinheiro.com.br>)
}

unit FactorySQLite3;

interface

uses
  DB,
  Classes,
  SysUtils,
  SQLiteTable3,
  // DBE
  DBE.FactoryConnection,
  DBE.FactoryInterfaces;

type
  // Fábrica de conexão concreta com dbExpress
  TFactorySQLite = class(TFactoryConnection)
  public
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverName: TDriverName); overload;
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverName: TDriverName;
      const AMonitor: ICommandMonitor); overload;
    constructor Create(const AConnection: TSQLiteDatabase;
      const ADriverName: TDriverName;
      const AMonitorCallback: TMonitorProc); overload;
    destructor Destroy; override;
    procedure AddTransaction(const AKey: String; const ATransaction: TComponent); override;
  end;

implementation

uses
  dbe.driver.sqlite3,
  dbe.driver.sqlite3.transaction;

{ TFactorySQLite }

constructor TFactorySQLite.Create(const AConnection: TSQLiteDatabase;
  const ADriverName: TDriverName);
begin
  FDriverTransaction := TDriverSQLite3Transaction.Create(AConnection);
  FDriverConnection  := TDriverSQLite3.Create(AConnection,
                                              FDriverTransaction,
                                              ADriverName,
                                              FCommandMonitor,
                                              FMonitorCallback);
  FAutoTransaction := False;
end;

constructor TFactorySQLite.Create(const AConnection: TSQLiteDatabase;
  const ADriverName: TDriverName; const AMonitor: ICommandMonitor);
begin
  Create(AConnection, ADriverName);
  FCommandMonitor := AMonitor;
end;

procedure TFactorySQLite.AddTransaction(const AKey: String;
  const ATransaction: TComponent);
begin
  if not (ATransaction is TSQLiteDatabase) then
    raise Exception.Create('Invalid transaction type. Expected TSQLiteDatabase.');

  inherited AddTransaction(AKey, ATransaction);
end;

constructor TFactorySQLite.Create(const AConnection: TSQLiteDatabase;
  const ADriverName: TDriverName; const AMonitorCallback: TMonitorProc);
begin
  Create(AConnection, ADriverName);
  FMonitorCallback := AMonitorCallback;
end;

destructor TFactorySQLite.Destroy;
begin
  FDriverConnection.Free;
  FDriverTransaction.Free;
  inherited;
end;

end.
