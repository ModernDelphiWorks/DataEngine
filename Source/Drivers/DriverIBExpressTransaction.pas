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

unit DriverIBExpressTransaction;

interface

uses
  Classes,
  DB,
  IBDatabase,
  // DBE
  DBE.DriverConnection,
  DBE.FactoryInterfaces;

type
  // Classe de conexão concreta com dbExpress
  TDriverIBExpressTransaction = class(TDriverTransaction)
  protected
    FConnection: TIBDatabase;
    FTransaction: TIBTransaction;
  public
    constructor Create(AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverIBExpressTransaction }

constructor TDriverIBExpressTransaction.Create(AConnection: TComponent);
begin
  FConnection := AConnection as TIBDatabase;
  FTransaction := TIBTransaction.Create(nil);
  FTransaction.DefaultDatabase := FConnection;
  FTransaction.AutoStopAction := saCommit;
  FConnection.DefaultTransaction := FTransaction;
end;

destructor TDriverIBExpressTransaction.Destroy;
begin
  FTransaction.Free;
  FConnection := nil;
  inherited;
end;

function TDriverIBExpressTransaction.InTransaction: Boolean;
begin
  Result := FTransaction.InTransaction;
end;

procedure TDriverIBExpressTransaction.StartTransaction;
begin
  inherited;
  FConnection.Connected := True;

  if not FTransaction.InTransaction then
    FTransaction.StartTransaction;
end;

procedure TDriverIBExpressTransaction.Commit;
begin
  inherited;
  FTransaction.Commit;
end;

procedure TDriverIBExpressTransaction.Rollback;
begin
  inherited;
  FTransaction.Rollback;
end;

end.
