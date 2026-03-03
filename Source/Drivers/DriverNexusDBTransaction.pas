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

unit DriverNexusDBTransaction;

interface

uses
  Classes,
  DB,
  nxdb,
  // DBE
  DBE.DriverConnection,
  DBE.FactoryInterfaces;

type
  // Classe de conexão concreta com dbExpress
  TDriverNexusDBTransaction = class(TDriverTransaction)
  protected
    FConnection: TnxDatabase;
  public
    constructor Create(AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverNexusDBTransaction }

constructor TDriverNexusDBTransaction.Create(AConnection: TComponent);
begin
  FConnection := AConnection as TnxDatabase;
end;

destructor TDriverNexusDBTransaction.Destroy;
begin
  FConnection := nil;
  inherited;
end;

function TDriverNexusDBTransaction.InTransaction: Boolean;
begin
  Result := FConnection.InTransaction;
end;

procedure TDriverNexusDBTransaction.StartTransaction;
begin
  inherited;
  FConnection.StartTransaction;
end;

procedure TDriverNexusDBTransaction.Commit;
begin
  inherited;
  FConnection.Commit;
end;

procedure TDriverNexusDBTransaction.Rollback;
begin
  inherited;
  FConnection.Rollback;
end;

end.
