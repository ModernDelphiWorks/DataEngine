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

unit DriverIBObjectsTransaction;

interface

uses
  Classes,
  DB,
  IBODataset,
  // DBE
  DBE.DriverConnection,
  DBE.FactoryInterfaces;

type
  /// <summary>
  /// Classe de conexão concreta com IBObjects
  /// </summary>
  TDriverIBObjectsTransaction = class(TDriverTransaction)
  protected
    FConnection: TIBODatabase;
  public
    constructor Create(AConnection: TComponent); override;
    destructor Destroy; override;
    procedure StartTransaction; override;
    procedure Commit; override;
    procedure Rollback; override;
    function InTransaction: Boolean; override;
  end;

implementation

{ TDriverIBObjectsTransaction }

constructor TDriverIBObjectsTransaction.Create(AConnection: TComponent);
begin
  FConnection := AConnection as TIBODatabase;
end;

destructor TDriverIBObjectsTransaction.Destroy;
begin
  FConnection := nil;
  inherited;
end;

function TDriverIBObjectsTransaction.InTransaction: Boolean;
begin
  Result := FConnection.DefaultTransaction.InTransaction;
end;

procedure TDriverIBObjectsTransaction.StartTransaction;
begin
  inherited;
  FConnection.Connected := True;

  if not FConnection.DefaultTransaction.InTransaction  then
    FConnection.DefaultTransaction.StartTransaction;
end;

procedure TDriverIBObjectsTransaction.Commit;
begin
  inherited;
  FConnection.DefaultTransaction.Commit;
end;

procedure TDriverIBObjectsTransaction.Rollback;
begin
  inherited;
  FConnection.DefaultTransaction.Rollback;
end;

end.
