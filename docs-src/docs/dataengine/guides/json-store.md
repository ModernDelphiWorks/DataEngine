---
displayed_sidebar: dataengineSidebar
title: JSON Store Driver (NoSQL)
---

O **JSON Store Driver** é uma implementação nativa do **DataEngine** projetada para persistência local baseada em arquivos JSON (NoSQL), sem a necessidade de engines de banco de dados externos como FireDAC ou UniDAC.

Ideal para configurações locais, cache simples ou pequenas persistências de dados que não justificam a complexidade de um banco de dados relacional.

## Características

- **Zero Dependency:** Utiliza apenas as bibliotecas nativas do Delphi (`System.JSON`).
- **Atomic Write:** Implementa o *Safe Write Pattern* via arquivos temporários para evitar corrupção de dados.
- **In-Memory Performance:** Carrega o conteúdo JSON integralmente em memória no momento da conexão para consultas rápidas.
- **NoSQL:** Persistência baseada em um array de objetos JSON, oferecendo flexibilidade de esquema.

## Como utilizar

### 1. Inicialização

Para utilizar o driver JSON, basta informar o caminho físico do arquivo `.json` na factory dedicada:

```delphi
uses
  DataEngine.FactoryJSON,
  DataEngine.FactoryInterfaces;

var
  LDB: IDBConnection;
begin
  // Cria a conexão baseada no arquivo 'data.json'
  LDB := TFactoryJSON.Create('C:\MeusDados\data.json');
  LDB.Connect;
end;
```

### 2. Operações DML (Inserção)

O driver intercepta comandos básicos de inserção para persistir novos objetos no arquivo:

```delphi
var
  LParams: TParams;
begin
  LParams := TParams.Create(nil);
  try
    with LParams.Add as TParam do
    begin
      Name := 'ID';
      Value := 1;
    end;
    with LParams.Add as TParam do
    begin
      Name := 'Nome';
      Value := 'Delphi User';
    end;
    
    // Executa a persistência atômica no arquivo JSON
    LDB.ExecuteDirect('INSERT', LParams);
  finally
    LParams.Free;
  end;
end;
```

### 3. Consultas (Dataset)

Para carregar os dados, você pode utilizar o método `CreateDataSet`:

```delphi
var
  LDataSet: IDBDataSet;
begin
  LDataSet := LDB.CreateDataSet('SELECT');
  LDataSet.Open;
  
  while not LDataSet.Eof do
  begin
    ShowMessage(Format('ID: %s, Nome: %s', [
      LDataSet.FieldByName('ID').AsString,
      LDataSet.FieldByName('Nome').AsString
    ]));
    LDataSet.Next;
  end;
end;
```

## Limitações conhecidas

- **Escalabilidade:** Como o driver carrega todo o arquivo em memória, ele não é recomendado para volumes de dados superiores a 50MB.
- **SQL Parsing:** O suporte a SQL é limitado à identificação de operações básicas (`INSERT`, `SELECT`). Não há suporte para joins complexos ou cláusulas `WHERE` via SQL parser nativo neste driver NoSQL.
