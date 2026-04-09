---
displayed_sidebar: dataengineSidebar
title: Operações em Massa (Bulk)
---

# Operações em Massa (Bulk Operations)

O **DataEngine** fornece suporte nativo a operações em massa de carregamento de dados via `IDBBulkLoader`. Esta funcionalidade é otimizada para alto desempenho, utilizando recursos específicos de cada driver como o **Array DML** do FireDAC e o **TUniLoader** do UniDAC.

## Quando usar

- Importação e migração de grandes volumes de dados.
- Sincronização offline-to-online.
- Processamento em lote de log ou telemetria.

> **Performance:** Inserir 10.000 registros via `BulkLoader` pode ser até **100x mais rápido** que executar 10.000 comandos de `INSERT` individuais.

## Exemplo de Uso

```delphi
uses
  DataEngine.FactoryInterfaces;

var
  FBulk: IDBBulkLoader;
  LIndex: Integer;
begin
  FBulk := FConn.BulkLoader;
  FBulk.TableName := 'CLIENTE';
  FBulk.BatchSize := 1000;
  
  // Define os parâmetros (colunas) que serão carregados
  FBulk.ParamByName('CLIENT_ID').DataType := ftInteger;
  FBulk.ParamByName('NOME').DataType := ftString;
  
  FBulk.Prepare;

  // Carrega o buffer em memória (push-based)
  for LIndex := 0 to 999 do
  begin
    FBulk.SetValue('CLIENT_ID', LIndex, LIndex + 1);
    FBulk.SetValue('NOME', LIndex, 'Cliente ' + IntToStr(LIndex + 1));
  end;

  // Executa o lote sob transação
  FConn.StartTransaction;
  try
    FBulk.Execute(1000);
    FConn.Commit;
  except
    FConn.Rollback;
    raise;
  end;
end;
```

## Suporte por Driver

| Driver | Motor Utilizado | Observações |
|--------|-----------------|-------------|
| **FireDAC** | `Array DML` | Utiliza o buffer de memória nativo de parâmetros de alto desempenho (`TFDQuery.Execute`). |
| **UniDAC** | `TUniLoader` | Utiliza o motor de carregamento rápido (`OnPutData`). |
| **ZeosLib** | `Batch Loop` | Executa o lote em loop dentro do contexto transacional (fallback de performance). |

## Melhores Práticas

1. **Uso de Transações:** Sempre use transações explícitas ao redor do método `Execute` para garantir integridade e manter a performance em drivers como o Zeos.
2. **Batch Size:** Encontre o tamanho de lote ideal para o seu driver e memória disponível. Lotes entre `1.000` e `5.000` registros são recomendados para a maioria dos cenários.
3. **Limpeza de Buffer:** Após um `Execute`, chame `Clear` se desejar reutilizar o Loader com uma configuração de parâmetros diferente.
