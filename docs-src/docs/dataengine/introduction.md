---
displayed_sidebar: dataengineSidebar
title: Introdução
---

O **DataEngine** (evolução do antigo framework **DBEBr**) é um framework Delphi que provê uma camada de abstração sólida para conexão e persistência de dados. Seu objetivo principal é permitir que o desenvolvedor escreva código independente do engine de banco de dados utilizado, facilitando a migração ou o suporte multi-engine sem alteração da lógica de negócio.

## Conceitos Chave

- **IDBConnection:** Interface central que abstrai a conexão nativa (TFDConnection, TSQLConnection, etc.).
- **IDBQuery:** Abstração para execução de comandos SQL e manipulação de parâmetros.
- **IDBDataSet:** Interface para manipulação e leitura de dados (compatível com TDataSet).
- **AsyncRunner:** Motor central para execução de operações em background.
- **Factory Pattern:** Sistema utilizado para injetar o driver específico de cada engine de conexão.

## Público Alvo

Desenvolvedores Delphi que buscam desacoplamento, testabilidade e flexibilidade em suas camadas de persistência, evitando o "lock-in" com componentes visuais de conexão.

## Por que usar o DataEngine?

- **Desacoplamento Total:** Troque de engine (ex: FireDAC, Zeos ou o nativo **JSON Store**) alterando apenas uma linha de código (a factory).
- **Consistência:** Interface única para operações de banco de dados e NoSQL em todo o projeto.
- **Ecossistema:** Base fundamentental para frameworks de alto nível como o Janus ORM.
- **Alta Performance:** Overhead mínimo sobre os drivers nativos.
