---
displayed_sidebar: dataengineSidebar
title: Erros Comuns e Soluções
---

## Erro: O Driver selecionado não foi encontrado

- **Sintoma:** Ao instanciar a factory, o sistema lança uma exceção `Driver not found`.
- **Causa Provável:** A unit do driver específico (ex: `DataEngine.DriverFireDac.pas`) não foi adicionada às cláusulas `uses` do projeto.
- **Solução:** Certifique-se de incluir a unit do driver e a sua factory no projeto.

## Erro de Parâmetro: Parâmetro ':ID' não encontrado

- **Sintoma:** Ao tentar atribuir um valor a um parâmetro via `ParamByName`, ocorre um erro.
- **Causa Provável:** O SQL foi definido com um nome ou formato incompatível com o engine nativo selecionado.
- **Solução:** Revise o SQL e garanta que o sinal de `:` preceda o nome do parâmetro. Verifique se o engine nativo (como FireDac) está configurado para reconhecer parâmetros nomeados.

## Atraso na Consulta: Lentidão no SELECT inicial

- **Sintoma:** A primeira consulta de uma tabela é sensivelmente mais lenta que as demais.
- **Causa Provável:** O **Metadata Cache** está sendo populado pela primeira vez para esta tabela.
- **Solução:** Este é um comportamento esperado. Para mitigar, agrupe as consultas de metadados em modo `preload` se o seu volume de dados for muito alto e o banco remoto for lento.

## Erro JSON: Arquivo não encontrado ou permissão negada

- **Sintoma:** Ao conectar o **JSON Store Driver**, ocorre um erro de E/S.
- **Causa Provável:** O caminho do arquivo informado em `TFactoryJSON.Create` é inválido ou o processo não tem permissão de escrita.
- **Solução:** Verifique o caminho físico. Como o driver utiliza o *Safe Write Pattern* (.tmp), certifique-se de que o diretório permite a criação de arquivos temporários.

## Erro Redis: Could not connect to Redis

- **Sintoma:** Ao iniciar a aplicação ou executar uma query com cache, ocorre um erro `ERedisError`.
- **Causa Provável:** O servidor Redis está offline, a porta (padão 6379) está bloqueada pelo firewall ou as credenciais (Password) estão incorretas.
- **Solução:** Verifique se o serviço Redis está ativo. Teste a conectividade via `redis-cli` ou `telnet`. Valide os parâmetros passados no `TRedisCacheProvider.Create`.

## Erro: Operação interrompida por queda de rede

- **Sintoma:** Exceções nativas de rede (ex: `Socket Error`, `Connection Interrupted`) durante a execução de comandos.
- **Causa Provável:** Falha física na rede ou timeout do servidor de banco de dados.
- **Solução:** Utilize o **Connection Resiliency System** através do `TGuardConnection`. Configure uma política de retry adequada (ex: 3 tentativas com 500ms de delay) para que o DataEngine recupere a operação automaticamente sem propagar o erro para a UI. Veja o guia de [Resiliência de Conexão](../guides/connection-resiliency.md).
