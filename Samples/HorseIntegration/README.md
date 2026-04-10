# DataEngine - Horse Integration Sample

This sample demonstrates how to use the `Horse.DataEngine` middleware to integrate the DataEngine connection pool into a Horse web application.

## Prerequisites

- Delphi Seattle or superior.
- [Horse](https://github.com/HashLoad/horse) framework.
- DataEngine Core and FireDAC driver.

## How it works

1. The middleware is registered using `THorse.Use(HorseDataEngine(PoolManager, ConnectionFactory))`.
2. For every incoming HTTP request:
   - A connection is acquired from the `PoolManager`.
   - The connection is stored in the `THorseRequest.Session`.
   - A transaction is automatically started (default behavior).
3. The request handler (route) can access the connection using `Req.SessionIDBConnection`.
4. After the request is processed:
   - If the HTTP status is < 400, the transaction is committed.
   - If the HTTP status is >= 400 or an exception occurred, the transaction is rolled back.
   - The connection is released back to the pool.

## Endpoints

- `GET /users`: Returns the count of users in the in-memory database.
- `POST /users`: Adds a new user with a timestamp.

## Sample Code

```delphi
THorse.Get('/users',
  procedure(Req: THorseRequest; Res: THorseResponse; Next: TNextProc)
  var
    LConn: IDBConnection;
  begin
    LConn := Req.SessionIDBConnection;
    // Use LConn...
  end);
```
