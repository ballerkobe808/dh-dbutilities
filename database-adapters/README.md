Creating Adapters
====================================================================
____________________________________________________________________

##Configuration Options

  // get the settings options from the config module.
  let options = {
    server: String,
    port: String,
    dbName: String,
    username: String,
    password: String,
    connectionPoolLimit: Number,
    sessionTableName: String,
    sessionSecret: String
  };


##Required function implementations:

  - configure(options, callback) (Called when initializing the db utility module. This is where you will perform the database connection/pool creation process).
  - close(callback) (Called when the application crashes or exits. This is where you will close existing connections and destroy your connection pool).
  - getSessionStore(callback) (called when setting up express sessions).
  - runStringQuery(queryString, callback)
  - runQuery(queryString, params, callback)
  - runStatement(statement, params, callback)
  - runStatementReturnResult(statement, params, idField, callback)
  - runStatementInTransaction(connection, statement, params, callback)
  - runStatementInTransactionReturnResult(connection, statement, params, idField, callback)
  - executeStoredProcedure(statement, params, callback)
  - runTransaction(executeFunction, callback);

    The execute function show be declared as the following:

        function (connection, callback) {}
