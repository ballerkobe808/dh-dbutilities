'use strict';

// dependencies
const sql = require('mssql');
const _ = require('lodash');
const { StringUtils } = require('dh-node-utilities');

// save the db options.
let dbOptions = null;
let dbConfig = null;
let poolInitialized = false;

//======================================================================================
// Initialization and Destruction Functions.
//======================================================================================

/**
 * Configures and initialized the mysql module and database connection.
 * @param options - The configuration options object from the config module.
 * @param callback - The finished callback function.
 */
exports.configure = function (options, callback) {
  // if the pool is already configured, just fire the finished callback.
  if (poolInitialized) {
    return callback();
  }

  // build the mysql specific connection pool options.
  let config = {
    user: options.username,
    password: options.password,
    database: options.dbName,
    pool: {
      max: options.connectionPoolLimit
    },
    options: {
      abortTransactionOnError: true
    }
  };

  // set both host and port if
  if (StringUtils.isEmpty(options.instanceName)) {
    config.server = options.server;
    config.port = options.port;
  }
  else {
    config.server = options.server;
    config.port = options.port;
    config.options.instanceName = options.instanceName;
  }

  // save the options.
  dbOptions = options;
  dbConfig = config;

  // create the mysql connection pool.
  sql.connect(dbConfig, function (err) {
    if (err) {
      return callback(err);
    }

    poolInitialized = true;
    return callback();
  });
};

/**
 * Performs the de-allocation/pool destruction code when application is exiting.
 */
exports.close = function (callback) {
  // if the pool wasn't created yet. Just fire the callback.
  if (!poolInitialized) {
    return callback();
  }

  // destroy the pool.
  sql.close(function (err) {
    return callback(err);
  });
};

/**
 * Gets the mysql session store object for express.
 * @param callback - The finished callback function.
 */
exports.getSessionStore = function(callback) {
  // make sure the module has been configured first.
  if (!poolInitialized) {
    return callback(new Error('DB connection pool not initialized.'));
  }

  // setup the session store.
  let session = require('express-session');
  let MSSQLStore = require('connect-mssql-v2')(session);

  // build the config options.
  let options = {
    table: (dbOptions.sessionTableName) ? dbOptions.sessionTableName : 'sessions',
  };

  // build the mysql specific connection pool options.
  let config = {
    user: dbOptions.username,
    password: dbOptions.password,
    database: dbOptions.sessionDatabaseName,
    pool: {
      max: dbOptions.connectionPoolLimit
    },
    options: {
      abortTransactionOnError: true
    }
  };

  // set both host and port if
  if (StringUtils.isEmpty(dbOptions.instanceName)) {
    config.server = dbOptions.server;
    config.port = dbOptions.port;
  }
  else {
    config.server = dbOptions.server;
    config.port = dbOptions.port;
    config.options.instanceName = dbOptions.instanceName;
  }

  // return a new instance of the MySQL session store.
  return callback(null, new MSSQLStore(config, options));
};

//======================================================================================
// SQL Functions.
//======================================================================================

/**
 * Runs a simple string query with no parameters.
 * @param sqlString - The string query.
 * @param callback - The finished callback function. callback (err, resultSet).
 * @param multipleResultSets - Flag indicating if multiple result sets are returned.
 */
exports.runStringQuery = function(sqlString, callback, multipleResultSets) {
  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // build the request object.
  let request = new sql.Request();

  // set the multiple flag.
  if (multipleResultSets) {
    request.multiple = true;
  }

  // run the query.
  request.query(sqlString, function(err, rows) {
    return callback(err, rows);
  });
};

/**
 * Runs a simple string query with no parameters.
 * @param transaction - The transaction object.
 * @param sqlString - The string query.
 * @param callback - The finished callback function. callback (err, resultSet).
 * @param multipleResultSets - Flag indicating if multiple result sets are returned.
 */
exports.runStringQueryInTransaction = function(transaction, sqlString, callback, multipleResultSets) {
  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // build the request object.
  let request = new sql.Request(transaction);

  // set the multiple flag.
  if (multipleResultSets) {
    request.multiple = true;
  }

  // run the query.
  request.query(sqlString, function(err, rows) {
    return callback(err, rows);
  });
};

/**
 * Runs a prepared statement with parameters.
 * @param queryString - The query string.
 * @param params - The params array.
 * @param callback - The finished callback function.
 * @param multipleResultSets - Flag indicating if multiple result sets are returned.
 */
exports.runQuery = function (queryString, params, callback, multipleResultSets) {
  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // build the prepared statement object.
  let ps = new sql.PreparedStatement();
  let query = null;

  // set the multiple flag.
  if (multipleResultSets) {
    ps.multiple = true;
  }

  // check if the params is an array of objects.
  if (isObjectParams(params)) {
    query = convertParamsObjectArrayToQueryObject(queryString, params, ps);
  }
  else {
    // convert the query.
    query = convertQueryAndParamsForMSSql(queryString, params, ps);
  }

  // prepare the statement.
  ps.prepare(query.sql, function (err) {
    if (err) {
      return callback(err);
    }

    // execute the statement.
    ps.execute(query.values, function (er, resultSet) {
      // un-prepare the statement.
      ps.unprepare(function(e) {
        if (e) {
          console.log(new Error('Failed to un-prepare a prepared statement.'));
        }

        return callback(err, resultSet);
      });
    });
  });
};

/**
 * Runs an update statement.
 * @param statement - The sql statement.
 * @param params - The parameters.
 * @param callback - The finished callback function.
 * @param multipleResultSets - Flag indicating if multiple result sets should be returned.
 */
exports.runStatement = function(statement, params, callback, multipleResultSets) {
  // save a reference.
  let _this = this;

  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // create a transaction connection object.
  let transaction = new sql.Transaction();

  // begin the transaction.
  transaction.begin(function (err) {
    // make sure the begin statement finished successfully.
    if (err) {
      return callback(err);
    }

    // run the statement.
    _this.runStatementInTransaction(transaction, statement, params, function (err, resultSet) {
      if (err) {
        transaction.rollback(function (e) {
          if (e) {
            console.log(e);
          }

          return callback(err);
        });
      }
      else {
        // commit the changes.
        transaction.commit(function (e) {
          return callback(e, resultSet);
        });
      }
    }, multipleResultSets);
  });
};

/**
 * Runs a bulk insert statement.
 * @param statement - The insert statement.
 * @param params - The values. Ex: [[values], [values]]
 * @param callback - The finished callback function. callback(err);
 */
exports.runBulkInsert = function (statement, params, callback) {
  return callback(new Error('Bulk insert not supported for mssql adapter.'));
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param idField - The field name of the ID field.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatementReturnResult = function(statement, params, idField, callback) {
  this.runStatement(statement, params, callback);
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param connection - The transaction object.
 * @param statement - The sql statement.
 * @param params - The params array.
 * @param callback - The finished callback function.
 * @param multipleResultSets - Flag indicating if multiple result sets should be returned.
 */
exports.runStatementInTransaction = function(connection, statement, params, callback, multipleResultSets) {
  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // build the prepared statement object.
  let ps = new sql.PreparedStatement(connection);
  let query = null;

  // set the multiple flag.
  if (multipleResultSets) {
    ps.multiple = true;
  }

  // check if the params is an array of objects.
  if (isObjectParams(params)) {
    query = convertParamsObjectArrayToQueryObject(statement, params, ps);
  }
  else {
    // convert the query.
    query = convertQueryAndParamsForMSSql(statement, params, ps);
  }

  // prepare the statement.
  ps.prepare(query.sql, function (err) {
    if (err) {
      return callback(err);
    }

    // execute the statement.
    ps.execute(query.values, function (er, resultSet) {
      // un-prepare the statement.
      ps.unprepare(function(e) {
        if (e) {
          console.log(new Error('Failed to un-prepare a prepared statement.'));
        }

        return callback(er, resultSet);
      });
    });
  });
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param connection - The transaction object.
 * @param statement - The sql statement.
 * @param params - The params array.
 * @param idField - The id field of the primary key.
 * @param callback - The finished callback function.
 * @param multipleResultSets - Flag indicating if multiple result sets should be returned.
 */
exports.runStatementInTransactionReturnResult = function(connection, statement, params, idField, callback, multipleResultSets) {
  this.runStatementInTransaction(connection, statement, params, callback, multipleResultSets);
};

/**
 * Executes a stored procedure.
 * params:
 * [
 *  paramType: 'input' | 'output',
 *  dataType: dbUtils.TYPES.NVarChar,
 *  value: <value>
 * ]
 * @param procedureName - The call statement.
 * @param params - The params array.
 * @param callback - The finished callback funciton.
 * @param multipleResultSets - flag indicating if multiple results sets are returned.
 */
exports.executeStoredProcedure = function(procedureName, params, callback, multipleResultSets) {
  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // build the request object.
  let request = new sql.Request();

  // set the multiple flag.
  if (multipleResultSets) {
    request.multiple = true;
  }

  // holds the names of the output params.
  let outputParams = [];

  // add the parameters to the call.
  for (let i = 0; i < params.length; i++) {
    let currentParam = params[i];
    if (currentParam.paramType.toLowerCase() === 'input') {
      request.input(currentParam.name, currentParam.dataType, currentParam.value);
    }
    else {
      request.output(currentParam.name, currentParam.dataType);
      outputParams.push(currentParam.name);
    }
  }

  // execute the proc.
  request.execute(procedureName, function (err, recordsets, returnValue) {
    if (err) {
      return callback(err);
    }

    // get the output values if there are any.
    let outputValues = {};

    // if there are output params. get the values.
    if (outputParams.length > 0) {

      // get the output values.
      for (let i = 0; i < outputParams.length; i++) {
        // make sure the value is set.
        if (request.parameters[outputParams[i]]) {
          outputValues[outputParams[i]] = request.parameters[outputParams[i]].value;
        }
      }
    }

    return callback(null, recordsets, returnValue, outputValues);
  });
};

/**
 * Creates and runs a transaction on the database.
 * @param executeFunction - The function to be executed containing the statements to run. Should take in a callback function.
 * @param callback - The finished callback function.
 */
exports.runTransaction = function(executeFunction, callback) {
  // make sure the connection pool was initialized.
  if (!poolInitialized) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // create a transaction connection object.
  let transaction = new sql.Transaction();

  // begin the transaction.
  transaction.begin(function (err) {
    // make sure the begin statement finished successfully.
    if (err) {
      return callback(err);
    }

    // fire the execute function.
    executeFunction(transaction, function (er) {
      if (er) {
        transaction.rollback(function (e) {
          if (e) {
            console.log(e);
          }

          return callback(er);
        });
      }
      else {
        // commit the changes.
        transaction.commit(function (e) {
          return callback(e);
        });
      }
    });
  });
};

//======================================================================================
// Private Functions.
//======================================================================================

/**
 * Converts the standard query the ? placeholders and params array to query with param
 * place hodlers and an object.
 * @param query - The string query.
 * @param params - The params array.
 * @param ps - The prepared statement object.
 */
function convertQueryAndParamsForMSSql(query, params, ps) {
  // initialize the object.
  let result = {
    sql: '',
    values: {}
  };

  // used to generate the parameter place holders.
  let paramString = 'param';
  let index = 0;
  let i = 0;

  // loop until all ? are found.
  while((i=query.indexOf('?', i+1)) >= 0) {
    // build the param name.
    let paramName = paramString + index.toString();

    // replace the question mark with $ + index.
    query = query.substr(0, i) + '@' + paramName + query.substr(i + 1);

    // add the value to the param object.
    result.values[paramName] = params[index];
    ps.input(paramName, getType(params[index]));

    // increment the index.
    index++;
  }

  // set the updated query.
  result.sql = query;

  return result;
}

/**
 * Attempts to get the field type.
 * @param value - The value to get the type of.
 */
function getType(value) {
  if (_.isDate(value)) {
    return sql.TYPES.DateTime;
  }
  else if (_.isString(value)) {
    return sql.TYPES.NVarChar;
  }
  else if (_.isNumber(value)) {
    if (isInt(value)) {
      return sql.TYPES.Int;
    }
    else {
      return sql.TYPES.Decimal;
    }
  }
  else if (_.isBoolean(value)) {
    return sql.TYPES.Bit;
  }
  else {
    return sql.TYPES.NVarChar;
  }
}

/**
 * Checks if a number is an integer.
 * @param n
 * @returns {boolean}
 */
function isInt(n){
  return Number(n) === n && n % 1 === 0;
}

/**
 * Checks if the params array is an array of type objects mssql module uses.
 * @param paramsArray - The params array.
 */
function isObjectParams(paramsArray) {
  // default result to false;
  let result = false;

  // check if the params array is empty or not.
  if (paramsArray && paramsArray.length > 0) {
    for (let i = 0; i < paramsArray.length; i++) {
      if (_.isObject(paramsArray[i]) && !StringUtils.isEmpty(paramsArray[i].type)) {
        result = true;
        break;
      }
    }
  }

  return result;
}

/**
 * Builds a query object out of the sql and params array.
 * @param sql
 * @param params
 * @param ps
 * @returns {{sql: *, values: {}}}
 */
function convertParamsObjectArrayToQueryObject(sql, params, ps) {
  // build the result object.
  let result = {
    sql: sql,
    values: {}
  };

  // add the input parameters.
  for (let i = 0; i < params.length; i++) {
    ps.input(params[i].name, params[i].type);
    result.values[params[i].name] = params[i].value;
  }

  return result;
}
