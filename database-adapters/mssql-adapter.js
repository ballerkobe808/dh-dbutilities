'use strict';

// dependencies
var sql = require('mssql');
var poolInitialized = false;
var _ = require('underscore');
var stringUtilities = require('dh-node-utilities').StringUtils;
var async = require('async');

// save the db options.
var dbOptions = null
var dbConfig = null;

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
  var config = {
    user: options.username,
    password: options.password,
    database: options.dbName,
    pool: {
      max: options.connectionPoolLimit
    }
  };

  // set both host and port if
  if (stringUtilities.isEmpty(options.instanceName)) {
    config.server = options.server;
    config.port = options.port;
  }
  else {
    config.server = options.server;
    config.port = options.port;
    config.options = {};
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
  var session = require('express-session');
  var MSSQLStore = require('connect-mssql')(session);

  // build the config options.
  var options = {
    table: (dbOptions.sessionTableName) ? dbOptions.sessionTableName : 'sessions',
  };

  // return a new instance of the MySQL session store.
  return callback(null, new MSSQLStore(dbConfig, options));
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
  var request = new sql.Request();

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
  var ps = new sql.PreparedStatement();
  var query = null;

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
      // unprepare the staetment.
      ps.unprepare(function(e) {
        if (e) {
          console.log(new Error('Failed to unprepare a prepared statement.'));
        }

        return callback(err, resultSet);
      });
    });
  });
}

//runStatement(statement, params, callback)
//runStatementReturnResult(statement, params, idField, callback)
//runStatementInTransaction(connection, statement, params, callback)
//runStatementInTransactionReturnResult(connection, statement, params, idField, callback)

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
  var request = new sql.Request();

  // set the multiple flag.
  if (multipleResultSets) {
    request.multiple = true;
  }

  // add the parameters to the call.
  for (var i = 0; i < params.length; i++) {
    var currentParam = params[i];
    if (currentParam.paramType.toLowerCase() == 'input') {
      request.input('input_parameter', currentParam.dataType, currentParam.value);
    }
    else {
      request.output('output_parameter', currentParam.dataType);
    }
  }

  // execute the proc.
  request.execute(procedureName, function (err, recordsets, returnValue) {
    return callback(err, recordsets, returnValue);
  });
};

//runTransaction(executeFunction, callback);

//======================================================================================
// Private Functions.
//======================================================================================

/**
 * Converts the standard query the ? placeholders and params array to query with param
 * placehodlers and an object.
 * @param query - The string query.
 * @param params - The params array.
 * @param ps - The prepared statement object.
 */
function convertQueryAndParamsForMSSql(query, params, ps) {
  // initialize the object.
  var result = {
    sql: '',
    values: {}
  }

  // used to generate the parameter place holders.
  var paramString = 'param';
  var index = 0;
  var i = 0;

  // loop until all ? are found.
  while((i=query.indexOf('?', i+1)) >= 0) {
    // build the param name.
    var paramName = paramString + index.toString();

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
};

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
  var result = false;

  // check if the params array is empty or not.
  if (paramsArray && paramsArray.length > 0) {
    for (var i = 0; i < paramsArray.length; i++) {
      if (_.isObject(paramsArray[i]) && !stringUtilities.isEmpty(paramsArray[i].type)) {
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
  var result = {
    sql: sql,
    values: {}
  };

  // build the values object.
  var values = {};

  // add the input parameters.
  for (var i = 0; i < params.length; i++) {
    ps.input(params[i].name, params[i].type);
    values[params[i].name] = params[i].value;
  }

  return result;
}