'use strict';

// dependencies
var pg = require('pg');
var pgTransaction = require('pg-transact');
var stringUtilities = require('dh-node-utilities').StringUtils;

// save the configuration object.
var config = null;
var connectionString = null;

//======================================================================================
// Initialization and Destruction Functions.
//======================================================================================

/**
 * Configures and initialized the mysql module and database connection.
 * @param options - The configuration options object from the config module.
 * @param callback - The finished callback function.
 */
exports.configure = function (options, callback) {
  // save the config options.
  config = options;

  // build the connection string.
  connectionString = 'postgres://' + options.username + ':' + options.password + '@' + options.server + '/' + options.dbName;

  // fire the finished callback.
  return callback();
};

/**
 * Performs the de-allocation/pool destruction code when application is exiting.
 */
exports.close = function (callback) {
  return callback();
};

/**
 * Gets the mysql session store object for express.
 * @param callback - The finished callback function.
 */
exports.getSessionStore = function(callback) {
  // make sure the module has been configured first.
  if (!driverInitialized()) {
    return callback(new Error('DB driver not initialized.'));
  }

  // dependencies.
  var express = require('express');
  var PGSessionStore = require('connect-pg-simple')(express.session);

  // build the options object.
  var options = {
    pg: pg,
    conString: connectionString,
    tableName: config.sessionTableName
  };

  // create and return the session store.
  return callback(null, new PGSessionStore(options));
};


//======================================================================================
// SQL Functions.
//======================================================================================

/**
 * Runs a string query with no external parameters on the database.
 * @param sqlQuery - The string query.
 * @param callback - The finished callback function. callback(err, rows);
 * @returns {*}
 */
exports.runStringQuery = function (sqlQuery, callback) {
  // make sure the drive is initialized first.
  if (!driverInitialized()) {
    return callback(new Error('Postgres adapter is not initialized.'));
  }

  // get a connection from the connection pool.
  pg.connect(connectionString, function (err, client, done) {
    // check if an error occurred.
    if (err) {
      // notify the pool to kill the client and remove the bad connection from the pool.
      done(client);
      return callback(err);
    }

    // execute the query.
    client.query(sqlQuery, function (err, result) {
      // return the client to the pool.
      done();

      // check if an error occurred.
      if (err) {
        return callback(err);
      }

      // return the rows.
      return callback(null, result.rows);
    });
  });
};

/**
 * Runs a sql query with parameters to be inserted into the statement.
 * @param sqlString - The sql string with question mark placeholders.
 * @param params - The array parameters to be added to the sql query.
 * @param callback - The finished callback function. callback(err, rows);
 */
exports.runQuery = function (sqlString, params, callback) {
  // make sure the drive is initialized first.
  if (!driverInitialized()) {
    return callback(new Error('Postgres adapter is not initialized.'));
  }

  // replace all the ? placeholders with the postgres style placeholders.
  var query = replacePlaceHolders(sqlString);

  // get a connection from the connection pool.
  pg.connect(connectionString, function (err, client, done) {
    // check if an error occurred.
    if (err) {
      // notify the pool to kill the client and remove the bad connection from the pool.
      done(client);
      return callback(err);
    }

    // execute the query.
    client.query(query, params, function (err, result) {
      // return the client to the pool.
      done();

      // check if an error occurred.
      if (err) {
        return callback(err);
      }

      // return the rows.
      return callback(null, result.rows);
    });
  });
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatement = function (statement, params, callback) {
  // make sure the drive is initialized first.
  if (!driverInitialized()) {
    return callback(new Error('Postgres adapter is not initialized.'));
  }

  // replace all the ? placeholders with the postgres style placeholders.
  var query = replacePlaceHolders(statement);

  // get a connection from the connection pool.
  pg.connect(connectionString, function (err, client, done) {
    // check if an error occurred.
    if (err) {
      // notify the pool to kill the client and remove the bad connection from the pool.
      done(client);
      return callback(err);
    }

    // execute the query.
    client.query(query, params, function (err, result) {
      // return the client to the pool.
      done();

      // check if an error occurred.
      if (err) {
        return callback(err);
      }

      // return the rows.
      return callback(null, result.rows);
    });
  });
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param idField - The field name of the ID field.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatementReturnResult = function (statement, params, idField, callback) {
  // make sure the drive is initialized first.
  if (!driverInitialized()) {
    return callback(new Error('Postgres adapter is not initialized.'));
  }

  // replace all the ? placeholders with the postgres style placeholders.
  var query = replacePlaceHolders(statement, idField);

  // add the returning id field.
  query = addReturningID(query);

  // get a connection from the connection pool.
  pg.connect(connectionString, function (err, client, done) {
    // check if an error occurred.
    if (err) {
      // notify the pool to kill the client and remove the bad connection from the pool.
      done(client);
      return callback(err);
    }

    // execute the query.
    client.query(query, params, function (err, result) {
      // return the client to the pool.
      done();

      // check if an error occurred.
      if (err) {
        return callback(err);
      }

      // return the rows.
      return callback(null, result);
    });
  });
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement as part of a sql transaction.
 * @param client - The sql client connection.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatementInTransaction = function (client, statement, params, callback) {
  // replace all the ? placeholders with the postgres style placeholders.
  var query = replacePlaceHolders(statement);

  // execute the query.
  client.query(query, params, function (err, result) {
    // check if an error occurred.
    if (err) {
      return callback(err);
    }

    // return the rows.
    return callback(null, result.rows);
  });
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement as part of a sql transaction.
 * @param client - The sql client connection.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param idField - The field name of the ID field.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatementInTransactionReturnResult = function (client, statement, params, idField, callback) {
  // replace all the ? placeholders with the postgres style placeholders.
  var query = replacePlaceHolders(statement);

  // add the returning id field.
  query = addReturningID(query, idField);

  // execute the query.
  client.query(query, params, function (err, result) {
    // check if an error occurred.
    if (err) {
      return callback(err);
    }

    // return the rows.
    return callback(null, result);
  });
};

/**
 * Executes a stored procedure and returns the results.
 * @param sql - The sql call.
 * @param params - Array of parameters needed by the call.
 * @param callback - The finished callback function.
 */
exports.executeStoredProcedure = function (sql, params, callback) {
  // make sure the drive is initialized first.
  if (!driverInitialized()) {
    return callback(new Error('Postgres adapter is not initialized.'));
  }

  // replace the work call with select since postgres uses select instead.
  var statement = replaceCall(sql);
  statement = replacePlaceHolders(statement);

  // get a connection from the connection pool.
  pg.connect(connectionString, function (err, client, done) {
    // check if an error occurred.
    if (err) {
      // notify the pool to kill the client and remove the bad connection from the pool.
      done(client);
      return callback(err);
    }

    // execute the query.
    client.query(statement, params, function (err, result) {
      // return the client to the pool.
      done();

      // check if an error occurred.
      if (err) {
        return callback(err);
      }

      // return the rows.
      return callback(null, result.rows);
    });
  });
};

/**
 * Creates and runs a transaction on the database.
 * @param executeFunction - The function to be executed containing the statements to run. Should take in a callback function.
 * @param callback - The finished callback function.
 */
exports.runTransaction = function (executeFunction, callback) {
  // get a connection from the connection pool.
  pg.connect(connectionString, function (err, client, done) {
    // check if an error occurred.
    if (err) {
      // notify the pool to kill the client and remove the bad connection from the pool.
      done(client);
      return callback(err);
    }

    // run the transaction.
    pgTransaction(client, executeFunction, done).then(
      // on success.
      function () {
        done();
        return callback();
      },

      // on error.
      function (err) {
        done();
        return callback(err);
      }
    );
  });
};

//======================================================================================
// Private Functions.
//======================================================================================

/**
 * Checks that the connection string was set.
 * @returns {boolean}
 */
function driverInitialized () {
  return (connectionString != null);
}

/**
 * Replaces the default ? placeholders with postgres expected placeholders.
 * @param str - The sql string to perform the replaces on.
 */
function replacePlaceHolders(str){
  // set the indexAt to -1 for not found.
  var i = -1;

  // set index to 1 since that's what postgres starts with on placeholders.
  var index = 1;

  // loop until all ? are found.
  while((i=str.indexOf('?',i+1)) >= 0) {
    // replace the question mark with $ + index.
    str = str.substr(0, i) + '$' + index.toString() + str.substr(i+1);

    // increment the index.
    index++;
  }

  // return the string.
  return str;
}

/**
 * Replaces standard stored procedure CALL with SELECT since postgres uses SELECT.
 * @param statement - The string statement.
 */
function replaceCall (statement) {
  var regex = new RegExp('call', "ig");
  return statement.replace(regex, 'SELECT');
}

/**
 * Adds the returning statement to sql statements for inserts.
 * @param statement
 */
function addReturningID(statement, idField) {
  // if its an insert statement.
  if (stringUtilities.contains(statement, 'insert', false)) {
    // if it doesn't already contain the returning statement.
    if (!stringUtilities.contains(statement, 'returning', false)) {
      return statement + ' RETURNING ' + idField;
    }
  }

  // if it gets here just return the original statement.
  return statement;
}