'use strict';

// dependencies.
const mysql = require('mysql');
const _ = require('lodash');

// save the db options.
let dbOptions = null;
let pool = null;

//======================================================================================
// Initialization and Destruction Functions.
//======================================================================================

/**
 * Configures and initialized the mysql module and database connection.
 * @param options - The configuration options object from the config module.
 * @param callback - The finished callback function.
 */
exports.configure = (options, callback) => {
  // if the pool is already configured, just fire the finished callback.
  if (pool) {
    return callback();
  }

  // build the mysql specific connection pool options.
  let config = {
    host: options.server,
    port: options.port,
    user: options.username,
    password: options.password,
    database: options.dbName,
    connectionLimit: options.connectionPoolLimit,
    sessionTableName: options.sessionTableName,
    sessionSecret: options.sessionSecret,
    acquireTimeout: 30000,
    multipleStatements: options.multipleStatements,
    ssl: options.ssl
  };

  // save the options.
  dbOptions = config;

  // create the mysql connection pool.
  pool = mysql.createPool(config);

  // wait for a connection to be established.
  return callback();
};

/**
 * Performs the de-allocation/pool destruction code when application is exiting.
 * @param callback - The finished callback function.
 */
exports.close = (callback) => {
  // if the pool wasn't created yet. Just fire the callback.
  if (!pool) {
    return callback();
  }

  // destroy the pool.
  pool.end((err) => {
    return callback(err);
  });
};

/**
 * Gets the mysql session store object for express.
 * @param callback - The finished callback function.
 */
exports.getSessionStore = (callback) => {
  // make sure the module has been configured first.
  if (!pool) {
    return callback(new Error('DB connection pool not initialized.'));
  }

  // setup the session store.
  let expressSession = require('express-session');
  let MySQLStore = require('connect-mysql')(expressSession);

  // build the config options.
  let config = {
    pool: pool,
    table: (dbOptions.sessionTableName) ? dbOptions.sessionTableName : 'session',
    secret: (dbOptions.sessionSecret) ? dbOptions.sessionSecret : 'SECRETSTRINGSHHH',
    cleanup: (dbOptions.cleanup) ? dbOptions.cleanup : false
  };

  // return a new instance of the MySQL session store.
  return callback(null, new MySQLStore(config));
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
exports.runStringQuery = (sqlQuery, callback) => {
  // make sure the pool is initialized first.
  if (!pool) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // get a pooled connection.
  pool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      // release the connection.
      if (connection) {
        connection.release();
      }
      return callback(err);
    }

    // fire the query.
    connection.query(sqlQuery, (err, rows) => {
      // release the connection back to the pool.
      connection.release();

      // return the results.
      return callback(err, rows);
    });
  });
};

/**
 * Runs a sql query with parameters to be inserted into the statement.
 * @param sqlString - The sql string with question mark placeholders.
 * @param params - The array parameters to be added to the sql query.
 * @param callback - The finished callback function. callback(err, rows);
 */
exports.runQuery = (sqlString, params, callback) => {
  // make sure the pool is initialized.
  if (!pool) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // get a connection from the connection pool.
  pool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      // release the connection.
      if (connection) {
        connection.release();
      }
      return callback(err);
    }


    // fire the query.
    connection.query(sqlString, params, (err, rows) => {
      // release the connection back to the pool.
      connection.release();

      // return the results.
      return callback(err, rows);
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
exports.runStatement = (statement, params, callback) => {
  // make sure the pool is initialized.
  if (!pool) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // get a connection from the connection pool.
  pool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      // release the connection.
      if (connection) {
        connection.release();
      }
      return callback(err);
    }

    // fire the query.
    connection.query(statement, params, (err, results) => {
      // release the connection back to the pool.
      connection.release();

      // return the results.
      return callback(err, results);
    });
  });
};

/**
 * Runs a bulk insert statement.
 * @param statement - The insert statement.
 * @param params - The values. Ex: [[values], [values]]
 * @param callback - The finished callback function. callback(err);
 */
exports.runBulkInsert = (statement, params, callback) => {
  // make sure the pool is initialized.
  if (!pool) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // get a connection from the connection pool.
  pool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      // release the connection.
      if (connection) {
        connection.release();
      }
      return callback(err);
    }

    // fire the query.
    connection.query(statement, [params], (err, results) => {
      // release the connection back to the pool.
      connection.release();

      // return the results.
      return callback(err, results);
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
exports.runStatementReturnResult = (statement, params, idField, callback) => {
  this.runStatement(statement, params, callback);
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param connection - The sql connection.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatementInTransaction = (connection, statement, params, callback) => {
  // make sure the pool is initialized.
  if (!pool) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // fire the query.
  connection.query(statement, params, (err, results) => {
    // return the results.
    return callback(err, results);
  });
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to
 * inject into the sql statement.
 * @param connection - The sql connection.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - An array of parameters.
 * @param idField - The field name of the ID field.
 * @param callback - The finished callback function. callback(err, results);
 */
exports.runStatementInTransactionReturnResult = (connection, statement, params, idField, callback) => {
  this.runStatementInTransaction(connection, statement, params, callback);
};

/**
 * Executes a stored procedure and returns the results.
 * @param sql - The sql call.
 * @param params - Array of parameters needed by the call.
 * @param callback - The finished callback function.
 */
exports.executeStoredProcedure = (sql, params, callback) => {
  if (!pool) {
    return callback(new Error('Connection pool not initialized.'));
  }

  // get a connection from the connection pool.
  pool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      // release the connection.
      if (connection) {
        connection.release();
      }
      return callback(err);
    }

    // run the procedure.
    connection.query(sql, params, (err, results) => {
      // release the connection back to the pool.
      connection.release();

      // check if an error occurred.
      if (err) {
        return callback(err);
      }

      // parse the results object.
      if (_.isArray(results)) {
        return callback(null, results[0]);
      }
      else {
        return callback(null, results);
      }
    });
  });
};

/**
 * Creates and runs a transaction on the database.
 * @param executeFunction - The function to be executed containing the statements to run. Should take in a callback function.
 * @param callback - The finished callback function.
 */
exports.runTransaction = (executeFunction, callback) => {
  // get a connection from the connection pool.
  pool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      // release the connection.
      if (connection) {
        connection.release();
      }

      return callback(err);
    }

    // begin a transaction.
    connection.beginTransaction((err) => {
      // check if an error occurred creating a transaction.
      if (err) {
        connection.release();
        return callback(err);
      }

      // execute the function.
      executeFunction(connection, (err) => {
        // check if an error occurred.
        if (err) {
          // rollback any changes in the event of an error.
          connection.rollback(() => {
            connection.release();
            return callback(err);
          });
        }
        else {
          connection.commit((err) => {
            if (err) {
              connection.rollback(() => {
                connection.release();
                return callback(err);
              });
            }
            else {
              connection.release();
              return callback();
            }
          });
        }
      });
    });
  });
};
