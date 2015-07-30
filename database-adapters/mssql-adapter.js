'use strict';

// dependencies
var sql = require('mssql');
var poolInitialized = false;
var _ = require('underscore');

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
    server: options.server,
    port: options.port,
    user: options.username,
    password: options.password,
    database: options.dbName,
    pool: {
      max: options.connectionPoolLimit
    }
  };

  // save the options.
  dbOptions = options;
  dbConfig = config;

  // create the mysql connection pool.
  sql.connect(dbConfig, function (err) {
    // wait for a connection to be established.
    return callback(err);
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
  return callback(null, new MySQLStore(config, options));
};