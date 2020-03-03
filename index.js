'use strict';

// dependencies.
const _ = require('lodash');
const validator = require('validator');
const async = require('async');
const StringUtils  = require('./utilities/string-utilities');

// Adapter names.
const constants = require('./constants/constants');
exports.constants = constants;

// MSSQL specific
const sql = require('mssql');
exports.TYPES = sql.TYPES;

// current adapter to use.
let currentAdapterName = null;
let currentAdapter = null;

// flag indicating if the module was configured or not yet.
let isConfigured = false;

// reference to the db options.
let dbOptions = null;

//======================================================================================
// Initialization and Destruction Functions.
//======================================================================================

/**
 * Performs initialization and configuration of the sql db adapter.
 * @param options - The db config options object.
 * @param callback - The finished callback function.
 */
exports.configure = (options, callback) => {
  // check if the module was already configured.
  if (isConfigured) {
    return callback();
  }

  // save the options.
  dbOptions = options;

  // save the adapter name.
  currentAdapterName = options.adapterName;

  // make sure the adapter name is set.
  if (!currentAdapterName) {
    return callback(new Error('Adapter name is not set.'));
  }

  // require the current used sql adapter.
  currentAdapter = require('./database-adapters/' + currentAdapterName);

  // configure the adapter.
  currentAdapter.configure(options, (err) => {
    // if there was no error, set the isConfigured flag to true.
    if (!err) {
      isConfigured = true;
    }

    // fire the callback to signal that the module was configured.
    return callback(err);
  });
};

/**
 * Getter function for the isConfigured flag.
 * @returns {boolean}
 */
exports.isConfigured = () => {
  return isConfigured;
};

/**
 * Close all connections to the underlying sql database.
 * @param callback - Finished callback function.
 */
exports.close = (callback) => {
  // make sure the adapter has been configured.
  if (!isConfigured) {
    return callback(new Error('Module not configured.'));
  }

  // close the pool.
  currentAdapter.close((err) => {
    return callback(err);
  });
};

/**
 * Gets the session store object for express.
 * @param callback - The finished callback function.
 */
exports.getSessionStore = (callback) => {
  // check if the module is configured or not.
  if (!isConfigured) {
    return callback(new Error('Module not configured.'));
  }

  // get the session store.
  currentAdapter.getSessionStore(callback);
};

//======================================================================================
// SQL Functions.
//======================================================================================

/**
 * Runs a string sql query with no external parameters.
 * @param query - The string query.
 * @param callback - The finished callback function.
 * @param multipleResultSets - Flag indicating if multiple result sets are returned.
 */
exports.runStringQuery = (query, callback, multipleResultSets) => {
  currentAdapter.runStringQuery(query, callback, multipleResultSets);
};

/**
 * Runs a sql query with parameters to be inserted into the statement.
 * @param sqlString - The sql string with question mark placeholders.
 * @param params - The array of parameters to be inserted.
 * @param callback - The finished callback function. callback(err, rows);
 * @param multipleResultSets - Flag indicating if multiple result sets are returned.
 */
exports.runQuery = (sqlString, params, callback, multipleResultSets) => {
  currentAdapter.runQuery(sqlString, params, callback, multipleResultSets);
};

/**
 * Runs a sql update, insert, delete on the database with an array of
 * parameters to inject into the sql statement.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - an object of key/value pairs where key is field name and value is the value.
 * @param callback - The finished callback function. callback(err, results);
 * @param multipleResultSets - Flag indicating whether or not multiple results sets are being returned.
 */
exports.runStatement = (statement, params, callback, multipleResultSets) => {
  currentAdapter.runStatement(statement, params, callback, multipleResultSets);
};

/**
 * Runs a bulk insert statement.
 * @param statement - The sql insert statement. Ex: INSERT INTO table_name (name, email, comment) VALUES ?;
 * @param params - The values. Ex: [[values], [values]]
 * @param callback - The finished callback function. callback(err);
 */
exports.runBulkInsert = (statement, params, callback) => {
  currentAdapter.runBulkInsert(statement, params, callback);
};

/**
 * Runs a sql update, insert, delete on the database with an array of parameters to inject into the sql
 * statement and returns the results back.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - an object of key/value pairs where key is field name and value is the value.
 * @param idField - The ID field name.
 * @param callback - The finished callback function. callback(err, results);
 * @param multipleResultSets - Flag indicating whether or not multiple results sets are being returned.
 */
exports.runStatementReturnResult = (statement, params, idField, callback, multipleResultSets) => {
  currentAdapter.runStatementReturnResult(statement, params, idField, (err, results) => {
    if (err) {
      return callback(err);
    }

    if (_.isEqual(currentAdapterName, constants.MYSQL_ADAPTER)) {
      results.newRowId = results.insertId;
    }
    else if (_.isEqual(currentAdapterName, constants.MSSQL_ADAPTER)) {
      if (results && results.length > 0) {
        let newRowId = results[0][idField];
        results = {
          resultSet: results,
          newRowId: newRowId
        };
      }
    }

    return callback(null, results);
  }, multipleResultSets);
};

/**
 * Runs a sql update, insert, delete on the database with an array of
 * parameters to inject into the sql statement in a transaction.
 * @param connection - The sql connection to use.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - an object of key/value pairs where key is field name and value is the value.
 * @param callback - The finished callback function. callback(err, results);
 * @param multipleResultSets - Flag indicating whether or not multiple results sets are being returned.
 */
exports.runStatementInTransaction = (connection, statement, params, callback, multipleResultSets) => {
  currentAdapter.runStatementInTransaction(connection, statement, params, callback, multipleResultSets);
};

/**
 * Runs a sql update, insert, delete on the database with an array of
 * parameters to inject into the sql statement in a transaction and returns a result object.
 * @param connection - The sql connection to use.
 * @param statement - The sql statement string with question mark placeholders.
 * @param params - an object of key/value pairs where key is field name and value is the value.
 * @param idField - The field name of the ID field.
 * @param callback - The finished callback function. callback(err, results);
 * @param multipleResultSets - Flag indicating whether or not multiple results sets are being returned.
 */
exports.runStatementInTransactionReturnResult = (connection, statement, params, idField, callback, multipleResultSets) => {
  currentAdapter.runStatementInTransactionReturnResult(connection, statement, params, idField, (err, results) => {
    if (err) {
      return callback(err);
    }

    if (_.isEqual(currentAdapterName, constants.MYSQL_ADAPTER)) {
      results.newRowId = results.insertId;
    }
    else if (_.isEqual(currentAdapterName, constants.MSSQL_ADAPTER)) {
      if (results && results.length > 0) {
        let newRowId = results[0][idField];
        results = {
          resultSet: results,
          newRowId: newRowId
        };
      }
    }

    return callback(null, results);
  }, multipleResultSets);
};

/**
 * Executes a stored procedure.
 * @param statement - The string procedure call with question mark wildcards.
 * @param params - The procedure parameters to replace.
 * @param callback - The finished callback function. callback(err, results);
 * @param multipleResultSets - Flag indicating whether or not multiple results sets are being returned.
 */
exports.executeStoredProcedure = (statement, params, callback, multipleResultSets) => {
  currentAdapter.executeStoredProcedure(statement, params, callback, multipleResultSets);
};

/**
 * Executes a transaction on the database.
 * @param executeFunction - The function to be run after the begin transaction statement.
 * @param callback - The finished callback function.
 */
exports.runTransaction = (executeFunction, callback) => {
  currentAdapter.runTransaction(executeFunction, callback);
};

//======================================================================================
// Helper Functions.
//======================================================================================

/**
 * Builds the list of select fields using an array of mappings.
 * @param tableName - The table name.
 * @param mappings - The array of mapping objects.
 * @returns {string}
 *
 * {
 *  field: String,
 *  rename: String
 * }
 */
exports.createSelectFields = (tableName, mappings) => {
  let selectClause = '';

  for (let i = 0; i < mappings.length; i++) {
    if (i !== 0) {
      selectClause += ', ';
    }

    selectClause += ((tableName) ? tableName + '.' : '') + mappings[i].field;

    if (mappings[i].rename && !_.isEmpty(mappings[i].rename)) {
      selectClause += ' AS ' + mappings[i].rename;
    }
  }

  return selectClause;
};

/**
 * Builds an insert object (object with sql and params array).
 * @param tableName - The name of the table to insert into.
 * @param parameters - An object of key value pairs.
 */
exports.generateInsertObject = (tableName, parameters) => {
  let sql = `INSERT INTO ${tableName}`;

  // build the parameter lists.
  let columnNamesString = '';
  let valuePlaceHolders = '';
  let paramsArray = [];

  // keep track of the current index.
  let index = 0;

  // loop over all the keys.
  for (let key in parameters) {
    if (parameters.hasOwnProperty(key) && parameters[key] !== undefined) {
      // if its not the last parameter, add a comma.
      if (index !== 0) {
        columnNamesString += ', ';
        valuePlaceHolders += ', ';
      }

      // increment the index.
      index++;

      columnNamesString += key;
      valuePlaceHolders += '?';
      paramsArray.push(parameters[key]);
    }
  }

  // connect it to the initial statement.
  sql += ' (' + columnNamesString + ') VALUES (' + valuePlaceHolders + ')';

  // build the insert object.
  return {
    sql: sql,
    params: paramsArray
  };
};

/**
 * Builds an update object (object with sql and params array).
 * @param tableName - The name of the table.
 * @param parameters - The object of key value pairs where keys are the db column names.
 * @param conditions - The sql condition statement.
 * @param conditionParams - The array of parameters that match the conditions place markers.
 */
exports.generateUpdateObject = (tableName, parameters, conditions, conditionParams) => {
  let sql = 'UPDATE ' + tableName + ' SET ';
  let paramsArray = [];

  let index = 0;

  for (let key in parameters) {
    if (parameters.hasOwnProperty(key)) {
      if (index !== 0) {
        sql += ', ';
      }

      // increment the index.
      index++;

      // add the key to they statement.
      sql += key + ' = ? ';
      paramsArray.push(parameters[key]);
    }
  }

  // add the conditions part of the statement.
  sql += 'WHERE ' + conditions;

  // add the condition parameters part.
  for (let i = 0; i < conditionParams.length; i++) {
    paramsArray.push(conditionParams[i]);
  }

  return {
    sql: sql,
    params: paramsArray
  };
};

/**
 * Converts a value to a boolean value.
 * @param value - the value to convert.
 */
exports.booleanValue = (value) => {
  let result = false;
  try {
    if (!StringUtils.isEmpty(value)) {
      if (Buffer.isBuffer(value)) {
        result = validator.toBoolean(value[0].toString());
      }
      else {
        result = validator.toBoolean(value.toString());
      }
    }
  }
  catch (ex) {
    console.log('Failed to parse database boolean value.');
    console.log(ex);
  }

  return result;
};

/**
 * Groups related db rows into objects.
 * @param groupByField - The field that relates rows together.
 * @param rows - The list of all rows to iterate over.
 * @param callback - The finished callback function.
 */
exports.groupRows = (groupByField, rows, callback) => {
  // stores the rows as separate objects.
  // {
  //   <groupByField>: 1,
  //   rows: []
  // }
  let rawRowList = [];

  try {
    // iterate over all the rows and split them into their group objects.
    async.each(rows,
      // item processor.
      function (row, cb) {
        // get the user id field.
        let groupByFieldValue = row[groupByField];

        let props = {};
        props[groupByField] = groupByFieldValue;
        let foundObject = _.find(rawRowList, props);

        if (!foundObject) {
          foundObject = {};
          foundObject[groupByField] = groupByFieldValue;
          foundObject.rows = [];
          foundObject.rows.push(row);
          rawRowList.push(foundObject);
        }
        else {
          foundObject.rows.push(row);
        }

        return cb();
      },

      // finished callback handler.
      function (err) {
        return callback(err, rawRowList);
      }
    );
  }
  catch (err) {
    console.log(err);
    return callback(err);
  }
};

/**
 * Converts a placeholder filled sql string with the params array for debug printing.
 * @param sql - The sql statement containing placeholders.
 * @param params - The array of parameters.
 * @param timezone - The timezone.
 * @returns {string}
 */
exports.queryToString = (sql, params, timezone) => {
  let final = '';
  let paramsIndex = 0;

  for (let i = 0; i < sql.length; i++) {
    if (sql.charAt(i) === '?') {
      if (typeof params[paramsIndex] == 'string') {
        if (params[paramsIndex].lastIndexOf('[@]', 0) === 0) {
          final += StringUtils.replaceAll(params[paramsIndex], '[@]', '@');
        }
        else {
          final += "'" + StringUtils.replaceAll(params[paramsIndex], "'", "\\'") + "'";
        }
      }
      else if (Object.prototype.toString.call(params[paramsIndex]) === '[object Date]') {
        final += "'" + dateToString(params[paramsIndex], timezone) + "'";
      }
      else {
        final += params[paramsIndex];
      }

      paramsIndex++;
    }
    else {
      final += sql.charAt(i);
    }
  }

  return final;
};

/**
 * Join the result sets into 1 result set.
 * @param results - The result sets.
 */
exports.joinResultSets = (results) => {
  // check if there is anything in the results.
  if (!results || results.length === 0) {
    return results;
  }

  // if the results is an array of result sets.
  if (results[0] && _.isArray(results[0])) {
    let newResults = [];

    for (let i = 0; i < results.length; i++) {
      let rs = results[i];

      for (let j =0; j < rs.length; j++) {
        newResults.push(rs[j]);
      }
    }

    results = newResults;
  }

  return results;
};

/**
 * Converts a date to string date.
 * @param date
 * @param timeZone
 * @return {string}
 */
function dateToString(date, timeZone) {
  let dt = new Date(date);

  if (timeZone && timeZone !== 'local') {
    let tz = convertTimezone(timeZone);

    dt.setTime(dt.getTime() + (dt.getTimezoneOffset() * 60000));
    if (tz !== false) {
      dt.setTime(dt.getTime() + (tz * 60000));
    }
  }

  let year   = dt.getFullYear();
  let month  = zeroPad(dt.getMonth() + 1, 2);
  let day    = zeroPad(dt.getDate(), 2);
  let hour   = zeroPad(dt.getHours(), 2);
  let minute = zeroPad(dt.getMinutes(), 2);
  let second = zeroPad(dt.getSeconds(), 2);
  let millisecond = zeroPad(dt.getMilliseconds(), 3);

  return year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second + '.' + millisecond;
}

/**
 * Zero pads numbers.
 * @param number
 * @param length
 * @return {string}
 */
function zeroPad(number, length) {
  number = number.toString();
  while (number.length < length) {
    number = '0' + number;
  }

  return number;
}

/**
 * Converts timezones.
 * @param tz - The timezone.
 * @return {boolean|number}
 */
function convertTimezone(tz) {
  if (tz === "Z") return 0;

  let m = tz.match(/([\+\-\s])(\d\d):?(\d\d)?/);
  if (m) {
    return (m[1] === '-' ? -1 : 1) * (parseInt(m[2], 10) + ((m[3] ? parseInt(m[3], 10) : 0) / 60)) * 60;
  }
  return false;
}
