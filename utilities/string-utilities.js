'use strict';

// module dependencies.
const _ = require('lodash');

/**
 * Checks if a value is undefined, null or empty.
 * @param value - the value to check.
 * @param [ignoreWhiteSpace] - Ignores white space when checking.
 * @returns {*} - if it is empty it returns true, else returns false.
 */
exports.isEmpty = (value, ignoreWhiteSpace) => {
  let stringValue = '';

  if (!_.isUndefined(value) && !_.isNull(value)) {
    stringValue = value.toString();
    if (ignoreWhiteSpace) {
      stringValue = stringValue.trim();
    }
  }

  return (_.isEmpty(stringValue));
};

/**
 * Replaces all matched values of a string.
 * @param stringValue - the full string value to perform the replace all on.
 * @param valueToReplace - The value that is being replaced.
 * @param replaceWith - The value to replace with.
 */
exports.replaceAll = (stringValue, valueToReplace, replaceWith) => {
  return stringValue.replace(new RegExp(escapeMetaCharacters(valueToReplace), 'g'), replaceWith);
};

/**
 * Escapes special characters to be used in a regex statement.
 * @param string
 * @returns {string|void}
 */
function escapeMetaCharacters(string) {
  return string.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
}
