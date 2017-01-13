var _ = require('lodash');
var Utils = require('../../utils');

function pad2(v) {
  return (v < 10 ? '0' : '') + v;
}

var PostgresUtils = _.clone(Utils);

PostgresUtils.prepareValues = function (values) {
  _.keys(values).forEach(function (key) {

    if (_.isUndefined(values[key]) || values[key] === null)
      return;

    // Cast functions to strings
    if (_.isFunction(values[key])) {
      values[key] = values[key].toString();
    }

    // Store Arrays and Objects as strings
    if (Array.isArray(values[key]) || (_.isObject(values[key]) && !_.isDate(values[key]) && !Buffer.isBuffer(values[key]))) {
      values[key] = JSON.stringify(values[key]);
    }

    if (_.isDate(values[key])) {
      var value = values[key];
      value = new Date(value.getTime()  + (-value.getTimezoneOffset()) * 60*1000);

      value = value.getUTCFullYear() + '-' +
          pad2(value.getUTCMonth() + 1) + '-' +
          pad2(value.getUTCDate()) + ' ' +
          pad2(value.getUTCHours()) + ':' +
          pad2(value.getUTCMinutes()) + ':' +
          pad2(value.getUTCSeconds());

      values[key] = value;
    }
  });
  return values;
};

module.exports = PostgresUtils;
