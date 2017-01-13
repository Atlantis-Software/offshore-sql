var _ = require('lodash');
var Knex = require('knex');
var CriteriaProcessor = require('../../criteriaProcessor');
var inherits = require('inherits');

var MysqlCriteriaProcessor = function() {
  CriteriaProcessor.apply(this, arguments);
};

inherits(MysqlCriteriaProcessor, CriteriaProcessor);

function pad2(v) {
  return (v < 10 ? '0' : '') + v;
}

MysqlCriteriaProcessor.prototype.processDate = function(query, col, comparator, value) {
  value = new Date(value.getTime()  + (-value.getTimezoneOffset()) * 60*1000);
  value = value.getUTCFullYear() + '-' +
      pad2(value.getUTCMonth() + 1) + '-' +
      pad2(value.getUTCDate()) + ' ' +
      pad2(value.getUTCHours()) + ':' +
      pad2(value.getUTCMinutes()) + ':' +
      pad2(value.getUTCSeconds());

  var criterion = this.connection.client.raw('??.?? ' + comparator + ' ?', [this.tableName, col, value]);
  return query[this.clause](criterion);
};

module.exports = MysqlCriteriaProcessor;
