var _ = require('lodash');
var Knex = require('knex');
var CriteriaProcessor = require('../../criteriaProcessor');
var inherits = require('inherits');

var OracleCriteriaProcessor = function() {
  CriteriaProcessor.apply(this, arguments);
};

inherits(OracleCriteriaProcessor, CriteriaProcessor);

OracleCriteriaProcessor.prototype.processArray = function(query, col, comparator, value) {
  var column = Knex.raw('??.??', [this.tableName, col]);
  var criterion = Knex.raw('??.?? ' + comparator + ' (?)', [this.tableName, col, value]);
  if (comparator === '!=') {
    comparator = 'NOT IN';
  }
  if (value.length >= 1000) {
    var chunks = _.chunk(value, 1000);
    var orClause = 'orWhere';
    if (this.clause === 'on') {
      orClause = 'orOn';
    }
    return query[this.clause](function() {
      var element = this;
      chunks.forEach(function(chunk) {
        criterion = Knex.raw('??.?? ' + comparator + ' (?)', [this.tableName, col, chunk]);
        element[orClause](column, comparator, chunk);
      });
    });
  }
  return query[this.clause](criterion);
};

module.exports = OracleCriteriaProcessor;