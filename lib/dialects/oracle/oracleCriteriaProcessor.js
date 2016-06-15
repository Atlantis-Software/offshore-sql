var _ = require('lodash');
var Knex = require('knex');
var CriteriaProcessor = require('../../criteriaProcessor');
var inherits = require('inherits');

var OracleCriteriaProcessor = function() {
  CriteriaProcessor.apply(this, arguments);
};

inherits(OracleCriteriaProcessor, CriteriaProcessor);

OracleCriteriaProcessor.prototype.processArray = function(query, col, comparator, value) {
  var self = this;

  var chunks = [value];
  if (value.length >= 1000) {
    chunks = _.chunk(value, 1000);
  }
  var orClause = 'orWhere';
  if (this.clause === 'on') {
    orClause = 'orOn';
  }

  if (comparator === '!=') {
    comparator = 'NOT IN';
  }
  return query[this.clause](function() {
    var element = this;
    chunks.forEach(function(chunk) {

      // Workaround for oracle array binding problem
      var inClause = ' (';
      value.forEach(function() {
        inClause += '?,';
      });
      inClause = inClause.slice(0, -1);
      inClause += ')';
      element[orClause](Knex.raw('??.?? ' + comparator + inClause, [self.tableName, col].concat(chunk)));
    });
  });
};

module.exports = OracleCriteriaProcessor;
