var _ = require('underscore');
var Knex = require('knex');

var OPERATORS = ['lessThanOrEqual', 'greaterThanOrEqual', 'lessThan', 'greaterThan', 'not', '!'];
var ALGEBRIC_OPERATORS = ['<=', '>=', '<', '>', '!=', '!='];

var Processor = function(connection, tableName, criteria, query, clause) {
  this.connection = connection;
  this.tableName = tableName;
  this.query = query;
  this.options = criteria;
  this.where = criteria.where;
  this.clause = clause || 'where';

  if (this.where) {
    var self = this;
    _.keys(this.where).forEach(function (key) {
      self.expand(self.query, key, self.where[key]);
    });
  }

  //return this;
};

Processor.prototype.getQuery = function() {
  return this.query;
};

Processor.prototype.expand = function(query, key, value) {
  var self = this;
  switch (key.toLowerCase()) {
    case 'or':
      self.or(query, value);
      break;

    case 'like':
      self.like(query, value);
      break;

    case 'and':
      self.and(query, value);
      break;

    default:
      if (_.isArray(value)) {
        self.knexify(query, key, value, 'in');
      } else {
        self.processElement(query, null, key, value);
      }
      break;
  }
};

Processor.prototype.isOperator = function(key) {
  return ALGEBRIC_OPERATORS.indexOf(key) > -1 || OPERATORS.indexOf(key) > -1;
};

Processor.prototype.getOperator = function(key) {
  var pos = ALGEBRIC_OPERATORS.indexOf(key);
  if (pos > -1)
    return key;
  pos = OPERATORS.indexOf(key);
  if (pos > -1)
    return ALGEBRIC_OPERATORS[pos];
  return null;
};

Processor.prototype.processElement = function(query, fatherKey, key, value) {
  var self = this;
  if (this.isFinalValue(key, value)) {
    var comparator;
    if (!fatherKey) {
      comparator = '=';
    } else {
      comparator = self.getOperator(key) || key; // pass the comparator as it was (it will be treated insied knexify()
    }
    return self.knexify(query, fatherKey || key, value, comparator);
  }
  _.keys(value).forEach(function(valueKey) {
    self.processElement(query, key, valueKey, value[valueKey]);
  });
};


Processor.prototype.knexify = function(query, col, val, comparator) {
  var value = val;
  var dialect = this.connection.dialect;

  switch (comparator) {
    case 'contains':
      comparator = 'LIKE';
      value = '%' + value + '%';
      break;
    case 'startsWith':
      comparator = 'LIKE';
      value = value + '%';
      break;
    case 'endsWith':
      comparator = 'LIKE';
      value = '%' + value;
      break;
    case 'or':
      return this.or(query, value);
      break;
    case 'and':
      return this.and(query, value);
      break;
  }

  if (_.isArray(value)) {
    if (comparator === '!=') {
      comparator = 'NOT IN';
    }
  }
  if (_.isString(val)) {
    if (this.clause === 'on') {
      return query.on(Knex.raw('LOWER( ??.?? ) ' + comparator + ' ' + value.toLowerCase()), [this.tableName, col]);
    }
    else {
      return query.whereRaw('LOWER( ??.?? ) ' + comparator + ' ?', [this.tableName, col, value.toLowerCase()]);
    }
  }
  if (_.isNull(value)) {
    if (comparator === '!=') {
      return query.whereNotNull(Knex.raw('??.??',[this.tableName, col]));
    }
    return query.whereNull(Knex.raw('??.??',[this.tableName, col]));
  }
  //query.where(Knex.raw('lower(email)="' + Knex.escape(email.toLowerCase())+'"'))

  query[this.clause](this.tableName + '.' + col, comparator, value);
  return;
};

Processor.prototype.like = function(query, value) {
  var self = this;
  _.keys(value).forEach(function (key) {
    query = self.knexify(query, key, value[key], 'like', self.clause);
  });
  return query;
};

Processor.prototype.or = function(query, value) {
  var self = this;
  var orClause = 'or' + self.clause.charAt(0).toUpperCase() + self.clause.substring(1).toLowerCase();

  query = query[self.clause](function() {
    var thisQuery = this;
    value.forEach(function(element) {
      thisQuery = thisQuery[orClause](function() {
        var elementQuery = this;
        _.keys(element).forEach(function(key) {
          self.expand(elementQuery, key, element[key]);
        });
      });
    });
  });
};

Processor.prototype.and = function(query, value) {
  var self = this;
  var orClause = 'and' + self.clause.charAt(0).toUpperCase() + self.clause.substring(1).toLowerCase();

  query = query[self.clause](function() {
    var thisQuery = this;
    value.forEach(function(element) {
      thisQuery = thisQuery[orClause](function() {
        var elementQuery = this;
        _.keys(element).forEach(function(key) {
          self.expand(elementQuery, key, element[key]);
        });
      });
    });
  });
};

Processor.prototype.isFinalValue = function(key, value) {
  return this.isOperator(key) || _.isDate(value) || !_.isObject(value);
};

module.exports = Processor;
