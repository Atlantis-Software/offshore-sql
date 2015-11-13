var _ = require('underscore');
var Knex = require('knex');

var OPERATORS = ['lessThanOrEqual', 'greaterThanOrEqual', 'lessThan', 'greaterThan', 'not', '!'];
var ALGEBRIC_OPERATORS = ['<=', '>=', '<', '>', '!=', '!='];
    
var Processor = function (connection, tableName, criteria, query, clause) {
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

Processor.prototype.processAggregates = function() {
    var self = this;
    if (!this.options) {
        return this;
    }
    if (this.options.sum)
        this.options.sum.forEach(function (keyToSum) {
            self.query = self.query.sum(keyToSum + ' as ' + keyToSum);
        });
    if (this.options.average)
        this.options.average.forEach(function (keyToAvg) {
            self.query = self.query.avg(keyToAvg + ' as ' + keyToAvg);
        });
    if (this.options.min)
        this.options.min.forEach(function (keyToMin) {
            self.query = self.query.min(keyToMin + ' as ' + keyToMin);
        });
    if (this.options.max)
        this.options.max.forEach(function (keyToMax) {
            self.query = self.query.max(keyToMax + ' as ' + keyToMax);
        });

    if (this.options.groupBy)
        this.options.groupBy.forEach(function (groupKey) {
            self.query = self.query.groupBy(groupKey);
        });

    if (this.options.sort) {
        _.keys(this.options.sort).forEach(function (toSort) {
            var direction = self.options.sort[toSort] === 1 ? 'ASC' : 'DESC';
            self.query = self.query.orderBy(toSort, direction);
        });
    }
    
    if (this.options.skip) {
        this.query = this.query.offset(this.options.skip);
    }
    
    if (this.options.limit) {
        this.query = this.query.limit(this.options.limit);
    }
    
    return this;
};

Processor.prototype.expand = function (query, key, value) {
    var self = this;
    switch (key.toLowerCase()) {
        case 'or':
            self.or(query, value);
            return;

        case 'like':
            self.like(query, value);
            return;

        default:

            if (_.isArray(value)) {
                self.knexify(query, key, value, 'in');
                return;
            }

            self.processElement(query, null, key, value);
            return;
    }
};

Processor.prototype.isOperator = function (key) {
    return ALGEBRIC_OPERATORS.indexOf(key) > -1 || OPERATORS.indexOf(key) > -1;
};

Processor.prototype.getOperator = function (key) {
    var pos = ALGEBRIC_OPERATORS.indexOf(key);
    if (pos > -1)
        return key;
    pos = OPERATORS.indexOf(key);
    if (pos > -1)
        return ALGEBRIC_OPERATORS[pos];
    return null;
};

Processor.prototype.processElement = function (query, fatherKey, key, value) {
    var self = this;
    if (this.isFinalValue(key, value)) {
        //console.log("final value: ",value);
        //var comparator = fatherKey? self.getOperator(key):'=';
        var comparator;
        if (!fatherKey) {
            comparator = '=';
        }
        else {
            comparator = self.getOperator(key);
            if (!comparator) {
                comparator = key; // pass the comparator as it was (it will be treated insied knexify()
            }
        }
        self.knexify(query, fatherKey || key, value, comparator);
        return;
    }
    else {
        //console.log("not final value: ",value);
        //console.log("not final key: ",key);
    }
    _.keys(value).forEach(function (valueKey) {
        self.processElement(query, key, valueKey, value[valueKey]);
    });
};


Processor.prototype.knexify = function (query, col, val, comparator) {
    var value = val;
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
    }
    if (_.isArray(value)) {
        if (comparator === '!=')
            comparator = 'NOT IN';
    }
    if (_.isString(val)) {
        var dialect = this.connection.dialect;
        
        if (this.clause === 'on') {
            return query.on(Knex.raw('LOWER(' + dialect.sqlEscapeTableName(this.tableName)  + '.' + dialect.sqlEscapeColumnName(col) + ') ' + comparator + ' ' + value.toLowerCase()));
        }
        else {
            return query.whereRaw('LOWER(' + dialect.sqlEscapeTableName(this.tableName)  + '.' + dialect.sqlEscapeColumnName(col) + ') ' + comparator + ' ?', value.toLowerCase());
        }
    }
    //query.where(Knex.raw('lower(email)="' + Knex.escape(email.toLowerCase())+'"'))
    
    query[this.clause](this.tableName + '.' + col, comparator, value);
    return;
};

Processor.prototype.like = function (query, value) {
    var self = this;
    _.keys(value).forEach(function (key) {
        query = self.knexify(query, key, value[key], 'like', self.clause);
    });
    return query;
};

Processor.prototype.or = function (query, value) {
    var self = this;
    var orClause = 'or' + self.clause.charAt(0).toUpperCase() + self.clause.substring(1).toLowerCase();
    
    query = query[self.clause](function () {
        var thisQuery = this;
        value.forEach(function (element) {
            thisQuery = thisQuery[orClause](function () {
                var elementQuery = this;
                _.keys(element).forEach(function (key) {
                    self.expand(elementQuery,key, element[key]);
                });
            });
        });
    });
};

Processor.prototype.isFinalValue = function (key, value) {
    if (this.isOperator(key)) console.log("ISOP:",key);
    return this.isOperator(key) || _.isDate(value) || !_.isObject(value);
};

module.exports = Processor;