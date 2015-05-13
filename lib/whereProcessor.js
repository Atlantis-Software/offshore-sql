var _ = require('underscore');

var Processor = function (query, collectionName,where) {
    this.query = query;
    this.where = where;
    this.collectionName = collectionName;
    this.operators = ['lessThanOrEqual', 'greaterThanOrEqual', 'lessThan', 'greaterThan', 'not', '!'];
    this.algebricOperators = ['<=', '>=', '<', '>', '!=', '!='];
    return this;
};


Processor.prototype.process = function () {
    if (!this.where)
        return this.query;
    var self = this;
    var whereKeys = _.keys(this.where);
    whereKeys.forEach(function (key) {
        self.expand(self.query, key, self.where[key]);
    });
    return this.query;
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

            self.processElement(query, null, key, value, 'where');
            return;
    }
};

Processor.prototype.isOperator = function (key) {
    return this.algebricOperators.indexOf(key) > -1 || this.operators.indexOf(key) > -1;
};

Processor.prototype.getOperator = function (key) {
    var pos = this.algebricOperators.indexOf(key);
    if (pos > -1)
        return key;
    pos = this.operators.indexOf(key);
    if (pos > -1)
        return this.algebricOperators[pos];
    return null;
};

Processor.prototype.processElement = function (query, fatherKey, key, value, whereFct) {
    var self = this;
    whereFct = whereFct || 'where';
    if (this.isFinalValue(key, value)) {
        console.log("final value: ",value);
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
        self.knexify(query, fatherKey || key, value, comparator, whereFct);
        return;
    }
    else {
        console.log("not final value: ",value);
        console.log("not final key: ",key);
    }
    _.keys(value).forEach(function (valueKey) {
        self.processElement(query, key, valueKey, value[valueKey]);
    });
};


Processor.prototype.knexify = function (query, column, value, comparator, whereFct) {
    whereFct = whereFct || 'where';
    switch (comparator) {
        case 'contains':
            comparator = 'like';
            value = '%' + value + '%';
            break;
        case 'startsWith':
            comparator = 'like';
            value = value + '%';
            break;
        case 'endsWith':
            comparator = 'like';
            value = '%' + value;
            break;
    }
    if (_.isArray(value)) {
        if (comparator === '!=')
            comparator = 'NOT IN';
    }
    query = query[whereFct](this.collectionName+'.'+column, comparator, value);
    return;
};

Processor.prototype.like = function (query, value, whereFct) {
    whereFct = whereFct || 'where';
    var self = this;
    _.keys(value).forEach(function (key) {
        query = self.knexify(query, key, value[key], 'like', whereFct);
    });
    return query;
};

Processor.prototype.or = function (query, value) {
    var self =this;
    query = query.where(function () {
        var thisQuery = this;
        value.forEach(function (element) {
            thisQuery = thisQuery.orWhere(function () {
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


