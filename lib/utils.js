var _ = require('underscore');

module.exports = {
    prepareValues: function (values) {
        _.keys(values).forEach(function (value) {
            if (_.isUndefined(value) || value === null)
                return;

            // Cast functions to strings
            if (_.isFunction(value)) {
                value = value.toString();
            }

            // Store Arrays and Objects as strings
            if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
                value = JSON.stringify(value);
            }
        });
        return values;
    },
    normalizeCriteria: function (_criteria) {
        var criteria = _.clone(_criteria);
        if (!criteria.sum && !criteria.average && !criteria.min && !criteria.max) {
            return criteria;
        }
        if (criteria.groupBy) {
            if (!_.isArray(criteria.groupBy))
                criteria.groupBy = [criteria.groupBy];
            criteria.select = criteria.select.concat(criteria.groupBy);
        }

        if (criteria.sum) {
            if (!_.isArray(criteria.sum))
                criteria.sum = [criteria.sum];
            criteria.sum.forEach(function (key) {
                var i = criteria.sum.indexOf(key);
                criteria.sum[i] = key;
            });
        }
        if (criteria.average) {
            if (!_.isArray(criteria.average))
                criteria.average = [criteria.average];
            criteria.average.forEach(function (key) {
                var i = criteria.average.indexOf(key);
                criteria.average[i] = key;
            });
        }
        if (criteria.max) {
            if (!_.isArray(criteria.max))
                criteria.max = [criteria.max];
            criteria.max.forEach(function (key) {
                var i = criteria.max.indexOf(key);
                criteria.max[i] = key;
            });
        }
        if (criteria.min) {
            if (!_.isArray(criteria.min))
                criteria.min = [criteria.min];
            criteria.min.forEach(function (key) {
                var i = criteria.min.indexOf(key);
                criteria.min[i] = key;
            });
        }
        return criteria;
    }  
};