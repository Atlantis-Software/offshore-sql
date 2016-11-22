var _ = require('lodash');

module.exports = {
  prepareValues: function (values) {
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
    });
    return values;
  },
  normalizeCriteria: function (_criteria) {
    var criteria = _.clone(_criteria);
    if (!criteria.sum && !criteria.average && !criteria.min && !criteria.max) {
      return criteria;
    }
    if (criteria.groupBy) {
      if (!_.isArray(criteria.groupBy)) {
        criteria.groupBy = [criteria.groupBy];
      }
      criteria.select = criteria.select || [];
      criteria.select = criteria.select.concat(criteria.groupBy);
    }

    if (criteria.sum) {
      if (!_.isArray(criteria.sum)) {
        criteria.sum = [criteria.sum];
      }
      criteria.sum.forEach(function (key) {
        var i = criteria.sum.indexOf(key);
        criteria.sum[i] = key;
      });
    }
    if (criteria.average) {
      if (!_.isArray(criteria.average)) {
        criteria.average = [criteria.average];
      }
      criteria.average.forEach(function (key) {
        var i = criteria.average.indexOf(key);
        criteria.average[i] = key;
      });
    }
    if (criteria.max) {
      if (!_.isArray(criteria.max)) {
        criteria.max = [criteria.max];
      }
      criteria.max.forEach(function (key) {
        var i = criteria.max.indexOf(key);
        criteria.max[i] = key;
      });
    }
    if (criteria.min) {
      if (!_.isArray(criteria.min)) {
        criteria.min = [criteria.min];
      }
      criteria.min.forEach(function (key) {
        var i = criteria.min.indexOf(key);
        criteria.min[i] = key;
      });
    }
    return criteria;
  },

  cast: function(type, value) {
    type = type.type || type;
    switch (type) {
      case 'integer':
        if (_.isString(value)) {
          if (value.indexOf('.') >= 0) {
            return parseFloat(value, 10);
          } else if(value == parseInt(value, 10)) {
            return parseInt(value, 10);
          } else {
            throw new Error('Invalid data type for : ', value);
          }
        }
        break;
      case 'boolean':
        if (_.isString(value) && (value === '0' || value === 'false')) {
          return false;
        }
        if (_.isString(value) && (value === '1' || value === 'true')) {
          return true;
        }
        break;
      case 'date':
      case 'time':
      case 'datetime':
        if(typeof value === 'number') {
          return new Date(value);
        }
        break;
    }
    return value;
  },

  castAll: function(definition, data) {
    var self = this;
    var result = _.isArray(data) ? data : [data];
    result.forEach(function(entry) {
      for (var key in entry) {
        if (definition[key]) {
          entry[key] = self.cast(definition[key], entry[key]);
        } else {
          delete entry[key];
        }
      }
    });
    return data;
  }
};
