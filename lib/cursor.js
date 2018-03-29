var Utils = require('./utils');
var _ = require('lodash');

var Cursor = function(select) {
  this.select = select;
};

Cursor.prototype.process = function(dbResults) {
  var self = this;
  var results = [];
  var index = {};
  dbResults.forEach(function(line) {
    var parent = {};
    if (self.select.aggregates.length > 0) {
      self.select.attributes.forEach(function(attribute) {
        parent[attribute.name] = Utils.cast(attribute.type, line[attribute.alias]);
      });
      return results.push(parent);
    }

    var parentPk = line[self.select.pk];
    // check parent exist
    if (_.isNull(parentPk)) {
      return;
    }
    // create parent index and add it to results
    if (!index[parentPk]) {
      // create a parent data instance
      self.select.attributes.forEach(function(attribute) {
        var value = Utils.cast(attribute.type, line[attribute.alias]);
        parent[attribute.name] = value;
      });
      // index parent data
      index[parentPk] = {
        data: parent,
        children: {}
      };
      // add parent data to returned results
      results.push(index[parentPk].data);
    }

    // populate childs
    self.select.associations.forEach(function(child) {
      // create index if not exist
      index[parentPk].children[child.name] = index[parentPk].children[child.name] || {};

      if (child.aggregates.length > 0) {
        var aggregateData = {};
        child.attributes.forEach(function(attribute) {
          aggregateData[attribute.name] =  Utils.cast(attribute.type, line[attribute.alias]);
        });
        if (child.collection) {
          index[parentPk].data[child.name] = index[parentPk].data[child.name] || [];
          index[parentPk].data[child.name].push(aggregateData);
        } else {
          index[parentPk].data[child.name] = aggregateData;
        }
        return;
      }

      var pk = line[child.pk];

      // check child exist
      if (_.isNull(pk)) {
        if (!child.collection && _.isUndefined(index[parentPk].data[child.name])) {
          index[parentPk].data[child.name] = null;
        }
        return;
      }
      // check if child has been previously added
      if (index[parentPk].children[child.name][pk]) {
        return;
      }

      // when skipFirst alias equals 1 (only parent data should be added)
      if (child.skipFirst && (line[child.skipFirst] === 1 || line[child.skipFirst] === '1')) {
        if (!child.collection) {
          index[parentPk].data[child.name] = null;
        } else {
          index[parentPk].data[child.name] = [];
        }
        return;
      }

      // create child data
      var childData = {};
      child.attributes.forEach(function(attribute) {
        var value = Utils.cast(attribute.type, line[attribute.alias]);
        childData[attribute.name] = value;
      });

      // insert child data into parent data
      if (child.collection) {
        index[parentPk].data[child.name] = index[parentPk].data[child.name] || [];
        index[parentPk].data[child.name].push(childData);
      } else {
        index[parentPk].data[child.name] = childData;
      }

      // update index
      index[parentPk].children[child.name][pk] = childData;
    });
  });
  return results;
};

module.exports = Cursor;
