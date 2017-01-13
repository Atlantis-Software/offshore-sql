var Utils = require('./utils');
var _ = require('lodash');

var Cursor = function(tableName, connection, options) {
  var self = this;
  this.pk = connection.getPk(tableName);
  this.attributes = [];
  this.associations = [];

  var populated = [];
  options.joins.forEach(function(join) {
    if (join.select) {
      var childName = join.parentKey;
      if (join.collection) {
        childName = join.alias;
      }
      populated.push(join.alias);
      var association = {
        name: childName,
        pk: connection.dialect.createAlias(join.alias, connection.getPk(join.child)),
        skipFirst: join.skipFirst,
        collection: join.collection,
        attributes: [],
        aggregates: []
      };
      var definition = connection.getCollection(join.child).definition;
      join.select.forEach(function(attributeName) {
        if (definition[attributeName]) {
          var columnAlias = connection.dialect.createAlias(join.alias, attributeName);
          association.attributes.push({
            name: attributeName,
            alias: columnAlias,
            type: definition[attributeName].type
          });
        }
      });
      join.aggregate.forEach(function(attribute) {
        association.aggregates.push({
          name: attribute.name,
          alias: attribute.alias,
          type: attribute.type
        });
      });

      self.associations.push(association);
    }
  });

  definition = connection.getCollection(tableName).definition;
  var select = options.select || _.keys(definition);
  select.forEach(function(attributeName) {
    if (definition[attributeName]) {
      // if attribute is a associations model do not add pk value
      if (definition[attributeName].model && populated.indexOf(definition[attributeName].alias) >= 0) {
         return;
      }
      self.attributes.push({
        name: attributeName,
        alias: attributeName,
        type: definition[attributeName].type
      });
    }
  });
};

Cursor.prototype.process = function(dbResults) {
  var self = this;
  var results = [];
  var index = {};

  dbResults.forEach(function(line) {

    var parentPk = line[self.pk];
    // check parent exist
    if (_.isNull(parentPk)) {
      return;
    }
    // create parent index and add it to results
    if (!index[parentPk]) {
      // create a parent data instance
      var parent = {};
      self.attributes.forEach(function(attribute) {
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

    self.associations.forEach(function(child) {
      // create index if not exist
      index[parentPk].children[child.name] = index[parentPk].children[child.name] || {};

      if (child.aggregates.length) {
        var aggregateData = {};
        child.aggregates.forEach(function(aggregate) {
          aggregateData[aggregate.name] =  Utils.cast(aggregate.type, line[aggregate.alias]);
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
        return;
      }
      // check if child has been previously added
      if (index[parentPk].children[child.name][pk]) {
        return;
      }

      // when skipFirst alias equals 1 (only parent data should be added)
      if (child.skipFirst && (line[child.skipFirst] === 1 || line[child.skipFirst] === '1')) {
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
