var Utils = require('./utils');
var _ = require('lodash');

var Cursor = function(tableName, connection, joins) {
  var self = this;
  this.fieldAlias = {};
  this.pk = connection.getPk(tableName);
  this.definition = connection.getCollection(tableName).definition;
  this.connection = connection;
  joins.forEach(function(join) {
    if (join.select) {
      var childPk = connection.getPk(join.child);
      var childPkAlias = connection.dialect.createAlias(join.alias, childPk);
      var definition = connection.getCollection(join.child).definition;
      var parentKey = join.parentKey;
      if (join.collection) {
        parentKey = join.alias;
      }
      join.select.forEach(function(columnName) {
        if (definition[columnName]) {
          var childAlias = connection.dialect.createAlias(join.alias, columnName);
          self.fieldAlias[childAlias] = {
            columnName: columnName,
            childPk: childPkAlias,
            parentKey: parentKey,
            collection: join.collection,
            childName: join.alias,
            type: definition[columnName].type
          };
        }
      });
    }
  });
};

Cursor.prototype.process = function(dbResults) {
  var self = this;
  var results = [];
  var index = {};
  dbResults.forEach(function(result) {
    var childrenPk = {};
    var parentPk = result[self.pk];
    for (var field in result) {
      if (self.fieldAlias[field]) {
        var childName = self.fieldAlias[field].childName;
        var parentKey = self.fieldAlias[field].parentKey;
        var collection = self.fieldAlias[field].collection;
        var childColumnName = self.fieldAlias[field].columnName;
        var childPkName = self.fieldAlias[field].childPk;
        var childPk = result[childPkName];
        var type = self.fieldAlias[field].type;

        //create parent index and add it to results
        if (!index[parentPk]) {
          // create a parent instance
          var parent = {};
          _.keys(result).forEach(function(columnName) {
            var def = self.definition[columnName];
            if (def && !(def.model || def.collection)) {
              parent[columnName] = result[columnName];
            }
          });
          index[parentPk] = {
            data: parent,
            children: {}
          };
          results.push(index[parentPk].data);
        }

        //create association index and empty collection data
        if (!index[parentPk].children[childName]) {
          if (childPk) {
            index[parentPk].children[childName] = {};
          }
          if (collection) {
            index[parentPk].data[childName] = [];
          }
        }

        //create child index
        if (childPk && !index[parentPk].children[childName][childPk]) {
          index[parentPk].children[childName][childPk] = {};
          if (collection) {
            index[parentPk].data[parentKey].push(index[parentPk].children[childName][childPk]);
          } else if (childPk) {
            index[parentPk].data[parentKey] = index[parentPk].children[childName][childPk];
          }
        }
        //insert child column data
        if (childPk) {
          var value = Utils.cast(type,result[field]);
          index[parentPk].children[childName][childPk][childColumnName] = value;
        }
      }
    }
  });
  return results;
};

module.exports = Cursor;
