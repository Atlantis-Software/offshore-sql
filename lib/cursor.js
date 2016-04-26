var Utils = require('./utils');

var Cursor = function(tableName, connection, joins) {
  var self = this;
  this.fieldAlias = {};
  this.pk = connection.getPk(tableName);
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
          self.fieldAlias[childAlias] = {columnName: columnName, childPk: childPkAlias, parentKey: parentKey, collection: join.collection, childName: join.alias, type: definition[columnName].type};
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
        var type = self.fieldAlias[field].type;

        //first time before result child pk is delete
        if (! childrenPk[childName]) {
          childrenPk[childName] = result[childPkName];
        }
        var childPk = childrenPk[childName];


        //create parent index and add it to results
        if (!index[parentPk]) {
          index[parentPk] = {
            data: result,
            children: {}
          };
          results.push(index[parentPk].data);
        }

        //create association index and empty collection data
        if (! index[parentPk].children[childName]) {
          index[parentPk].children[childName] = {};
          if (collection) {
            index[parentPk].data[childName] = [];
          }
        }

        //create child index
        if (childPk && ! index[parentPk].children[childName][childPk]) {
          index[parentPk].children[childName][childPk] = {};
          if (collection) {
            index[parentPk].data[parentKey].push(index[parentPk].children[childName][childPk]);
          }
          else {
            index[parentPk].data[parentKey] = index[parentPk].children[childName][childPk];
          }
        }
        //insert child column data
        if (childPk) {
          var value = Utils.cast(type,result[field]);
          index[parentPk].children[childName][childPk][childColumnName] = value;
        }
        if (collection && parentKey === field) {
          continue;
        }
        if (result[field] === null) {
          delete result[field];
        }
        if (! collection && field !== parentKey) {
          delete result[field];
        }
        continue;
      }
    }
  });
  return results;
};

module.exports = Cursor;