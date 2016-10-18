var _ = require('lodash');
var LOG_QUERIES = false;

var Dialect = require('../../dialect.js');

var Sqlite3Dialect = module.exports = function(){};

Sqlite3Dialect.prototype = new Dialect();

Sqlite3Dialect.prototype.describe = function(connection, collection, callback) {
    var tableName = this.normalizeTableName(collection.tableName);
    var query = 'PRAGMA table_info(' + tableName + ')';
    var pkQuery = 'PRAGMA index_list(' + tableName + ')';

    if (LOG_QUERIES) {
        console.log('Executing Sqlite3 query :', query);
        console.log('Executing Sqlite3 query :', pkQuery);
    }
    connection.client.raw(query).then(function __DESCRIBE__(result) {
      if(result.length === 0) {
        return callback({code: 'ER_NO_SUCH_TABLE'});
      }
        connection.client.raw(pkQuery).then(function (pkResult) {
            var schema = result;
            schema.forEach(function (attr) {

                if (attr.Key === 'PRI') {
                    attr.primaryKey = true;

                    if (attr.Type === 'int(11)') {
                        attr.autoIncrement = true;
                    }
                }

                if (attr.Key === 'UNI') {
                    attr.unique = true;
                }
            });

            pkResult.forEach(function (result) {
                schema.forEach(function (attr) {
                    if (attr.Field !== result.Column_name)
                        return;
                    attr.indexed = true;
                });
            });
            callback(null,schema);
        });
    }).catch(function (e) {
            callback(e,null);
    });
};

Sqlite3Dialect.prototype.normalizeSchema = function (schema) {
        var normalized = _.reduce(schema, function (memo, field) {
            var attrName = field.name;
            var type = field.type;

            type = type.replace(/\([0-9]+\)$/, '');
            memo[attrName] = {
                type: type,
                defaultsTo: field.Default,
                autoIncrement: field.Extra === 'autoIncrement'
            };
            if (field.primaryKey) {
                memo[attrName].primaryKey = field.primaryKey;
            }
            if (field.autoIncrement) {
                memo[attrName].autoIncrement = field.autoIncrement;
            }
            if (field.unique) {
                memo[attrName].unique = field.unique;
            }
            if (field.indexed) {
                memo[attrName].indexed = field.indexed;
            }
            return memo;
        }, {});
        return normalized;
    };

Sqlite3Dialect.prototype.escapeString = function(string) {
    if (_.isUndefined(string)) {
      return null;
    }
    return this.stringDelimiter + string + this.stringDelimiter;
};

Sqlite3Dialect.prototype.joinSkipLimit = function(connection, select, join) {
  var self = this;
  var childDefinition = connection.getCollection(join.child).definition;
  var parent = join.parent;
  if (parent === select.tableName) {
    parent = select.alias;
  }
  if (join.select === false) {
    select.query.leftJoin(join.child, function() {
      this.on(parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
    });
  }
  else {
    select.query.leftJoin(join.child + ' as ' + join.alias, function() {
      this.on(parent + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
      if (join.criteria) {
        new self.CriteriaProcessor(connection, join.alias, join.criteria, this, 'on');
      }
    });
    // Add column with alias in selection
    if (join.select) {
      join.select.forEach(function(columnName) {
        if (childDefinition[columnName]) {
          var childAlias = self.createAlias(join.alias, columnName);
          var column = join.alias + '.' + columnName + ' as ' + childAlias;
          if (select.selection.indexOf(column) < 0) {
            select.selection.push(column);
            select.query.select(column);
          }
        }
      });
    }

    var skLmtAlias = this.createAlias('_SKLMT_', join.alias);
    var skLmtQuery = connection.client(join.child + ' as ' + skLmtAlias).count('*');

    if (join.junctionTable) {
      var junctionTable = _.find(select.options.joins, function(junction) {
        return (junction.select === false && junction.alias === join.alias);
      });
      if (junctionTable) {
        skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
        skLmtQuery.leftJoin(junctionTable.parent, junctionTable.parent + '.' + junctionTable.parentKey, junctionTable.child + '.' + junctionTable.childKey);
        skLmtQuery.andWhereRaw( '??.?? = ??.??', [ junctionTable.parent, junctionTable.parentKey, select.alias, select.pk ] );
      }
      else {
        console.log('error junctionTable', junctionTable.length);
      }
    }
    else {
      skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
      skLmtQuery.andWhereRaw( '?? = ??', [ join.parent + '.' + join.parentKey, select.alias + '.' + select.pk ] );
    }
    new self.CriteriaProcessor(connection, skLmtAlias, join.criteria, skLmtQuery);

    if (!join.criteria.sort) {
      join.criteria.sort = {};
      join.criteria.sort[join.childKey] = 1;
    }
    var j;
    var keys = _.keys(join.criteria.sort);
    skLmtQuery.andWhere(function() {
      for (var i in keys) {
        this.orWhere(function() {
          j = 0;
          while (j < i) {
            this.andWhereRaw('??.?? = ??.??', [ join.alias, keys[j], skLmtAlias, keys[j]]);
          }
          var key = keys[i];
          if (join.criteria.sort[key]) {
            this.andWhereRaw('??.?? > ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
          }
          else {
            this.andWhereRaw('??.?? < ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
          }
        });
      }
    });

    select.selection.push(skLmtQuery.as(skLmtAlias));
    select.query.select(skLmtQuery.as(skLmtAlias));

    if (join.criteria.skip && join.criteria.limit) {
      select.query.andWhere(skLmtAlias, '>=', join.criteria.skip);
      select.query.andWhere(skLmtAlias, '<', join.criteria.limit + join.criteria.skip);
    } else if (join.criteria.skip) {
      select.query.andWhere(skLmtAlias, '>=', join.criteria.skip);
    } else if (join.criteria.limit) {
      select.query.andWhere(skLmtAlias, '<', join.criteria.limit);
    }

    if (join.criteria && join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function(toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        select.query.orderBy(join.alias + '.' + toSort, direction);
      });
    }

  }
};