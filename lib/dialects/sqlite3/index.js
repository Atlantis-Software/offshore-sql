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

Sqlite3Dialect.prototype.joinSkipLimit = function(select, association) {
  var self = this;
  var connection = select.connection;
  var join = association.join;
  var childDefinition = connection.getCollection(join.child).definition;

  select.query.leftJoin(join.child + ' as ' + join.alias, function() {
    this.on(select.alias + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
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
  var skLmtQuery = connection.client(join.child + ' as ' + skLmtAlias).select(connection.client.raw('count(*)+1'));

  skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
  skLmtQuery.andWhereRaw( '?? = ??', [ join.parent + '.' + join.parentKey, select.alias + '.' + select.pk ] );

  new self.CriteriaProcessor(connection, skLmtAlias, join.criteria, skLmtQuery);

  if (!join.criteria.sort) {
    join.criteria.sort = {};
    join.criteria.sort[join.childKey] = 1;
  }

  var sort = _.clone(join.criteria.sort);
  sort[join.childKey] = 1;
  var keys = _.keys(sort);
  skLmtQuery.andWhere(function() {
    var self = this;
    for (var i in keys) {
      (function(i) {
        self.orWhere(function() {
          var j = 0;
          while (j < i) {
            this.andWhereRaw('??.?? = ??.??', [ join.alias, keys[j], skLmtAlias, keys[j]]);
            ++j;
          }
          var key = keys[i];
          if (sort[key] === 1) {
            this.andWhereRaw('??.?? > ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
          } else {
            this.andWhereRaw('??.?? < ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
          }
        });
      })(i);
    }
  });

  select.selection.push(skLmtQuery.as(skLmtAlias));
  select.query.select(skLmtQuery.as(skLmtAlias));

  if (join.criteria.skip && join.criteria.limit) {
    select.query.where(function(){
      this.andWhere(function() {
        this.andWhere(skLmtAlias, '>', join.criteria.skip);
        this.andWhere(skLmtAlias, '<=', join.criteria.skip + join.criteria.limit);
      });
      // do not skip parent data when there is no child
      this.orWhereNull(skLmtAlias);
      // do not skip parent data when child skip >= child count
      this.orWhere(skLmtAlias, '=', 1);
    });
    // inform cursor to skip first child data
    association.skipFirst = skLmtAlias;
  } else if (join.criteria.skip) {
    select.query.where(function(){
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      // do not skip parent data when there is no child
      this.orWhereNull(skLmtAlias);
      // do not skip parent data when child skip >= child count
      this.orWhere(skLmtAlias, '=', 1);
    });
    // inform cursor to skip first child data
    association.skipFirst = skLmtAlias;
  } else if (join.criteria.limit) {
    select.query.where(function(){
      this.andWhere(skLmtAlias, '<=', join.criteria.limit);
      // do not skip parent data when there is no child
      this.orWhereNull(skLmtAlias);
    });
  }

  if (join.criteria && join.criteria.sort) {
    _.keys(join.criteria.sort).forEach(function(toSort) {
      var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
      select.query.orderBy(join.alias + '.' + toSort, direction);
    });
  }
};

Sqlite3Dialect.prototype.joinManyToManySkipLimit = function(select, association) {
  var self = this;
  var connection = select.connection;
  var join = association.join;
  var junction = association.junction;
  var childDefinition = connection.getCollection(join.child).definition;
  var junctionAlias = this.createAlias('junction_', join.alias);

  select.query.leftJoin(function() {
    var query = this;
    join.select.forEach(function(columnName) {
      if (childDefinition[columnName]) {
        query.select(join.child + '.' + columnName);
      }
    });
    this.select(junction.child + '.' + junction.childKey + ' as ' + junctionAlias);
    new self.CriteriaProcessor(connection, join.child, join.criteria, this);

    this.from(join.child).leftJoin(join.parent, function() {
      this.on(join.parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
    }).as(join.alias);
  }, function() {
    this.on(select.alias + '.' + junction.parentKey, join.alias + '.' + junctionAlias);
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
  var skLmtQuery = connection.client(join.child + ' as ' + skLmtAlias).select(connection.client.raw('count(1)+1'));

  skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
  skLmtQuery.andWhereRaw( '?? = ??', [ junction.child + '.' + junction.childKey, select.alias + '.' + select.pk ] );

  new self.CriteriaProcessor(connection, skLmtAlias, join.criteria, skLmtQuery);

  if (!join.criteria.sort) {
    join.criteria.sort = {};
    join.criteria.sort[join.childKey] = 1;
  }

  var sort = _.clone(join.criteria.sort);
  sort[join.childKey] = 1;
  var keys = _.keys(sort);
  skLmtQuery.andWhere(function() {
    var self = this;
    for (var i in keys) {
      (function(i) {
        self.orWhere(function() {
          var j = 0;
          while (j < i) {
            this.andWhereRaw('??.?? = ??.??', [ join.alias, keys[j], skLmtAlias, keys[j]]);
            ++j;
          }
          var key = keys[i];
          if (sort[key] === 1) {
            this.andWhereRaw('??.?? > ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
          } else {
            this.andWhereRaw('??.?? < ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
          }
        });
      })(i);
    }
  });

  select.selection.push(skLmtQuery.as(skLmtAlias));
  select.query.select(skLmtQuery.as(skLmtAlias));

  if (join.criteria.skip && join.criteria.limit) {
    select.query.where(function(){
      this.andWhere(function() {
        this.andWhere(skLmtAlias, '>', join.criteria.skip);
        this.andWhere(skLmtAlias, '<=', join.criteria.skip + join.criteria.limit);
      });
      // do not skip parent data when there is no child
      this.orWhereNull(skLmtAlias);
      // do not skip parent data when child skip >= child count
      this.orWhere(skLmtAlias, '=', 1);
    });
    // inform cursor to skip first child data
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.skip) {
    select.query.where(function(){
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      // do not skip parent data when there is no child
      this.orWhereNull(skLmtAlias);
      // do not skip parent data when child skip >= child count
      this.orWhere(skLmtAlias, '=', 1);
    });
    // inform cursor to skip first child data
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.limit) {
    select.query.where(function(){
      this.andWhere(skLmtAlias, '<=', join.criteria.limit);
      // do not skip parent data when there is no child
      this.orWhereNull(skLmtAlias);
    });
  }

  if (join.criteria && join.criteria.sort) {
    _.keys(join.criteria.sort).forEach(function(toSort) {
      var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
      select.query.orderBy(join.alias + '.' + toSort, direction);
    });
  }
};
