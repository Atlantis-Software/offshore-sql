var asynk = require('asynk');
var _ = require('lodash');
var crypto = require('crypto');
var Dialect = require('../../dialect.js');
var OracleCriteriaProcessor = require('./oracleCriteriaProcessor');

var OracleDialect = module.exports = function () {
};

OracleDialect.prototype = new Dialect();

OracleDialect.prototype.CriteriaProcessor = OracleCriteriaProcessor;

OracleDialect.prototype.describe = function(connection, collection, callback) {

  var tableName = this.normalizeTableName(collection.tableName);
  var queries = [];

  queries[0] = connection.client.select('COLUMN_NAME', 'DATA_TYPE', 'NULLABLE').from('USER_TAB_COLUMNS').where('TABLE_NAME', tableName);
  queries[1] = connection.client.select('INDEX_NAME', 'COLUMN_NAME').from('USER_IND_COLUMNS').where('TABLE_NAME', tableName);
  queries[2] = connection.client.select('cols.TABLE_NAME', 'cols.COLUMN_NAME', 'cols.POSITION', 'cons.STATUS', 'cons.OWNER')
          .from('ALL_CONSTRAINTS AS cons').leftJoin('ALL_CONS_COLUMNS AS cols', 'cols.CONSTRAINT_NAME', 'cons.CONSTRAINT_NAME')
          .where({'cols.TABLE_NAME': tableName, 'cons.CONSTRAINT_TYPE': 'P'});
  connection.client.count().from('USER_SEQUENCES').where('SEQUENCE_NAME', tableName.toLowerCase() + '_seq').then(function(count) {

    if (count[0]['COUNT(*)']) {
      queries[3] = connection.client.raw('SELECT "' + tableName.toLowerCase() + '_seq".nextval FROM DUAL');
    }

    asynk.each(queries, function(query, nextQuery) {
      query.asCallback(nextQuery);
    }).serie().done(function(results) {
      var schema = results[0];
      var indexes = results[1];
      var tablePrimaryKeys = results[2];
      if (schema.length === 0) {
        return callback({code: 'ER_NO_SUCH_TABLE', message: 'Table ' + tableName + ' doesn\'t exist.'}, null);
      }
      // Loop through Schema and attach extra attributes
      schema.forEach(function(attribute) {
        tablePrimaryKeys.forEach(function(pk) {
          // Set Primary Key Attribute
          if (attribute.COLUMN_NAME === pk.COLUMN_NAME) {
            attribute.primaryKey = true;
            // If also a number set auto increment attribute
            if (attribute.DATA_TYPE === 'NUMBER') {
              attribute.autoIncrement = true;
            }
          }
        });
        // Set Unique Attribute
        if (attribute.NULLABLE === 'N') {
          attribute.required = true;
        }

      });
      // Loop Through Indexes and Add Properties
      indexes.forEach(function(index) {
        schema.forEach(function(attribute) {
          if (attribute.COLUMN_NAME === index.COLUMN_NAME)
          {
            attribute.indexed = true;
          }
        });
      });
      //console.log("describe schema: ",schema);
      callback(null, schema);
    }).fail(function(err) {
      return callback(err, null);
    });
  });
};

OracleDialect.prototype.normalizeSchema = function (schema, definition) {
    var normalized = _.reduce(_.clone(schema), function (memo, field) {

        var attrName = field.COLUMN_NAME;
        var type = field.DATA_TYPE;

        // Remove (n) column-size indicators
        type = type.replace(/\([0-9]+\)$/, '');

        memo[attrName] = {
            type: type
        };

        if (field.primaryKey) {
            memo[attrName].primaryKey = field.primaryKey;
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


OracleDialect.prototype.sqlEscapeString = function (string) {
    if (_.isUndefined(string))
        return null;
    return this.stringDelimiter + string + this.stringDelimiter;
};

OracleDialect.prototype.normalizeTableName = function (tableName) {
    if (tableName.length < 30) {
        return tableName;
    }

    return crypto.createHash('sha1').update(tableName).digest('base64').replace('=', '');
};

OracleDialect.prototype.createAlias = function(tableAlias,columnName) {
    var alias = tableAlias + '_' + columnName;
    if(!columnName) {
      alias = tableAlias;
    }
    if (alias.length > 30) {
        return crypto.createHash('sha1').update(alias).digest('base64').replace('=', '');
    }
    return alias;
};

OracleDialect.prototype.afterSelect = function(connection, select) {
  if (!select.skipLimitQuery) {
    return;
  }
  select.query = select.skipLimitQuery.from(select.query.as('SKLMT'));
};

OracleDialect.prototype.selectSkipLimit = function(connection, select) {
  if (select.options.skip || select.options.limit) {
    if (!select.skipLimitQuery) {
      select.skipLimitQuery = connection.client.select('SKLMT.*');
    }

    var denseRank = connection.client.raw('(dense_rank() over (order by ?? ASC)) "row_PARENT"',[select.alias + '.' + select.pk]);

    if (select.options.sort) {
      var sort = '';
      _.keys(select.options.sort).forEach(function (toSort) {
        var direction = select.options.sort[toSort] === 1 ? 'ASC' : 'DESC';
        if (sort !== '') {
          sort += connection.client.raw(', ?? ' + direction, [select.alias + '.' + toSort]).toString();
        } else {
          sort = connection.client.raw('order by ?? ' + direction, [select.alias + '.' + toSort]).toString();
        }
      });
      denseRank = connection.client.raw('(dense_rank() over (' + sort + ')) "row_PARENT"');
    }

    select.query.select(denseRank);

    if (select.options.skip && select.options.limit) {
      select.skipLimitQuery.where('SKLMT.row_PARENT','>', select.options.skip);
      select.skipLimitQuery.where('SKLMT.row_PARENT','<=', select.options.limit + select.options.skip);
    } else if (select.options.skip) {
      select.skipLimitQuery.where('SKLMT.row_PARENT','>', select.options.skip);
    } else if (select.options.limit) {
      select.skipLimitQuery.where('SKLMT.row_PARENT','<=', select.options.limit);
    }
  }
};

OracleDialect.prototype.joinSkipLimit = function(select, association) {
  var connection = select.connection;
  var join = association.join;
  if (join.criteria.skip || join.criteria.limit) {
    if (!select.skipLimitQuery) {
      select.skipLimitQuery = connection.client.select('SKLMT.*');
    }

    var skip = join.criteria.skip;
    var limit = join.criteria.limit;
    delete join.criteria.skip;
    delete join.criteria.limit;
    this.join(select, association);
    join.criteria.skip = skip;
    join.criteria.limit = limit;
    var sort = '';
    if (!join.criteria.sort) {
      var pk = connection.getPk(join.child);
      join.criteria.sort = {};
      join.criteria.sort[pk] = 1;
    }
    if (join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function (toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        select.query.orderBy(join.alias + '.' + toSort, direction);
        if (sort !== '') {
          sort += connection.client.raw(', ?? ' + direction,[join.alias + '.' + toSort]).toString();
        } else {
          sort = connection.client.raw('order by ?? ' + direction,[join.alias + '.' + toSort]).toString();
        }
      });
    }
    var sklmtAlias = this.createAlias('_SKLMT_',join.alias);

    var denseRankQuery = this._getDenseRank(connection, sort, select.alias, select.pk, sklmtAlias);
    select.query.select(denseRankQuery);

    if (skip && limit) {
      select.skipLimitQuery.where(function(){
        this.andWhere(function() {
          this.andWhere('SKLMT.' + sklmtAlias, '>', skip);
          this.andWhere('SKLMT.' + sklmtAlias, '<=', skip + limit);
        });
        // do not skip parent data when child skip >= child count
        this.orWhere('SKLMT.' + sklmtAlias, '=', 1);
      });
      // inform cursor to skip first child data
      association.skipFirst = sklmtAlias;
    } else if (skip) {
      select.skipLimitQuery.where(function(){
        this.andWhere('SKLMT.' + sklmtAlias, '>', skip);
        // do not skip parent data when child skip >= child count
        this.orWhere('SKLMT.' + sklmtAlias, '=', 1);
      });
      // inform cursor to skip first child data
      association.skipFirst = sklmtAlias;
    } else if (limit) {
      select.skipLimitQuery.where(function(){
        this.andWhere('SKLMT.' + sklmtAlias, '<=', limit);
      });
    }
  }
};

OracleDialect.prototype.joinManyToManySkipLimit = function(select, association) {
  var connection = select.connection;
  var join = association.join;
  var junction = association.junction;
  if (join.criteria.skip || join.criteria.limit) {
    if (!select.skipLimitQuery) {
      select.skipLimitQuery = connection.client.select('SKLMT.*');
    }
    var self = this;
    var childDefinition = connection.getCollection(join.child).definition;
    var junctionAlias = this.createAlias('junction_', join.alias);
    var skip = join.criteria.skip;
    var limit = join.criteria.limit;
    var sort = '';

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

    join.select.forEach(function(columnName) {
      var childAlias = self.createAlias(association.name, columnName);
      select.query.select(join.alias + '.' + columnName + ' as ' + childAlias);
    });

    if (!join.criteria.sort) {
      var pk = connection.getPk(join.child);
      join.criteria.sort = {};
      join.criteria.sort[pk] = 1;
    }
    if (join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function (toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        select.query.orderBy(join.alias + '.' + toSort, direction);
        if (sort !== '') {
          sort += connection.client.raw(', ?? ' + direction,[join.alias + '.' + toSort]).toString();
        } else {
          sort = connection.client.raw('order by ?? ' + direction,[join.alias + '.' + toSort]).toString();
        }
      });
    }
    var sklmtAlias = this.createAlias('_SKLMT_',join.alias);

    var denseRankQuery = this._getDenseRank(connection, sort, select.alias, select.pk, sklmtAlias);
    select.query.select(denseRankQuery);


    if (skip && limit) {
      select.skipLimitQuery.where(function(){
        this.andWhere(function() {
          this.andWhere('SKLMT.' + sklmtAlias, '>', skip);
          this.andWhere('SKLMT.' + sklmtAlias, '<=', skip + limit);
        });
        // do not skip parent data when child skip >= child count
        this.orWhere('SKLMT.' + sklmtAlias, '=', 1);
      });
      // inform cursor to skip first child data
      association.skipFirst = sklmtAlias;
    } else if (skip) {
      select.skipLimitQuery.where(function(){
        this.andWhere('SKLMT.' + sklmtAlias, '>', skip);
        // do not skip parent data when child skip >= child count
        this.orWhere('SKLMT.' + sklmtAlias, '=', 1);
      });
      // inform cursor to skip first child data
      association.skipFirst = sklmtAlias;
    } else if (limit) {
      select.skipLimitQuery.where(function(){
        this.andWhere('SKLMT.' + sklmtAlias, '<=', limit);
      });
    }
  }
};
