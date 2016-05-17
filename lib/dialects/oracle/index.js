var asynk = require('asynk');
var _ = require('lodash');
var crypto = require('crypto');
var Dialect = require('../../dialect.js');
var OracleCriteriaProcessor = require('./oracleCriteriaProcessor');

var OracleDialect = module.exports = function () {
};

OracleDialect.prototype = new Dialect();

OracleDialect.prototype.CriteriaProcessor = OracleCriteriaProcessor;

OracleDialect.prototype.describe = function (connection, collection, callback) {

    var tableName = this.normalizeTableName(collection.tableName); 
    var queries = [];
    queries[0] = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
    queries[1] = "SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE table_name = '" + tableName + "'";
    queries[2] = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner "
            + "FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '" + tableName
            + "' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner "
            + "ORDER BY cols.table_name, cols.position";

    asynk.each(queries, function (query, nextQuery) {
        connection.client.raw(query).asCallback(nextQuery);
    }).serie().done(function (results) {
      var schema = results[0];
      var indexes = results[1];
      var tablePrimaryKeys = results[2];
      if (schema.length === 0) {
        return callback({code: 'ER_NO_SUCH_TABLE', message: 'Table ' + tableName + ' doesn\'t exist.'}, null);
      }
      // Loop through Schema and attach extra attributes
      schema.forEach(function (attribute) {
          tablePrimaryKeys.forEach(function (pk) {
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
      indexes.forEach(function (index) {
          schema.forEach(function (attribute) {
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

    var rownum = connection.client.raw('(dense_rank() over (order by ??)) "row_PARENT"',[select.alias + '.' + select.pk]);
    select.query.select(rownum);

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

OracleDialect.prototype.joinSkipLimit = function(connection, select, join) {
  if (join.criteria.skip || join.criteria.limit) {
    if (!select.skipLimitQuery) {
      select.skipLimitQuery = connection.client.select('SKLMT.*');
    }

    var skip = join.criteria.skip;
    var limit = join.criteria.limit;
    delete join.criteria.skip;
    delete join.criteria.limit;
    this.join(connection, select, join);
    var sort = '';
    if (!join.criteria.sort) {
      var pk = connection.getPk(join.child);
      join.criteria.sort = {};
      join.criteria.sort[pk] = 1;
    }
    if (join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function (toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        if (sort !== '') {
          sort += connection.client.raw(', ?? ' + direction,[join.alias + '.' + toSort]).toString();
        }
        else {
          sort = connection.client.raw('order by ?? ' + direction,[join.alias + '.' + toSort]).toString();
        }
      });
    }
    var sklmtAlias = this.createAlias('_SKLMT_',join.alias);

    var rownum = connection.client.raw('(dense_rank() over (partition by ??.?? ' + sort + ')) ??',[select.alias, select.pk, sklmtAlias]);
    select.query.select(rownum);

    if (skip && limit) {
      select.skipLimitQuery.where(function(){
        this.andWhere('SKLMT.' + sklmtAlias, '>', skip);
        this.andWhere('SKLMT.' + sklmtAlias, '<=', skip + limit);
      });
    } else if (skip) {
      select.skipLimitQuery.where('SKLMT.' + sklmtAlias, '>', skip);
    } else if (limit) {
      select.skipLimitQuery.where('SKLMT.' + sklmtAlias, '<=', limit);
    }
    select.skipLimitQuery.orWhereNull('SKLMT.' + sklmtAlias);
  }
};