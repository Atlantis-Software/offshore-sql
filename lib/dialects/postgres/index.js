var _ = require('lodash');
var LOG_QUERIES = false;

var Dialect = require('../../dialect.js');

var PostgresDialect = module.exports = function() {};

PostgresDialect.prototype = new Dialect();

PostgresDialect.prototype.describe = function(connection, collection, callback) {
  var tableName = this.normalizeTableName(collection.tableName);
  var query = "select column_name, data_type, character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name = '" + tableName + "';";
  var pkQuery = "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type \
FROM   pg_index i \
JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
WHERE  i.indrelid = '" + tableName + "'::regclass \
AND    i.indisprimary;";

  if (LOG_QUERIES) {
    console.log('Executing Postgres query :', query);
    console.log('Executing Postgres query :', pkQuery);
  }
  connection.client.raw(query).then(function __DESCRIBE__(result) {
    if (result.rows.length === 0) {
      return callback({code: 'ER_NO_SUCH_TABLE'});
    }
    connection.client.raw(pkQuery).then(function(pkResult) {
      pkResult = pkResult.rows;
      var schema = result.rows;
      schema.forEach(function(attr) {

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

      pkResult.forEach(function(result) {
        schema.forEach(function(attr) {
          if (attr.Field !== result.Column_name)
            return;
          attr.indexed = true;
        });
      });
      callback(null, schema);
    }).catch(function(e) {
      if (e && e.code && e.code === '42P01') {
        return callback({code: 'ER_NO_SUCH_TABLE'});
      }
      callback(e, null);
    });
  }).catch(function(e) {
    if (e && e.code && e.code === '42P01') {
      return callback({code: 'ER_NO_SUCH_TABLE'});
    }
    callback(e, null);
  });
};

PostgresDialect.prototype.normalizeSchema = function(schema) {
  var normalized = _.reduce(schema, function(memo, field) {
    var attrName = field.column_name;
    var type = field.data_type;

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

PostgresDialect.prototype.escapeString = function(string) {
  if (_.isUndefined(string)) {
    return null;
  }
  return this.stringDelimiter + string + this.stringDelimiter;
};

PostgresDialect.prototype.afterSelect = function(connection, select) {
  if (!select.skipLimitQuery) {
    return;
  }
  select.query = select.skipLimitQuery.from(select.query.as('SKLMT'));
};

PostgresDialect.prototype.selectSkipLimit = function(connection, select) {
  if (select.options.skip || select.options.limit) {
    if (!select.skipLimitQuery) {
      select.skipLimitQuery = connection.client.select('SKLMT.*');
    }

    var rownum = connection.client.raw('(dense_rank() over (order by ??)) as "row_PARENT"',[select.alias + '.' + select.pk]);
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

PostgresDialect.prototype.joinSkipLimit = function(connection, select, join) {
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

    var rownum = connection.client.raw('(dense_rank() over (partition by ??.?? ' + sort + ')) as ??',[select.alias, select.pk, sklmtAlias]);
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