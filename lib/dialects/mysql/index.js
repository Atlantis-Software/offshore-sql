var _ = require('lodash');
var LOG_QUERIES = false;

var Dialect = require('../../dialect.js');

var MysqlDialect = module.exports = function(){};

MysqlDialect.prototype = new Dialect();

MysqlDialect.prototype.describe = function(connection, collection, callback) {
    var tableName = this.normalizeTableName(collection.tableName);
    var query = 'DESCRIBE ' + tableName;
    var pkQuery = 'SHOW INDEX FROM ' + tableName;

    if (LOG_QUERIES) {
        console.log('Executing MySQL query :', query);
        console.log('Executing MySQL query :', pkQuery);
    }
    connection.client.raw(query).then(function __DESCRIBE__(result) {
        connection.client.raw(pkQuery).then(function (pkResult) {
            var schema = result[0];
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

MysqlDialect.prototype.normalizeSchema = function (schema) {
        var normalized = _.reduce(schema, function (memo, field) {

            var attrName = field.Field;
            var type = field.Type;

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

MysqlDialect.prototype.escapeString = function(string) {
    if (_.isUndefined(string)) {
      return null;
    }
    return this.stringDelimiter + string + this.stringDelimiter;
};

MysqlDialect.prototype.joinSkipLimit = function(connection, select, join) {
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
  } else {
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

    if (!join.criteria.sort) {
      join.criteria.sort = {};
      join.criteria.sort[join.childKey] = 1;
    }

    if (join.criteria && join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function(toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        select.query.orderBy(join.alias + '.' + toSort, direction);
      });
    }

    // Subquery 'order by' needs a limit or the parser ignores it (0xefffffffffffffff is around max javascript parseInt() number)
    select.query.limit(0xefffffffffffffff);

    if (join.junctionTable) {
      var junctionTable = _.find(select.options.joins, function(junction) {
        return (junction.select === false && junction.alias === join.alias);
      });
      if (junctionTable) {
        select.query = this._getDenseRank(connection, select.query, [junctionTable.parentKey, this.createAlias(join.alias, connection.getPk(join.child))]);
      } else {
        console.log('error junctionTable', junctionTable.length);
      }
    } else {
      select.query = this._getDenseRank(connection, select.query, [join.parentKey, this.createAlias(join.alias, connection.getPk(join.child))]);
    }
    select.query = connection.client().select('*').from(select.query);

    select.query.orHaving(function() {
      if (join.criteria.skip && join.criteria.limit) {
        this.andWhere('denseRankQuery.denseRank', '>', join.criteria.skip);
        this.andWhere('denseRankQuery.denseRank', '<=', join.criteria.limit + join.criteria.skip);
      } else if (join.criteria.skip) {
        this.andWhere('denseRankQuery.denseRank', '>', join.criteria.skip);
      } else if (join.criteria.limit) {
        this.andWhere('denseRankQuery.denseRank', '<=', join.criteria.limit);
      }
    });
    select.query.orHavingRaw('??.?? is null', ['denseRankQuery', this.createAlias(join.alias, connection.getPk(join.child))] );
  }
};

MysqlDialect.prototype._getDenseRank = function(connection, query, partitionBy) {
  /* select *, @rank := if(@previous_id=`selectQuery`.`id`, if(@previous_child_id=`selectQuery`.`child_id`, @rank, @rank+1),1) as denseRank, @previous_id := `selectQuery`.`id`, @previous_child_id := `selectQuery`.`child_id`
   *
   * (SELECT @rank := 1, @previous_id := '', @previous_child_id := '') as varDeclaration)
   * */

  // incDenseRank contains strings like '@rank := if(@previous_id=`selectQuery`.`id`, if(@previous_child_id=`selectQuery`.`child_id`, @rank, @rank+1),1) as denseRank'
  // it checks if any of the values have changed, and, if they have, increment rank
  var incDenseRank = '@rank := ';

  // updateVar contains strings like '@previous_id := `selectQuery`.`id`, @previous_child_id := `selectQuery`.`child_id`'
  // it simply assign variable values for each new row
  var updateVar = '';

  // updateVar contains strings like '(SELECT @rank := 1, @previous_id := '', @previous_child_id := '') as varDeclaration)'
  // it's used for inline variable initialization (we can't use "set" as Knex doesn't support it yet)
  var initVar = '(SELECT @rank := 1, ';

  // For each given column, create its corresponding variable and add it to all three parts
  partitionBy.forEach(function(partition) {
    var previousVar = '@previous_' + partition;
    incDenseRank += 'if(' + previousVar + '=`selectQuery`.`' + partition + '`, ';
    updateVar += previousVar + ' := `selectQuery`.`' + partition + '`, ';
    initVar += previousVar + ' := \'\', ';
  });

  // Add last arguments to all 'if'
  incDenseRank += '@rank, @rank+1)';
  for(var i = 0; i < partitionBy.length-1; i++) {
    incDenseRank += ',1)';
  }
  incDenseRank += ' as denseRank, ';

  // Remove trailing space and comma
  initVar = initVar.slice(0, -2);
  updateVar = updateVar.slice(0, -2);

  initVar += ') as varDeclaration';
  return connection.client().select('*', connection.client.raw(incDenseRank + updateVar)).from(query.as('selectQuery')).join(connection.client.raw(initVar)).as('denseRankQuery');
};
