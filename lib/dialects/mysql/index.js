var _ = require('lodash');
var crypto = require('crypto');
var LOG_QUERIES = false;

var Dialect = require('../../dialect.js');
var MysqlCriteriaProcessor = require('./mysqlCriteriaProcessor');

var MysqlDialect = module.exports = function(){};

MysqlDialect.prototype = new Dialect();

MysqlDialect.prototype.CriteriaProcessor = MysqlCriteriaProcessor;

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

MysqlDialect.prototype.createAlias = function(tableAlias,columnName) {
    var alias = tableAlias + '_' + columnName;
    if (alias.length > 64) {
        return crypto.createHash('sha1').update(alias).digest('base64').replace('=', '');
    }
    return alias;
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

MysqlDialect.prototype.afterSelect = function(connection, select) {
  if (!select.skipLimitQuery) {
    return;
  }

  select.skipLimitSubQuery = select.skipLimitSubQuery.from(select.query.as('SKLMT'));
  select.query = select.skipLimitQuery.from(select.skipLimitSubQuery.as('SUBSKLMT'));
};

MysqlDialect.prototype.selectSkipLimit = function(connection,select) {
  var self = this;
  if (select.options.skip || select.options.limit) {
    if (!select.options.joins) {
      if (select.options.skip) {
        select.query.offset(select.options.skip);
      }
      if (select.options.limit) {
        select.query.limit(select.options.limit);
      }
    } else {
        if (select.options.sort) {
          _.keys(select.options.sort).forEach(function(toSort) {
            var direction = select.options.sort[toSort] === 1 ? 'ASC' : 'DESC';
            select.query.orderBy(select.alias + '.' + toSort, direction);
          });
          // Subquery 'order by' needs a limit or the parser ignores it (0xefffffffffffffff is around max javascript parseInt() number)
          select.query.limit(0xefffffffffffffff);
        }

        var parentPk = connection.client.raw('??.??', ['SKLMT', connection.getPk(select.tableName)]);
        var sklmtAlias = self.createAlias('SKLMT', select.alias);

        if (!select.skipLimitQuery) {
          select.skipLimitQuery = connection.client.select('SUBSKLMT.*');
          select.skipLimitSubQuery = connection.client.select('SKLMT.*');
        }

        select.skipLimitSubQuery = this._getDenseRank(connection, sklmtAlias, select.skipLimitSubQuery, [parentPk]);

        if (select.options.skip && select.options.limit) {
          select.skipLimitQuery.having(function() {
            this.andWhere(sklmtAlias, '>', select.options.skip);
            this.andWhere(sklmtAlias, '<=', select.options.limit + select.options.skip);
          });

        } else if (select.options.skip) {
          select.skipLimitQuery.having(function() {
            this.andWhere(sklmtAlias, '>', select.options.skip);
          });
        } else if (select.options.limit) {
          select.skipLimitQuery.having(function() {
            this.andWhere(sklmtAlias, '<=', select.options.limit);
          });
        }
    }
  }
};

MysqlDialect.prototype.joinSkipLimit = function(select, association) {
  var self = this;
  var connection = select.connection;
  var join = association.join;
  var childDefinition = connection.getCollection(join.child).definition;

  var skip = join.criteria.skip;
  var limit = join.criteria.limit;
  delete join.criteria.skip;
  delete join.criteria.limit;
  this.join(select, association);
  join.criteria.skip = skip;
  join.criteria.limit = limit;

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

  if (!select.skipLimitQuery) {
    select.skipLimitQuery = connection.client.select('SUBSKLMT.*');
    select.skipLimitSubQuery = connection.client.select('SKLMT.*');
  }

  var parentPk = connection.client.raw('??.??', ['SKLMT', connection.getPk(select.tableName)]);
  var childPk = connection.client.raw('??.??', ['SKLMT', self.createAlias(join.alias, connection.getPk(join.child))]);
  var sklmtAlias = self.createAlias('SKLMT', join.alias);
  select.skipLimitSubQuery = this._getDenseRank(connection, sklmtAlias, select.skipLimitSubQuery, [parentPk, childPk]);

  if (join.criteria.skip && join.criteria.limit) {
    select.skipLimitQuery.having(function() {
      this.andWhere(function() {
        this.andWhere(sklmtAlias, '>', join.criteria.skip);
        this.andWhere(sklmtAlias, '<=', join.criteria.limit + join.criteria.skip);
      });
      // do not skip parent data when there is no child
      this.orWhereNull(sklmtAlias);
      // do not skip parent data when child skip >= child count
      this.orWhere(sklmtAlias, '=', 1);
    });
    // inform cursor to skip first child data
    association.skipFirst = sklmtAlias;

  } else if (join.criteria.skip) {
    select.skipLimitQuery.having(function() {
      this.andWhere(sklmtAlias, '>', join.criteria.skip);
      // do not skip parent data when there is no child
      this.orWhereNull(sklmtAlias);
      // do not skip parent data when child skip >= child count
      this.orWhere(sklmtAlias, '=', 1);
    });
    association.skipFirst = sklmtAlias;
  } else if (join.criteria.limit) {
    select.skipLimitQuery.having(function() {
      this.andWhere(sklmtAlias, '<=', join.criteria.limit);
    });
  }
};

MysqlDialect.prototype.joinManyToManySkipLimit = function(select, association) {
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

  join.select.forEach(function(columnName) {
    var childAlias = self.createAlias(join.alias, columnName);
    select.query.select(join.alias + '.' + columnName + ' as ' + childAlias);
  });

  var skLmtAlias = this.createAlias('_SKLMT_', join.alias);

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

  if (!select.skipLimitQuery) {
    select.skipLimitQuery = connection.client.select('SUBSKLMT.*');
    select.skipLimitSubQuery = connection.client.select('SKLMT.*');
  }

  var parentPk = connection.client.raw('??.??', ['SKLMT', connection.getPk(select.tableName)]);
  var childPk = connection.client.raw('??.??', ['SKLMT', this.createAlias(join.alias, connection.getPk(join.child))]);
  select.skipLimitSubQuery = this._getDenseRank(connection, skLmtAlias, select.skipLimitSubQuery, [parentPk, childPk]);

  if (join.criteria.skip && join.criteria.limit) {
    select.skipLimitQuery.andHaving(function() {
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      this.andWhere(skLmtAlias, '<=', join.criteria.limit + join.criteria.skip);
    });
    select.skipLimitQuery.orHaving(skLmtAlias, '=', 1);
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.skip) {
    select.skipLimitQuery.andHaving(function() {
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      this.orWhere(skLmtAlias, '=', 1);
    });
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.limit) {
    select.skipLimitQuery.andHaving(skLmtAlias, '<=', join.criteria.limit);
  }
};

MysqlDialect.prototype._getDenseRank = function(connection, alias, query, partitionBy) {
  /* select *, @rank := if(@previous_id=`selectQuery`.`id`, if(@previous_child_id=`selectQuery`.`child_id`, @rank, @rank+1),1) as denseRank, @previous_id := `selectQuery`.`id`, @previous_child_id := `selectQuery`.`child_id`
   *
   * (SELECT @rank := 1, @previous_id := '', @previous_child_id := '') as varDeclaration)
   * */

  // incDenseRank contains strings like '@rank := if(@previous_id=`selectQuery`.`id`, if(@previous_child_id=`selectQuery`.`child_id`, @rank, @rank+1),1) as denseRank'
  // it checks if any of the values have changed, and, if they have, increment rank

  // updateVar contains strings like '@previous_id := `selectQuery`.`id`, @previous_child_id := `selectQuery`.`child_id`'
  // it simply assign variable values for each new row

  // updateVar contains strings like '(SELECT @rank := 1, @previous_id := '', @previous_child_id := '') as varDeclaration)'
  // it's used for inline variable initialization (we can't use "set" as Knex doesn't support it yet)

  // For each given column, create its corresponding variable and add it to all three parts
  var incDenseRank = '@rank_' + alias + ' := ';
  partitionBy.forEach(function(partition, index) {
    var previousVar = '@previous_' + alias + '_' + index;
    incDenseRank += 'if(' + previousVar + ' = ' + partition + ', ';
  });

  // Add last arguments to all 'if'
  incDenseRank += '@rank_' + alias + ', @rank_' + alias + '+1)';
  for(var i = 0; i < partitionBy.length-1; i++) {
    incDenseRank += ',1)';
  }
  incDenseRank += ' as ' + alias + ', ';

  var updateVar = [];
  partitionBy.forEach(function(partition, index) {
    var previousVar = '@previous_' + alias + '_' + index;
    updateVar.push(previousVar + ' := ' + partition);
  });
  incDenseRank += updateVar.join();

  var initVar = ['(SELECT @rank_' + alias + ' := 0'];
  partitionBy.forEach(function(partition, index) {
    var previousVar = '@previous_' + alias + '_' + index;
    initVar.push(previousVar + ' := \'\'');
  });
  initVar = initVar.join();
  initVar += ') as varDeclaration_' + alias;

  return query.select(connection.client.raw(incDenseRank)).join(connection.client.raw(initVar));
};
