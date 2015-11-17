var asynk = require('asynk');
var _ = require('underscore');
var crypto = require('crypto');

var GenericDialect = require('./genericDialect.js');

var OracleDialect = module.exports = function () {
};

OracleDialect.prototype = new GenericDialect();

OracleDialect.prototype.describe = function (connection, collection, callback) {

    var tableName = this.normalizeTableName(collection.tableName); 
    var queries = [], results = [];
    queries[0] = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
    queries[1] = "SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE table_name = '" + tableName + "'";
    queries[2] = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner "
            + "FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '" + tableName
            + "' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner "
            + "ORDER BY cols.table_name, cols.position";

    asynk.each(queries, function (query, nextQuery) {
        connection.client.raw(query).then(function (result) {
            results[queries.indexOf(query)] = result;
            nextQuery();
        }).catch(function (e) {
            nextQuery(e);
        });
    }).args(asynk.item, asynk.callback).serie(function (err) {
        if (err) {
            callback(err, null);
            return;
        }
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
    }, [null, asynk.data('all')]);
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

OracleDialect.prototype.sqlEscapeTableName = function(tableName) {
    return '"' + tableName + '"';
};

OracleDialect.prototype.sqlEscapeColumnName = function(ColumnName) {
    return '"' + ColumnName + '"';
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

OracleDialect.prototype.join = function (connection, select, join) {
  if (join.criteria && (join.criteria.skip || join.criteria.limit)) {
    this.joinSkipLimit(connection, select, join);
  }
  else {
    var self = this;
    var childDefinition = connection.getCollection(join.child).definition;
    if (join.select === false) {
      select.query.leftJoin(join.child, function () {
        this.on(join.parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
        if (join.criteria && (join.criteria.skip || join.criteria.limit)) {
          var subQuery = connection.client(join.child + ' as ' + join.alias);
          var criteriaSkip = join.criteria.skip;
          delete join.criteria.skip;
          var criteriaLimit = join.criteria.limit;
          delete join.criteria.limit;
          subQuery = new CriteriaProcessor(connection, select.tableName, select.options, subQuery, 'where').getQuery();
          subQuery = new CriteriaProcessor(connection, join.alias, join.criteria, subQuery, 'where').getQuery();
          if (join.select) {
            var subSelection = [];
            join.select.forEach(function (columnName) {
              if (childDefinition[columnName]) {
                var childAlias = self.createAlias(join.alias, columnName);
                subSelection.push(join.alias + '.' + columnName);
                select.selection.push(join.alias + '.' + columnName + ' as ' + childAlias);
              }
            });

            var sort = '';
            if (join.criteria.sort) {
              _.keys(join.criteria.sort).forEach(function (toSort) {
                var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
                if (sort !== '') {
                  sort += ', ';
                }
                else {
                  sort += ' order by ';
                }
                sort += '"' + join.alias + '"."' + toSort + '" ' + direction;
              });
            }

            subSelection.push(Knex.raw('(row_number() over (partition by "' + select.tableName + '"."' + select.pk + '"' + sort + ')) "row_"'));
            subQuery = subQuery.select(subSelection);
          }

          if (join.junctionTable) {
            var junctionTable = _.filter(select.options.joins, function (junction) {
              return (junction.select === false && junction.alias === join.alias);
            });
            if (junctionTable.length === 1) {
              junctionTable = junctionTable[0];
              subQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, join.alias + '.' + join.childKey);
              subQuery.leftJoin(junctionTable.parent, junctionTable.parent + '.' + junctionTable.parentKey, junctionTable.child + '.' + junctionTable.childKey);

            }
            else {
              console.log('error junctionTable', junctionTable.length);
            }
          }
          else {
            subQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, join.alias + '.' + join.childKey);
          }
          subQuery = subQuery.as(join.alias);


          select.query.leftJoin(subQuery, join.parent + '.' + join.parentKey, join.alias + '.' + join.childKey);

          select.query.where(function () {
            if (criteriaSkip) {
              this.whereRaw('"row_" > ?', criteriaSkip);
            }

            if (criteriaLimit) {
              this.whereRaw('"row_" <= ?', criteriaLimit + (criteriaSkip || 0));
            }

            this.orWhereNull(join.alias + '.' + join.childKey);
          });
        }
        else {
          if (join.select === false) {
            select.query.leftJoin(join.child, function () {
              this.on(join.parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
            });
          }
          else {
            select.query.leftJoin(join.child + ' as ' + join.alias, function () {
              this.on(join.parent + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
              if (join.criteria) {
                new CriteriaProcessor(connection, join.alias, join.criteria, this, 'on');
              }
            });
            //ADD COLUMN WITH ALIAS IN SELECTION
            if (join.select) {
              join.select.forEach(function (columnName) {
                if (childDefinition[columnName]) {
                  var childAlias = self.createAlias(join.alias, columnName);
                  select.selection.push(join.alias + '.' + columnName + ' as ' + childAlias);
                }
              });
            }
            if (join.criteria && join.criteria.sort) {
              _.keys(join.criteria.sort).forEach(function (toSort) {
                var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
                select.query = select.query.orderBy(join.alias + '.' + toSort, direction);
              });
            }
          }
        }
      });
    }
    else {
      select.query.leftJoin(join.child + ' as ' + join.alias, function () {
        this.on(join.parent + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
        if (join.criteria) {
          new CriteriaProcessor(connection, join.alias, join.criteria, this, 'on');
        }
      });
      //ADD COLUMN WITH ALIAS IN SELECTION
      if (join.select) {
        join.select.forEach(function (columnName) {
          if (childDefinition[columnName]) {
            var childAlias = self.createAlias(join.alias, columnName);
            select.selection.push(join.alias + '.' + columnName + ' as ' + childAlias);
          }
        });
      }
      if (join.criteria && join.criteria.sort) {
        _.keys(join.criteria.sort).forEach(function (toSort) {
          var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
          select.query = select.query.orderBy(join.alias + '.' + toSort, direction);
        });
      }
    }
  }
};

OracleDialect.prototype.joinSkipLimit = function (connection, select, join) {
  var self = this;
  var childDefinition = connection.getCollection(join.child).definition;

  var subQuery = connection.client(join.child + ' as ' + join.alias);
  var criteriaSkip = join.criteria.skip;
  delete join.criteria.skip;
  var criteriaLimit = join.criteria.limit;
  delete join.criteria.limit;
  subQuery = new CriteriaProcessor(connection, select.tableName, select.options, subQuery, 'where').getQuery();
  subQuery = new CriteriaProcessor(connection, join.alias, join.criteria, subQuery, 'where').getQuery();
  if (join.select) {
    var subSelection = [];
    join.select.forEach(function (columnName) {
      if (childDefinition[columnName]) {
        var childAlias = self.createAlias(join.alias, columnName);
        subSelection.push(join.alias + '.' + columnName);
        select.selection.push(join.alias + '.' + columnName + ' as ' + childAlias);
      }
    });

    var sort = '';
    if (join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function (toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        if (sort !== '') {
          sort += ', ';
        }
        else {
          sort += ' order by ';
        }
        sort += '"' + join.alias + '"."' + toSort + '" ' + direction;
      });
    }

    subSelection.push(Knex.raw('(row_number() over (partition by "' + select.tableName + '"."' + select.pk + '"' + sort + ')) "row_"'));
    subQuery = subQuery.select(subSelection);
  }

  if (join.junctionTable) {
    var junctionTable = _.filter(select.options.joins, function (junction) {
      return (junction.select === false && junction.alias === join.alias);
    });
    if (junctionTable.length === 1) {
      junctionTable = junctionTable[0];
      subQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, join.alias + '.' + join.childKey);
      subQuery.leftJoin(junctionTable.parent, junctionTable.parent + '.' + junctionTable.parentKey, junctionTable.child + '.' + junctionTable.childKey);

    }
    else {
      console.log('error junctionTable', junctionTable.length);
    }
  }
  else {
    subQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, join.alias + '.' + join.childKey);
  }
  subQuery = subQuery.as(join.alias);


  select.query.leftJoin(subQuery, join.parent + '.' + join.parentKey, join.alias + '.' + join.childKey);

  select.query.where(function () {
    if (criteriaSkip) {
      this.whereRaw('"row_" > ?', criteriaSkip);
    }

    if (criteriaLimit) {
      this.whereRaw('"row_" <= ?', criteriaLimit + (criteriaSkip || 0));
    }

    this.orWhereNull(join.alias + '.' + join.childKey);
  });
};