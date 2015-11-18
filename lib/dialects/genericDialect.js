var Knex = require('knex');
var _ = require('underscore');
var Utils = require('../utils');
var CriteriaProcessor = require('../criteriaProcessor');

var GenericDialect = function () {
};

GenericDialect.prototype.stringDelimiter = "'";

/* Retrieving infos about tables */
GenericDialect.prototype.describe = function (connection, collection, callback) {
  callback("ERROR: UNDEFINED_METHOD");
};

/* Normalizing schema */
GenericDialect.prototype.normalizeSchema = function (schema) {
  throw new Error("DIALECT UNDEFINED METHOD: normalizeSchema");
};


GenericDialect.prototype.normalizeTableName = function (tableName) {
  return tableName;
};

GenericDialect.prototype.sqlEscapeString = function (str) {
  return str;
};

GenericDialect.prototype.sqlEscapeTableName = function (tableName) {
  return tableName;
};

GenericDialect.prototype.sqlEscapeColumnName = function (columnName) {
  return columnName;
};

GenericDialect.prototype.createAlias = function (tableAlias, columnName) {
  return tableAlias + '_' + columnName;
};

GenericDialect.prototype.defineColumn = function (table, attrName, attribute) {
  var column;

  if (attribute.autoIncrement && attribute.primaryKey) {
    return table.increments(attrName).primary();
  }

  if (attribute.autoIncrement) {
    table.increments(attrName);
  }
  else {
    switch (attribute.type) {// defining type
      case 'string':
        column = table.string(attrName, attribute.size || undefined);
        break;
      case 'text':
        column = table.text(attrName);
        break;
      case 'mediumtext':
        column = table.text(attrName, 'mediumtext');
        break;
      case 'array':
        column = table.json(attrName);
        break;
      case 'json':
        column = table.json(attrName);
        break;
      case 'longtext':
        column = table.text(attrName, 'longtext');
        break;
      case 'binary':
        column = table.binary(attrName);
        break;
      case 'boolean':
        column = table.boolean(attrName);
        break;
      case 'datetime':
        column = table.datetime(attrName);
        break;
      case 'date':
        column = table.date(attrName);
        break;
      case 'time':
        column = table.time(attrName);
        break;
      case 'float':
      case 'double':
        column = table.float(attrName, 23, 8);
        break;
      case 'decimal':
        column = table.decimal(attrName);
        break;
      case 'int':
      case 'integer':
        column = table.integer(attrName);
        break;
      default:
        console.error("Unregistered type given: '" + attribute.type + "', TEXT type will be used");
        return "TEXT";
    }
  }
  if (attribute.primaryKey) {
    column.primary();
  }


  else if (attribute.unique) {
    column.unique();
  }

  if (attribute.required || attribute.notNull) {
    column.notNullable();
  }

  if (attribute.index) {
    column.index();
  }

  return column;
};

GenericDialect.prototype.insert = function (connection, collection, record) {
  var tableName = this.normalizeTableName(collection.tableName);
  var pk = connection.getPk(tableName);
  var insertQuery = connection.client(tableName).insert(record);
  //ORACLE HACK
  if (collection.definition[pk].autoIncrement) {
    insertQuery.returning(pk);
  }
  return insertQuery.then(function (result) {
    var pkval = {};
    if (collection.definition[pk].autoIncrement) {
      pkval[pk] = result[0];
    }
    return _.extend({}, record, pkval);
  });
};

GenericDialect.prototype.select = function (connection, collection, opts) {
  var self = this;
  var select = {};
  select.tableName = this.normalizeTableName(collection.tableName);
  select.alias = this.createAlias('_PARENT_',select.tableName);
  select.pk = connection.getPk(select.tableName);
  var definition = collection.definition;
  select.options = Utils.normalizeCriteria(opts);
  select.selection = [];
  if (!select.options.select) {
    _.keys(definition).forEach(function (field) {
      select.selection.push(select.alias + '.' + field);
    });
  }
  else {
    select.options.select.forEach(function (field) {
      select.selection.push(select.alias + '.' + field);
    });
    delete select.options.select;
  }

  //if ((select.options.limit || select.options.skip) && select.options.joins) {
    //var subquery = new CriteriaProcessor(connection, select.tableName, select.options, connection.client(select.tableName)).getQuery().select(select.selection).as(select.tableName);
    //var subquery = new CriteriaProcessor(connection, select.tableName, select.options).select(selection).as(select.tableName);
    //select.selection = [];
    //select.query = connection.client.select().from(subquery);
  //}
  //else {
    select.query = new CriteriaProcessor(connection, select.alias, select.options, connection.client(select.tableName + ' as ' + select.alias)).getQuery(); 
  //}

  // Aggregates TODO => refactorize
  if (select.options.sum) {
    select.options.sum.forEach(function (keyToSum) {
      var sumAlias = self.createAlias('_SUM_',keyToSum);
      var subQuery = connection.client(select.tableName + ' as ' + sumAlias).sum(sumAlias + '.' + keyToSum + ' as ' + keyToSum).as(keyToSum);
      subQuery = new CriteriaProcessor(connection, sumAlias, select.options, subQuery).getQuery();
      if (select.options.groupBy) {
        select.options.groupBy.forEach(function (groupKey) {
          subQuery.groupBy(sumAlias + '.' + groupKey);
          subQuery.andWhere(Knex.raw(sumAlias + '.' + groupKey + ' = ' + select.alias + '.' + groupKey));
        });
      }
      select.selection.push(subQuery);
    });
  }

  if (select.options.average) {
    select.options.average.forEach(function (keyToAvg) {
      var avgAlias = self.createAlias('_AVG_',keyToAvg);
      var subQuery = connection.client(select.tableName + ' as ' + avgAlias).avg(avgAlias + '.' + keyToAvg + ' as ' + keyToAvg).as(keyToAvg);
      subQuery = new CriteriaProcessor(connection, avgAlias, select.options, subQuery).getQuery();
      if (select.options.groupBy) {
        select.options.groupBy.forEach(function (groupKey) {
          subQuery.groupBy(avgAlias + '.' + groupKey);
          subQuery.andWhere(Knex.raw(avgAlias + '.' + groupKey + ' = ' + select.alias + '.' + groupKey));
        });
      }      
      select.selection.push(subQuery);
    });
  }

  if (select.options.min) {
    select.options.min.forEach(function (keyToMin) {
      var minAlias = self.createAlias('_MIN_',keyToMin);
      var subQuery = connection.client(select.tableName + ' as ' + minAlias).min(minAlias + '.' + keyToMin + ' as ' + keyToMin).as(keyToMin);
      subQuery = new CriteriaProcessor(connection, minAlias, select.options, subQuery).getQuery();
      if (select.options.groupBy) {
        select.options.groupBy.forEach(function (groupKey) {
          subQuery.groupBy(minAlias + '.' + groupKey);
          subQuery.andWhere(Knex.raw(minAlias + '.' + groupKey + ' = ' + select.alias + '.' + groupKey));
        });
      }      
      select.selection.push(subQuery);
    });
  }

  if (select.options.max) {
    select.options.max.forEach(function (keyToMax) {
      var maxAlias = self.createAlias('_MAX_',keyToMax);
      var subQuery = connection.client(select.tableName + ' as ' + maxAlias).max(maxAlias + '.' + keyToMax + ' as ' + keyToMax).as(keyToMax);
      subQuery = new CriteriaProcessor(connection, maxAlias, select.options, subQuery).getQuery();
      if (select.options.groupBy) {
        select.options.groupBy.forEach(function (groupKey) {
          subQuery.groupBy(maxAlias + '.' + groupKey);
          subQuery.andWhere(Knex.raw(maxAlias + '.' + groupKey + ' = ' + select.alias + '.' + groupKey));
        });
      }      
      select.selection.push(subQuery);
    });
  }

  if (select.options.groupBy) {
    select.options.groupBy.forEach(function (groupKey) {
      select.query = select.query.groupBy(select.alias + '.' + groupKey);
    });
  }

  if (select.options.skip || select.options.limit) {
    if (!select.options.joins) {
      if (select.options.skip) {
        select.query = select.query.offset(select.options.skip);
      }
      if (select.options.limit) {
        select.query = select.query.limit(select.options.limit);
      }
    }
    else {
      var skLmtAlias = 'SKLMT';
      var skLmtQuery = connection.client(select.tableName + ' as ' + skLmtAlias).count('*');
      new CriteriaProcessor(connection, skLmtAlias, select.options, skLmtQuery);
      
      if (!select.options.sort) {
        select.options.sort = {};
        select.options.sort[select.pk] = 1;
      }
      var j;
      var keys = _.keys(select.options.sort);
      skLmtQuery.andWhere(function(){
        for (var i in keys) {
          this.orWhere(function(){
            j = 0;
            while(j < i) {
              this.andWhere(Knex.raw(skLmtAlias + '.' + keys[j] + ' = ' + select.alias + '.' + keys[j]));
              j++;
            }
            var key = keys[i];
            if (select.options.sort[key]) {
              this.andWhere(Knex.raw(select.alias + '.' + keys[i] + ' > ' + skLmtAlias + '.' + keys[i]));
            }
            else {
              this.andWhere(Knex.raw(select.alias + '.' + keys[i] + ' < ' + skLmtAlias + '.' + keys[i]));
            }
          });
        }
      });
      select.selection.push(skLmtQuery.as(skLmtAlias));
      if (select.options.skip && select.options.limit) {
        select.query.andHaving(skLmtAlias,'>=',select.options.skip);
        select.query.andHaving(skLmtAlias, '<', select.options.limit + select.options.skip);
      } else if (select.options.skip) {
        select.query.andHaving(skLmtAlias,'>=',select.options.skip);
      } else if (select.options.limit) {
        select.query.andHaving(skLmtAlias,'<',select.options.limit);
      }
    }
  }
  
  if (select.options.sort) {
    _.keys(select.options.sort).forEach(function (toSort) {
      var direction = select.options.sort[toSort] === 1 ? 'ASC' : 'DESC';
      select.query = select.query.orderBy(select.alias + '.' + toSort, direction);
    });
  }

  if (select.options.joins) {
    select.options.joins.forEach(function (join) {
      join.parent = self.normalizeTableName(join.parent);
      join.child = self.normalizeTableName(join.child);
      self.join(connection, select, join);
    });
  }

  return select.query.select(select.selection).then(function(res){console.log('res',res);return res;});
};
GenericDialect.prototype.join = function (connection, select, join) {
  if (join.criteria && (join.criteria.skip || join.criteria.limit)) {
    this.joinSkipLimit(connection, select, join);
  }
  else {
    var self = this;
    var childDefinition = connection.getCollection(join.child).definition;
    var parent = join.parent;
    if (parent === select.tableName) {
      parent = select.alias;
    }
    if (join.select === false) {
      select.query.leftJoin(join.child, function () {
        this.on(parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
      });
    }
    else {
      select.query.leftJoin(join.child + ' as ' + join.alias, function () {
        this.on(parent + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
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

GenericDialect.prototype.joinSkipLimit = function (connection, select, join) {
  var self = this;
  var childDefinition = connection.getCollection(join.child).definition;
  var parent = join.parent;
  if (parent === select.tableName) {
    parent = select.alias;
  }
  var parentKey = join.parentKey;
  if (join.select === false) {
    select.query.leftJoin(join.child, function () {
      this.on(parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
    });
  }
  else {
    select.query.leftJoin(join.child + ' as ' + join.alias, function () {
      this.on(parent + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
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

    var skLmtAlias = this.createAlias('_SKLMT_', join.alias);
    var skLmtQuery = connection.client(join.child + ' as ' + skLmtAlias).count('*');

    if (join.junctionTable) {
      var junctionTable = _.find(select.options.joins, function (junction) {
        return (junction.select === false && junction.alias === join.alias);
      });
      if (junctionTable) {
        skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
        skLmtQuery.leftJoin(junctionTable.parent, junctionTable.parent + '.' + junctionTable.parentKey, junctionTable.child + '.' + junctionTable.childKey);
        skLmtQuery.andWhere(Knex.raw(junctionTable.parent + '.' + junctionTable.parentKey + ' = ' + select.alias + '.' + select.pk));
      }
      else {
        console.log('error junctionTable', junctionTable.length);
      }
    }
    else {
      skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
      skLmtQuery.andWhere(Knex.raw(join.parent + '.' + join.parentKey + ' = ' + select.alias + '.' + select.pk));
    }
    new CriteriaProcessor(connection, skLmtAlias, join.criteria, skLmtQuery);

    if (!join.criteria.sort) {
      join.criteria.sort = {};
      join.criteria.sort[join.childKey] = 1;
    }
    var j;
    var keys = _.keys(join.criteria.sort);
    skLmtQuery.andWhere(function () {
      for (var i in keys) {
        this.orWhere(function () {
          j = 0;
          while (j < i) {
            this.andWhere(Knex.raw(join.alias + '.' + keys[j] + ' = ' + skLmtAlias + '.' + keys[j]));
          }
          var key = keys[i];
          if (join.criteria.sort[key]) {
            this.andWhere(Knex.raw(join.alias + '.' + keys[i] + ' > ' + skLmtAlias + '.' + keys[i]));
          }
          else {
            this.andWhere(Knex.raw(join.alias + '.' + keys[i] + ' < ' + skLmtAlias + '.' + keys[i]));
          }
        });
      }
    });
    
    select.selection.push(skLmtQuery.as(skLmtAlias));
    if (join.criteria.skip && join.criteria.limit) {
      select.query.andHaving(skLmtAlias, '>=', join.criteria.skip);
      select.query.andHaving(skLmtAlias, '<', join.criteria.limit + join.criteria.skip);
    } else if (join.criteria.skip) {
      select.query.andHaving(skLmtAlias, '>=', join.criteria.skip);
    } else if (join.criteria.limit) {
      select.query.andHaving(skLmtAlias, '<', join.criteria.limit);
    }
    
    if (join.criteria && join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function (toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        select.query = select.query.orderBy(join.alias + '.' + toSort, direction);
      });
    }
  }
};

GenericDialect.prototype.update = function (connection, collection, opts, data) {
  var tableName = this.normalizeTableName(collection.tableName);
  var options = Utils.normalizeCriteria(opts);
  var updateQuery = new CriteriaProcessor(connection, tableName, options, connection.client(tableName)).getQuery();
  return updateQuery.update(data);
};

GenericDialect.prototype.delete = function (connection, collection, opts) {
  var tableName = this.normalizeTableName(collection.tableName);
  var options = Utils.normalizeCriteria(opts);
  var deleteQuery = new CriteriaProcessor(connection, tableName, options, connection.client(tableName)).getQuery();
  return deleteQuery.del();
};

GenericDialect.prototype.createTable = function (connection, collection, definition) {
  var self = this;
  var tableName = this.normalizeTableName(collection.tableName);
  return connection.client.schema.createTable(tableName, function (table) {
    _.keys(definition).forEach(function (attrName) {
      self.defineColumn(table, attrName, definition[attrName]);
    });
  });
};

GenericDialect.prototype.dropTable = function (connection, collection) {
  var tableName = this.normalizeTableName(collection.tableName);
  return connection.client.schema.dropTableIfExists(tableName);
};


module.exports = GenericDialect;



