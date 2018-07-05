var _ = require('lodash');
var inherits = require('inherits');
var Utils = require('./utils');
var CriteriaProcessor = require('./criteriaProcessor');
var hooks = require('./hooks');

var GenericDialect = function() {
  hooks.apply(this, arguments);
};

inherits(GenericDialect, hooks);

GenericDialect.prototype.CriteriaProcessor = CriteriaProcessor;

GenericDialect.prototype.Utils = Utils;

GenericDialect.prototype.stringDelimiter = "'";

/* Retrieving infos about tables */
GenericDialect.prototype.describe = function(connection, collection, callback) {
  callback(new Error("ERROR: UNDEFINED_METHOD"));
};

/* Normalizing schema */
GenericDialect.prototype.normalizeSchema = function() {
  throw new Error("DIALECT UNDEFINED METHOD: normalizeSchema");
};


GenericDialect.prototype.normalizeTableName = function(tableName) {
  return tableName;
};

GenericDialect.prototype.sqlEscapeString = function(str) {
  return str;
};

GenericDialect.prototype.createAlias = function(tableAlias, columnName) {
  if (!columnName) {
    return tableAlias;
  }
  return tableAlias + '_' + columnName;
};

GenericDialect.prototype.defineColumn = function(table, attrName, attribute) {
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
        column = table.text(attrName);
        break;
      case 'json':
        column = table.text(attrName);
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

GenericDialect.prototype.insert = function(connection, collection, record) {
  record = this.Utils.prepareValues(record);
  var tableName = this.normalizeTableName(collection.tableName);
  var pk = connection.getPk(tableName);
  var insertQuery = connection.client(tableName).insert(record);
  if (collection.definition[pk].autoIncrement) {
    insertQuery.returning(pk);
  }
  return insertQuery;
};

GenericDialect.prototype.count = function(connection, collection, opts) {
  var tableName = this.normalizeTableName(collection.tableName);
  var options = this.Utils.normalizeCriteria(opts);
  var query = new this.CriteriaProcessor(connection, tableName, options, connection.client(tableName)).getQuery();
  query.count('* as cnt');
  return query.then(function(cnt){ return cnt[0]['cnt']; });
};

GenericDialect.prototype.select = function(connection, collection, opts) {

  this.beforeSelect(connection, collection, opts);

  var self = this;
  var select = {};
  select.connection = connection;
  select.tableName = this.normalizeTableName(collection.tableName);
  select.alias = this.createAlias('_PARENT_',select.tableName);
  select.pk = connection.getPk(select.tableName);
  select.definition = collection.definition;
  select.options = this.Utils.normalizeCriteria(opts);
  select.attributes = [];
  select.associations = [];
  select.query = new this.CriteriaProcessor(connection, select.alias, select.options, connection.client(select.tableName + ' as ' + select.alias)).getQuery();

  select.selection = [];
  select.aggregates = [];

  var aggregate = function(type, columns) {
    columns.forEach(function(key) {
      select.query[type](select.alias + '.' + key + ' as ' + key);
      select.selection.push(select.alias + '.' + key);
      select.aggregates.push({tableName: select.tableName, columnName: key, alias: key, type: 'float'});
    });
  };

  if (select.options.sum) {
    aggregate('sum', select.options.sum);
  }

  if (select.options.average) {
    aggregate('avg', select.options.average);
  }

  if (select.options.min) {
    aggregate('min', select.options.min);
  }

  if (select.options.max) {
    aggregate('max', select.options.max);
  }

  if (select.aggregates.length > 0) {
    if (select.options.groupBy) {
      select.options.groupBy.forEach(function(groupVal) {
        select.query.select(select.alias + '.' + groupVal);
        select.query.groupBy(select.alias + '.' + groupVal);
      });
    }
  } else if (!select.options.select) {
    _.keys(select.definition).forEach(function(field) {
      var column = select.alias + '.' + field;
      if (select.selection.indexOf(column) < 0) {
        select.selection.push(column);
        select.query.select(column);
      }
    });
  } else {
    select.options.select.forEach(function(field) {
      var column = select.alias + '.' + field;
      if (select.selection.indexOf(column) < 0) {
        select.selection.push(select.alias + '.' + field);
        select.query.select(column);
      }
    });
  }

  // sort parent before childs sort
  this.selectSort(connection,select);

  var aggregateChild = function(association) {
    var aggregates = [];
    var definition = connection.getCollection(association.join.child).definition;
    var criteria = association.join.criteria;
    if (criteria) {
      if (criteria.sum && _.isArray(criteria.sum)) {
        criteria.sum.forEach(function(columnName) {
          if (definition[columnName]) {
            aggregates.push({
              aggregate: 'sum',
              name: columnName,
              alias: self.createAlias(association.name, columnName),
              type: definition[columnName].type
            });
          }
        });
      }

      if (criteria.average) {
        criteria.average.forEach(function(columnName) {
          if (definition[columnName]) {
            aggregates.push({
              aggregate: 'avg',
              name: columnName,
              alias: self.createAlias(association.name, columnName),
              type: 'float'
            });
          }
        });
      }

      if (criteria.min) {
        criteria.min.forEach(function(columnName) {
          if (definition[columnName]) {
            aggregates.push({
              aggregate: 'min',
              name: columnName,
              alias: self.createAlias(association.name, columnName),
              type: definition[columnName].type
            });
          }
        });
      }

      if (criteria.max) {
        criteria.max.forEach(function(columnName) {
          if (definition[columnName]) {
            aggregates.push({
              aggregate: 'max',
              name: columnName,
              alias: self.createAlias(association.name, columnName),
              type: definition[columnName].type
            });
          }
        });
      }
    }
    return aggregates;
  };

  var populated = [];
  var relations = _.groupBy(select.options.joins, 'alias');

  // avoid exponential rows on joins
  this.joinId(select, relations);

  var associationId = 1;
  _.forEach(relations, function(relation) {
    var association = {};
    association.id = associationId++;
    association.join = null;
    association.junction = false;

    if (relation.length === 1) {
      association.join = _.clone(relation[0]);
    } else if (relation.length === 2) {
      if (relation[0].select === false) {
        association.junction = _.clone(relation[0]);
        association.join = _.clone(relation[1]);
      } else {
        association.join = _.clone(relation[0]);
        association.junction = _.clone(relation[1]);
      }
    }

    if (association.join) {
      association.name = association.join.alias;
      // normalize tableName
      association.join.parent = self.normalizeTableName(association.join.parent);
      association.join.child = self.normalizeTableName(association.join.child);

      association.join.alias = connection.dialect.createAlias(association.join.alias);

      if (association.junction) {
        association.junction.parent = self.normalizeTableName(association.junction.parent);
        association.junction.child = self.normalizeTableName(association.junction.child);
      }
      populated.push(association.name);
      var definition = connection.getCollection(association.join.child).definition;

      association.pk = connection.dialect.createAlias(association.name, connection.getPk(association.join.child));
      association.collection = association.join.collection;
      association.aggregates = aggregateChild(association);
      association.skipFirst = false;
      association.attributes = [];
      if (association.aggregates.length > 0) {
        association.attributes = _.clone(association.aggregates);
        if (association.join.criteria.groupBy && _.isArray(association.join.criteria.groupBy)) {
          association.join.criteria.groupBy.forEach(function(attributeName) {
            if (definition[attributeName]) {
              var columnAlias = connection.dialect.createAlias(association.name, attributeName);
              association.attributes.push({
                name: attributeName,
                alias: columnAlias,
                type: definition[attributeName].type
              });
            }
          });
        }
      } else if (association.join.select && _.isArray(association.join.select)) {
        _.uniq(association.join.select).forEach(function(attributeName) {
          if (definition[attributeName]) {
            var columnAlias = connection.dialect.createAlias(association.name, attributeName);
            association.attributes.push({
              name: attributeName,
              alias: columnAlias,
              type: definition[attributeName].type
            });
          }
        });
      }
      select.associations.push(association);
    }

    if (association.junction) {
      if (association.aggregates.length > 0) {
        self.joinManyToManyAggregate(select, association);
      } else {
        self.joinManyToMany(select, association);
      }
    } else {
      if (association.aggregates.length > 0) {
        self.joinAggregate(select, association);
      } else {
        self.join(select, association);
      }
    }
  });

  // parent selection
  var definition = connection.getCollection(select.tableName).definition;
  if (select.aggregates.length === 0) {
    var selection = select.options.select || _.keys(definition);
    selection.forEach(function(attributeName) {
      if (definition[attributeName]) {
        // if attribute is a associations model do not add pk value
        if (definition[attributeName].model && populated.indexOf(definition[attributeName].alias) >= 0) {
          return;
        }
        select.attributes.push({
          name: attributeName,
          alias: attributeName,
          type: definition[attributeName].type
        });
      }
    });
  // AGGREGATES
  } else {
    select.aggregates.forEach(function(attribute) {
      select.attributes.push({
        name: attribute.columnName,
        alias: attribute.alias,
        type: attribute.type
      });
    });
    // GROUP BY
    if (select.options.groupBy && _.isArray(select.options.groupBy)) {
      select.options.groupBy.forEach(function(attributeName) {
        if (definition[attributeName]) {
          select.attributes.push({
            name: attributeName,
            alias: attributeName,
            type: definition[attributeName].type
          });
        }
      });
    }
  }

  if (select.aggregates.length === 0) {
    //skip limit
    this.selectSkipLimit(connection, select);
  }

  this.afterSelect(connection, select);

  return select;
};

GenericDialect.prototype.joinId = function(select, relations) {
  if (_.keys(relations).length) {
    var joinSubquery = 'select 1 as id';
    for (var i = 2; i <= _.keys(relations).length; i++) {
      joinSubquery += ' UNION ALL SELECT ' + i;
    }
    select.query.join(connection.client.raw('(' + joinSubquery + ') as joins'));
  }
};

GenericDialect.prototype.joinAggregate = function(select, association) {
  var self = this;
  var joinAlias = select.connection.dialect.createAlias(association.join.alias, association.join.childKey);

  association.attributes.forEach(function(attribute) {
    select.query.select(association.join.alias + '.' + attribute.alias);
  });

  select.query.leftJoin(function() {
    var query = this;
    association.attributes.forEach(function(attribute) {
      if (attribute.aggregate) {
        query[attribute.aggregate](association.join.child + '.' + attribute.name + ' as ' + attribute.alias);
      } else {
        query.select(association.join.child + '.' + attribute.name + ' as ' + attribute.alias);
      }
    });
    // add join.childKey to selection and groupBy
    this.select(association.join.child + '.' + association.join.childKey + ' as ' + joinAlias);
    this.groupBy(association.join.child + '.' + association.join.childKey);
    // apply groupBy
    if (association.join.criteria && association.join.criteria.groupBy && _.isArray(association.join.criteria.groupBy)) {
      association.join.criteria.groupBy.forEach(function(attributeName) {
        query.groupBy(association.join.child + '.' + attributeName);
      });
    }
    this.from(association.join.child).as(association.join.alias);
  }, function() {
    this.on(select.alias + '.' + association.join.parentKey, association.join.alias + '.' + joinAlias);
  });

  // orderBy
  if (association.join.criteria.sort) {
    _.keys(association.join.criteria.sort).forEach(function (toSort) {
      var direction = association.join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
      var sortAlias = self.createAlias(association.name, toSort);
      select.query.orderBy(association.join.alias + '.' + sortAlias, direction);
    });
  }

};

GenericDialect.prototype.joinManyToManyAggregate = function(select, association) {
  var self = this;
  var junctionAlias = this.createAlias('junction_', association.join.alias);

  association.attributes.forEach(function(attribute) {
    select.query.select(association.join.alias + '.' + attribute.alias);
  });

  select.query.leftJoin(function() {
    var query = this;
    association.attributes.forEach(function(attribute) {
      if (attribute.aggregate) {
        query[attribute.aggregate](association.join.child + '.' + attribute.name + ' as ' + attribute.alias);
      } else {
        query.select(association.join.child + '.' + attribute.name + ' as ' + attribute.alias);
      }
    });
    // add junction.childKey to selection and groupBy
    this.select(association.junction.child + '.' + association.junction.childKey + ' as ' + junctionAlias);
    this.groupBy(association.junction.child + '.' + association.junction.childKey);


    // apply groupBy
    if (association.join.criteria && association.join.criteria.groupBy && _.isArray(association.join.criteria.groupBy)) {
      association.join.criteria.groupBy.forEach(function(attributeName) {
        query.groupBy(association.join.child + '.' + attributeName);
      });
    }
    this.from(association.join.child);
    this.leftJoin(association.join.parent, function() {
      this.on(association.join.parent + '.' + association.join.parentKey, '=', association.join.child + '.' + association.join.childKey);
    }).as(association.join.alias);

  }, function() {
    this.on(select.alias + '.' + association.junction.parentKey, association.join.alias + '.' + junctionAlias);
  });

  // orderBy
  if (association.join.criteria.sort) {
    _.keys(association.join.criteria.sort).forEach(function (toSort) {
      var direction = association.join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
      var sortAlias = self.createAlias(association.name, toSort);
      select.query.orderBy(association.join.alias + '.' + sortAlias, direction);
    });
  }
};

GenericDialect.prototype.selectSort = function(connection,select) {
  if (select.options.sort) {
    _.keys(select.options.sort).forEach(function(toSort) {
      var direction = select.options.sort[toSort] === 1 ? 'ASC' : 'DESC';
      select.query.orderBy(select.alias + '.' + toSort, direction);
    });
  }
};

GenericDialect.prototype.selectSkipLimit = function(connection,select) {
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
      select.query.andWhere(select.alias + '.' + select.pk, 'IN', connection.client.select('*').from(function() {
        var query = this;
        this.select(select.pk);
        this.from(select.tableName);
        new self.CriteriaProcessor(connection, select.tableName, select.options, this);
        if (select.options.skip) {
          this.offset(select.options.skip);
        }
        if (select.options.limit) {
          this.limit(select.options.limit);
        }
        if (select.options.sort) {
          _.keys(select.options.sort).forEach(function(toSort) {
            var direction = select.options.sort[toSort] === 1 ? 'ASC' : 'DESC';
            query.orderBy(select.tableName + '.' + toSort, direction);
          });
        }
        this.as('SKLMT');
      }));
    }
  }
};

GenericDialect.prototype.join = function(select, association) {
  var connection = select.connection;
  var join = association.join;
  if (join.criteria && (join.criteria.skip || join.criteria.limit)) {
    this.joinSkipLimit(select, association);
  } else {
    var self = this;
    var childDefinition = connection.getCollection(join.child).definition;
    var parent = join.parent;
    if (parent === select.tableName) {
      parent = select.alias;
    }
    if (join.select === false) {
      var joinAlias = self.createAlias(join.parent + '_' + join.parentKey, join.child + '_' + join.childKey);
      select.query.leftJoin(join.child + ' as ' + joinAlias, function() {
        this.on(parent + '.' + join.parentKey, '=', joinAlias + '.' + join.childKey);
        this.on('joins.id', '=', association.id);
      });
    }
    else {
      var junctionTable = _.find(select.options.joins, function(junctionTable) {
        return (junctionTable.select === false && junctionTable.child === join.parent && junctionTable.alias === join.alias);
      });
      if (junctionTable) {
        parent = self.createAlias(junctionTable.parent + '_' + junctionTable.parentKey, junctionTable.child + '_' + junctionTable.childKey);
      }
      select.query.leftJoin(join.child + ' as ' + join.alias, function() {
        this.on(parent + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
        this.on('joins.id', '=', association.id);
        if (join.criteria) {
          new self.CriteriaProcessor(connection, join.alias, join.criteria, this, 'on');
        }
      });
      //ADD COLUMN WITH ALIAS IN SELECTION
      if (join.select) {
        join.select.forEach(function(columnName) {
          if (childDefinition[columnName]) {
            var childAlias = self.createAlias(association.name, columnName);
            var column = join.alias + '.' + columnName + ' as ' + childAlias;
            if (select.selection.indexOf(column) < 0) {
              select.selection.push(column);
              select.query.select(column);
            }
          }
        });
      }
      if (join.criteria && join.criteria.sort) {
        _.keys(join.criteria.sort).forEach(function(toSort) {
          var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
          select.query.orderBy(join.alias + '.' + toSort, direction);
        });
      }
    }
  }
};

GenericDialect.prototype.joinManyToMany = function(select, association) {
  var connection = select.connection;
  var join = association.join;
  var junction = association.junction;
  if (join.criteria && (join.criteria.skip || join.criteria.limit)) {
    this.joinManyToManySkipLimit(select, association);
  } else {
    var self = this;
    var childDefinition = connection.getCollection(join.child).definition;
    var junctionAlias = self.createAlias(junction.parent + '_' + junction.parentKey, junction.child + '_' + junction.childKey);
    select.query.leftJoin(junction.child + ' as ' + junctionAlias, function() {
      this.on(select.alias + '.' + junction.parentKey, '=', junctionAlias + '.' + junction.childKey);
      this.on('joins.id', '=', association.id);
    });
    select.query.leftJoin(join.child + ' as ' + join.alias, function() {
      this.on(junctionAlias + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
      this.on('joins.id', '=', association.id);
      if (join.criteria) {
        new self.CriteriaProcessor(connection, join.alias, join.criteria, this, 'on');
      }
    });
    //ADD COLUMN WITH ALIAS IN SELECTION
    if (join.select) {
      join.select.forEach(function(columnName) {
        if (childDefinition[columnName]) {
          var childAlias = self.createAlias(association.name, columnName);
          var column = join.alias + '.' + columnName + ' as ' + childAlias;
          if (select.selection.indexOf(column) < 0) {
            select.selection.push(column);
            select.query.select(column);
          }
        }
      });
    }
    if (join.criteria && join.criteria.sort) {
      _.keys(join.criteria.sort).forEach(function(toSort) {
        var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
        select.query.orderBy(join.alias + '.' + toSort, direction);
      });
    }
  }
};

GenericDialect.prototype.joinSkipLimit = function(select, association) {
  var connection = select.connection;
  var join = association.join;
  var self = this;
  var childDefinition = connection.getCollection(join.child).definition;

  select.query.leftJoin(join.child + ' as ' + join.alias, function() {
    this.on(select.alias + '.' + join.parentKey, '=', join.alias + '.' + join.childKey);
    this.on('joins.id', '=', association.id);
    if (join.criteria) {
      new self.CriteriaProcessor(connection, join.alias, join.criteria, this, 'on');
    }
  });
  //ADD COLUMN WITH ALIAS IN SELECTION
  if (join.select) {
    join.select.forEach(function(columnName) {
      if (childDefinition[columnName]) {
        var childAlias = self.createAlias(association.name, columnName);
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
  skLmtQuery.andWhereRaw( '?? = ??', [ join.parent + '.' + join.parentKey, select.alias + '.' + select.pk ] );

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
          ++j;
        }
        var key = keys[i];

        if (join.criteria.sort[key] === 1) {
          this.andWhereRaw('??.?? > ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
        } else {
          this.andWhereRaw('??.?? < ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
        }
      });
    }
  });

  select.selection.push(skLmtQuery.as(skLmtAlias));
  select.query.select(skLmtQuery.as(skLmtAlias));

  if (join.criteria.skip && join.criteria.limit) {
    select.query.andHaving(function() {
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      this.andWhere(skLmtAlias, '<=', join.criteria.limit + join.criteria.skip);
    });
    select.query.orHaving(skLmtAlias, '=', 1);
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.skip) {
    select.query.andHaving(function() {
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      this.orWhere(skLmtAlias, '=', 1);
    });
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.limit) {
    select.query.andHaving(skLmtAlias, '<=', join.criteria.limit);
  }

  if (join.criteria && join.criteria.sort) {
    _.keys(join.criteria.sort).forEach(function(toSort) {
      var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
      select.query.orderBy(join.alias + '.' + toSort, direction);
    });
  }

};

GenericDialect.prototype.joinManyToManySkipLimit = function(select, association) {
  var self = this;
  var connection = select.connection;
  var join = association.join;
  var junction = association.junction;
  var childDefinition = connection.getCollection(join.child).definition;
  var parent = join.parent;
  var junctionAlias = this.createAlias('junction_', join.alias);
  if (parent === select.tableName) {
    parent = select.alias;
  }

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
      this.on('joins.id', '=', association.id);
    }).as(join.alias);
  }, function() {
    this.on(select.alias + '.' + junction.parentKey, join.alias + '.' + junctionAlias);
    this.on('joins.id', '=', association.id);
  });

  join.select.forEach(function(columnName) {
    var childAlias = self.createAlias(association.name, columnName);
    select.query.select(join.alias + '.' + columnName + ' as ' + childAlias);
  });

  var skLmtAlias = this.createAlias('_SKLMT_', join.alias);
  var skLmtQuery = connection.client(join.child + ' as ' + skLmtAlias).select(connection.client.raw('count(1)+1'));

  skLmtQuery.leftJoin(join.parent, join.parent + '.' + join.parentKey, skLmtAlias + '.' + join.childKey);
  skLmtQuery.leftJoin(junction.parent, junction.parent + '.' + junction.parentKey, junction.child + '.' + junction.childKey);
  skLmtQuery.andWhereRaw( '??.?? = ??.??', [ junction.parent, junction.parentKey, select.alias, select.pk ] );

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
          this.andWhereRaw('??.?? = ??.??', [join.alias, keys[j], skLmtAlias, keys[j]]);
          ++j;
        }
        var key = keys[i];
        if (join.criteria.sort[key] === 1) {
          this.andWhereRaw('??.?? > ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
        } else {
          this.andWhereRaw('??.?? < ??.??', [join.alias, keys[i], skLmtAlias, keys[i]]);
        }
      });
    }
  });

  select.selection.push(skLmtQuery.as(skLmtAlias));
  select.query.select(skLmtQuery.as(skLmtAlias));

  if (join.criteria.skip && join.criteria.limit) {
    select.query.andHaving(function() {
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      this.andWhere(skLmtAlias, '<=', join.criteria.limit + join.criteria.skip);
    });
    select.query.orHaving(skLmtAlias, '=', 1);
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.skip) {
    select.query.andHaving(function() {
      this.andWhere(skLmtAlias, '>', join.criteria.skip);
      this.orWhere(skLmtAlias, '=', 1);
    });
    association.skipFirst = skLmtAlias;

  } else if (join.criteria.limit) {
    select.query.andHaving(skLmtAlias, '<=', join.criteria.limit);
  }

  if (join.criteria && join.criteria.sort) {
    _.keys(join.criteria.sort).forEach(function(toSort) {
      var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
      select.query.orderBy(join.alias + '.' + toSort, direction);
    });
  }
};

GenericDialect.prototype.update = function(connection, collection, opts, data) {
  data = this.Utils.prepareValues(data);
  var tableName = this.normalizeTableName(collection.tableName);
  var options = this.Utils.normalizeCriteria(opts);
  var updateQuery = new this.CriteriaProcessor(connection, tableName, options, connection.client(tableName)).getQuery();
  return updateQuery.update(data);
};

GenericDialect.prototype.delete = function(connection, collection, opts) {
  var tableName = this.normalizeTableName(collection.tableName);
  var options = this.Utils.normalizeCriteria(opts);
  var deleteQuery = new this.CriteriaProcessor(connection, tableName, options, connection.client(tableName)).getQuery();
  return deleteQuery.del();
};

GenericDialect.prototype.createTable = function(connection, collection, definition) {
  var self = this;
  var tableName = this.normalizeTableName(collection.tableName);
  return connection.client.schema.createTableIfNotExists(tableName, function(table) {
    _.keys(definition).forEach(function(attrName) {
      self.defineColumn(table, attrName, definition[attrName]);
    });
  });
};

GenericDialect.prototype.dropTable = function(connection, collection) {
  var tableName = this.normalizeTableName(collection.tableName);
  return connection.client.schema.dropTableIfExists(tableName);
};

GenericDialect.prototype._getDenseRank = function(connection, sort, tableAlias, pk, queryAlias) {
  return connection.client.raw('(dense_rank() over (partition by ??.?? ' + sort + ')) as ??',[tableAlias, pk, queryAlias]);
};

module.exports = GenericDialect;
