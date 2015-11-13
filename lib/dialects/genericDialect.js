var Knex = require('knex');
var _ = require('underscore');
var Utils = require('../utils');
var CriteriaProcessor = require('../criteriaProcessor');

var GenericDialect = function() {};

GenericDialect.prototype.stringDelimiter = "'";

/* Retrieving infos about tables */
GenericDialect.prototype.describe = function(connection, collection,callback) {
    callback("ERROR: UNDEFINED_METHOD");
};

/* Normalizing schema */
GenericDialect.prototype.normalizeSchema = function(schema) {
    throw new Error("DIALECT UNDEFINED METHOD: normalizeSchema");
};


GenericDialect.prototype.normalizeTableName = function(tableName) {
    return tableName;
};

GenericDialect.prototype.sqlEscapeString = function(str) {
    return str;
};

GenericDialect.prototype.sqlEscapeTableName = function(tableName) {
    return tableName;
};

GenericDialect.prototype.sqlEscapeColumnName = function(columnName) {
    return columnName;
};

GenericDialect.prototype.createAlias = function(tableAlias,columnName) {
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
                column = table.float(attrName);
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
    if (attribute.primaryKey)
        column.primary();


    else if (attribute.unique)
        column.unique();

    if (attribute.required || attribute.notNull)
        column.notNullable();

    if (attribute.index)
        column.index();

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
    return insertQuery.then(function(result){
        var pkval = {};
        if (collection.definition[pk].autoIncrement) {
            pkval[pk] = result[0];
        }
        return _.extend({}, record, pkval);
    });
};

GenericDialect.prototype.select = function (connection, collection, opts) {
    var self = this;
    var tableName = this.normalizeTableName(collection.tableName);
    var pk = connection.getPk(tableName);
    var definition = collection.definition;
    var options = Utils.normalizeCriteria(opts);
    var selection = [];
    if (!options.select) {
        _.keys(definition).forEach(function (field) {
            selection.push(tableName + '.' + field);
        });
    }
    else {
        options.select.forEach(function (field) {
            selection.push(tableName + '.' + field);
        });
        delete options.select;
    }
    var query; 
    if ((options.limit || options.skip) && options.joins) {
        var subquery = new CriteriaProcessor(connection, tableName, options, connection.client(tableName)).processAggregates().getQuery().select(selection).as(tableName);
        //var subquery = new CriteriaProcessor(connection, tableName, options).select(selection).as(tableName);
        selection = [];
        query = connection.client.select().from(subquery);
    }
    else {
        query = new CriteriaProcessor(connection, tableName, options, connection.client(tableName)).processAggregates().getQuery();
        //query = new CriteriaProcessor(connection, tableName, options);
    }

    if (options.joins) {
        options.joins.forEach(function (join) {
            join.parent = self.normalizeTableName(join.parent);
            join.child = self.normalizeTableName(join.child);
            var childDefinition = connection.getCollection(join.child).definition;

            if (join.criteria && (join.criteria.skip || join.criteria.limit)) {
                var subQuery = connection.client(join.child + ' as ' + join.alias);
                var criteriaSkip = join.criteria.skip;
                delete join.criteria.skip;
                var criteriaLimit = join.criteria.limit;
                delete join.criteria.limit;
                subQuery = new CriteriaProcessor(connection, tableName, options,subQuery, 'where').getQuery();
                subQuery = new CriteriaProcessor(connection, join.alias, join.criteria, subQuery, 'where').getQuery();
                if (join.select) {
                    var subSelection = [];
                    join.select.forEach(function (columnName) {
                        if (childDefinition[columnName]) {
                            var childAlias = self.createAlias(join.alias, columnName);
                            subSelection.push(join.alias + '.' + columnName);
                            selection.push(join.alias + '.' + columnName + ' as ' + childAlias);
                        }
                    });

                    var sort = '';
                    if (join.criteria.sort) {
                        _.keys(join.criteria.sort).forEach(function (toSort) {
                            var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
                            if ( sort !== '' ) {
                                sort += ', ';
                            }
                            else {
                                sort += ' order by ';
                            }
                            sort += '"' + join.alias + '"."' + toSort + '" ' + direction;
                        });
                    }

                    subSelection.push(Knex.raw('(row_number() over (partition by "' + tableName + '"."' + pk +'"' + sort + ')) "row_"'));
                    subQuery = subQuery.select(subSelection);
                }

                if (join.junctionTable) {
                    var junctionTable = _.filter(options.joins,function(junction){
                        return (junction.select === false && junction.alias === join.alias);
                    });
                    if (junctionTable.length === 1) {
                        junctionTable = junctionTable[0];
                        subQuery.leftJoin(join.parent,join.parent + '.' + join.parentKey,join.alias + '.' + join.childKey);
                        subQuery.leftJoin(junctionTable.parent,junctionTable.parent + '.' + junctionTable.parentKey,junctionTable.child + '.' + junctionTable.childKey);

                    }
                    else {
                        console.log('error junctionTable',junctionTable.length);
                    }
                }
                else {
                    subQuery.leftJoin(join.parent,join.parent + '.' + join.parentKey,join.alias + '.' + join.childKey);
                }
                subQuery = subQuery.as(join.alias);


                query.leftJoin(subQuery,join.parent + '.' + join.parentKey,join.alias + '.' + join.childKey);

                query.where(function() {
                    if (criteriaSkip) {
                        this.whereRaw('"row_" > ?', criteriaSkip);
                    }

                    if (criteriaLimit) {
                        this.whereRaw('"row_" <= ?', criteriaLimit + (criteriaSkip || 0));
                    }

                    this.orWhereNull(join.alias + '.' + join.childKey);
                })
            }
            else {
                if (join.select === false) {
                    query.leftJoin(join.child, function() {
                        this.on(join.parent + '.' + join.parentKey, '=', join.child + '.' + join.childKey);
                    });
                }
                else {
                    query.leftJoin(join.child + ' as ' + join.alias, function() {
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
                                selection.push(join.alias + '.' + columnName + ' as ' + childAlias);
                            }
                        });
                    }
                    if (join.criteria && join.criteria.sort) {
                        _.keys(join.criteria.sort).forEach(function (toSort) {
                            var direction = join.criteria.sort[toSort] === 1 ? 'ASC' : 'DESC';
                            query = query.orderBy(join.alias + '.' + toSort, direction);
                        });
                    }
                }
            }
        });
    }

    return query.select(selection);
};

GenericDialect.prototype.update = function (connection, collection, opts, data) {
    var tableName = this.normalizeTableName(collection.tableName);
    var options = Utils.normalizeCriteria(opts);
    var updateQuery = new CriteriaProcessor(connection, tableName, options, connection.client(tableName)).processAggregates().getQuery();
    return updateQuery.update(data);
};

GenericDialect.prototype.delete = function (connection, collection, opts) {
    var tableName = this.normalizeTableName(collection.tableName);
    var options = Utils.normalizeCriteria(opts);
    var deleteQuery = new CriteriaProcessor(connection, tableName, options, connection.client(tableName)).processAggregates().getQuery();
    return deleteQuery.del();
};

GenericDialect.prototype.createTable = function(connection, collection, definition){
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



