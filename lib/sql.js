var Knex = require('knex');
var _ = require('underscore');
var WhereProcessor = require('./whereProcessor');
var sql = {
    defineColumn: function (table, attrName, attribute) {
        var column;
        if (attribute.autoIncrement)
            table.increments(attrName);
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
                case 'json':
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
    },
    normalizeSchema: function (schema) {
        return _.reduce(schema, function (memo, field) {

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
    },
    select: function (client, collectionName, schema, options, sqlMode) {
        this.schema = schema;
        var query;
        if (!sqlMode)
            query = client(collectionName);
        else
            query = client.select(options.select).from(collectionName);

        if (options.where)
            query = new WhereProcessor(query, collectionName, options.where).process();
        if (options.sum)
            options.sum.forEach(function (keyToSum) {
                query = query.sum(keyToSum + ' as ' + keyToSum);
            });
        if (options.average)
            options.average.forEach(function (keyToAvg) {
                query = query.avg(keyToAvg + ' as ' + keyToAvg);
            });
        if (options.min)
            options.min.forEach(function (keyToMin) {
                query = query.min(keyToMin + ' as ' + keyToMin);
            });
        if (options.max)
            options.max.forEach(function (keyToMax) {
                query = query.max(keyToMax + ' as ' + keyToMax);
            });

        if (options.groupBy)
            options.groupBy.forEach(function (groupKey) {
                query = query.groupBy(groupKey);
            });

        if (options.limit)
            query = query.limit(options.limit);

        if (options.skip)
            query = query.offset(options.skip);

        if (options.sort)
            _.keys(options.sort).forEach(function (toSort) {
                var direction = options.sort[toSort] === 1 ? 'ASC' : 'DESC';
                query = query.orderBy(toSort, direction);
            });

        query = this.processPopulations(query, options);
        if (!sqlMode)
            query = query.select(options.select);
        return query;
    },
    normalizeCriteria: function (criteria, attributes) {
        var _criteria = _.clone(criteria);
        /*if (_criteria.select) {
            _criteria.select = _.map(_criteria.select, function (attr) {
                return attr;
            });
        }*/
        //this.normalizeWhere(_criteria.where, attributes);
        this.normalizeAgregates(_criteria, attributes);
        //this.normalizeSort(_criteria.sort, attributes);
        return _criteria;
        //return criteria;
    },
    dropTable: function (client, collectionName) {
        return client.schema.dropTableIfExists(collectionName);
    },
    destroy: function (client, collectionName, options) {
        var deleteQuery = client(collectionName);
        if (options.where)
            deleteQuery = new WhereProcessor(deleteQuery, collectionName, options.where).process();
        return deleteQuery.del();
    },
    insert: function (client, collectionName, record) {
        return client(collectionName).insert(record);
    },
    update: function (client, collectionName, options, data) {
        var updateQuery = client(collectionName);
        if (options.where)
            updateQuery = new WhereProcessor(updateQuery, collectionName, options.where).process();
        return updateQuery.update(data);
    },
    prepareValue: function (value) {
        if (_.isUndefined(value) || value === null)
            return value;

        // Cast functions to strings
        if (_.isFunction(value)) {
            value = value.toString();
        }

        // Store Arrays and Objects as strings
        if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
            try {
                value = JSON.stringify(value);
            } catch (e) {
                // just keep the value and let the db handle an error
                value = value;
            }
        }


        return value;
    },
    normalizeWhere: function (where, attributes) {
        if (!where)
            return;
        var self = this;
        var whereKeys = _.keys(where);
        whereKeys.forEach(function (columnName) {
            if (_.isObject(where[columnName]))
                self.normalizeWhere(where[columnName], attributes);
        });
    },
    normalizeAgregates: function (criteria, attributes) {
        if (!criteria.sum && !criteria.average && !criteria.min && !criteria.max) {
            return;
        }
        criteria.isThereAgreagtes = true;

        criteria.select = [];
        if (criteria.groupBy) {
            if (!_.isArray(criteria.groupBy))
                criteria.groupBy = [criteria.groupBy];
            criteria.select = criteria.select.concat(criteria.groupBy);
        }

        if (criteria.sum) {
            if (!_.isArray(criteria.sum))
                criteria.sum = [criteria.sum];
            criteria.sum.forEach(function (key) {
                var i = criteria.sum.indexOf(key);
                criteria.sum[i] = key;
            });
        }
        if (criteria.average) {
            if (!_.isArray(criteria.average))
                criteria.average = [criteria.average];
            criteria.average.forEach(function (key) {
                var i = criteria.average.indexOf(key);
                criteria.average[i] = key;
            });
        }
        if (criteria.max) {
            if (!_.isArray(criteria.max))
                criteria.max = [criteria.max];
            criteria.max.forEach(function (key) {
                var i = criteria.max.indexOf(key);
                criteria.max[i] = key;
            });
        }
        if (criteria.min) {
            if (!_.isArray(criteria.min))
                criteria.min = [criteria.min];
            criteria.min.forEach(function (key) {
                var i = criteria.min.indexOf(key);
                criteria.min[i] = key;
            });
        }
    },
    normalizeSort: function (sort, attributes) {
    },
    processPopulations: function (query, criteria) {
        var self = this;
        if (!criteria.instructions)
            return query;
        _.keys(criteria.instructions).forEach(function (population) {
            var populationObject = criteria.instructions[population];
            if (populationObject.strategy.strategy === 1) {
                var infos = populationObject.instructions[0];
                var childTableAlias = '_' + infos.alias;
                query = query.leftOuterJoin(infos.child + ' as ' + childTableAlias, infos.parent + '.' + infos.parentKey, '=', childTableAlias + '.' + infos.childKey);

                var parentDefinition = self.getDefinitionBytableName(infos.parent).attributes;
                var childDefinition = self.getDefinitionBytableName(infos.child).attributes;

                if (!criteria.select)
                    criteria.select = [];
                _.keys(parentDefinition).forEach(function (attributeName) {
                    if (parentDefinition[attributeName].collection)
                        return;
                    var columnName = parentDefinition[attributeName].columnName || attributeName;
                    criteria.select.push(infos.parent + '.' + columnName);
                });
                var childColumnAliasPrefix = infos.parentKeyAlias || infos.parentKey;
                _.keys(childDefinition).forEach(function (attributeName) {
                    if (childDefinition[attributeName].collection)
                        return;
                    var columnName = childDefinition[attributeName].columnName || attributeName;
                    var childColumnAlias = childColumnAliasPrefix + '___' + columnName;
                    criteria.select.push(childTableAlias + '.' + columnName + ' as ' + childColumnAlias);
                });
            }
        });
        return query;
    },
    getDefinitionBytableName: function (tableName) {
        var self = this;
        return self.schema[_.find(_.keys(self.schema), function (collection) {
            return self.schema[collection].tableName === tableName;
        })];
    },
    splitStrategyOneChildren: function (parent) {
        var splitedChild = {};
        _.keys(parent).forEach(function (key) {
            // Check if we can split this on our special alias identifier '___' and if
            // so put the result in the cache
            var split = key.split('___');
            if (split.length < 2)
                return;
            var parentKey = split[0];
            if (!_.has(splitedChild, parentKey))
                splitedChild[parentKey] = {};
            splitedChild[parentKey][split[1]] = parent[key];
            delete parent[key];
        });
        if (_.keys(splitedChild).length > 0)
            return splitedChild;
        return null;
    },
    makeUnion: function (client, queryObject) {
        var self = this;
        if (!queryObject || queryObject.criteriasByParent.length === 0)
            return null;
        var unionQuery = client.select('*').from(function () {
            var from = this;
            queryObject.criteriasByParent.forEach(function (criteria,index) {
                from.union(function(){
                    self.select(this, queryObject.collectionName, queryObject.schema, criteria, true);
                },true);
                if(index === queryObject.criteriasByParent.length -1)
                    from.as('union');
            });
        });
        return unionQuery;
    },
    manyToManyUnion : function(client,queryObject){
        var self = this;
        if (!queryObject || queryObject.parentIds.length === 0)
            return null;
        if(!queryObject.criteria.criteria.select) queryObject.criteria.criteria.select = [];
        queryObject.criteria.criteria.select.push(queryObject.childCollection+'.*');
        queryObject.criteria.criteria.select.push(queryObject.jonctionCollection+'.'+queryObject.jonctionParentFK+' as ___'+queryObject.jonctionParentFK);
        if(!queryObject.criteria.criteria.sort){
            queryObject.criteria.criteria.sort = {};
            queryObject.criteria.criteria.sort[queryObject.childPK] = 1;
        }
        var unionQuery = client.select('*').from(function () {
            var from = this;
            queryObject.parentIds.forEach(function(id, index){
                from.union(function(){
                    var req = self.select(this, queryObject.childCollection,queryObject.schema,queryObject.criteria.criteria, true);
                    req = req.innerJoin(queryObject.jonctionCollection,queryObject.jonctionCollection+'.'+queryObject.jonctionChildFK,queryObject.childCollection+'.'+queryObject.childPK);
                    req.whereIn(queryObject.childCollection+'.'+queryObject.childPK,function(){
                        this.select(queryObject.jonctionCollection+'.'+queryObject.jonctionChildFK).from(queryObject.jonctionCollection).where(queryObject.jonctionCollection+'.'+queryObject.jonctionParentFK,id);
                    });
                },true);
                if(index === queryObject.parentIds.length -1)
                    from.as('union');
            });
        });
        return unionQuery;
    }
};



module.exports = sql;



