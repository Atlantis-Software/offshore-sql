var Knex = require('./node_modules/knex');
var asynk = require('asynk');
var SQL = require('./lib/sql');
var BuffersHandler = require('./lib/associationsUtils/buffersHandler');
var _ = require('underscore');
var Errors = require('waterline-errors').adapter;
var Cursor = require('waterline-cursor');
var util = require('util');
var LOG_QUERIES = true;
var LOG_ERRORS = true;

var oracleDialect = require('./lib/dialects/oracleDialect.js');
var mysqlDialect = require('./lib/dialects/mysqlDialect.js');

module.exports = (function () {
    var connections = {};

    var adapter = {
        defaults: {
            // For example:
            // port: 3306,
            // host: 'localhost'
            dbType: '',
            user: '',
            password: '',
            // If setting syncable, you should consider the migrate option, 
            // which allows you to set how the sync will be performed.
            // It can be overridden globally in an app (config/adapters.js) and on a per-model basis.
            //
            // drop   => Drop schema and data, then recreate it
            // alter  => Drop/add columns as necessary, but try 
            // safe   => Don't change anything (good for production DBs)
            migrate: 'safe'
        },
        dialect: null,
        registerConnection: function (connection, collections, cb) {
            switch(connection.dbType) {
                case 'mysql':
                    this.dialect = new mysqlDialect();
                    break;
                case 'oracle':
                    this.dialect = new oracleDialect();
            }
            console.log('connection',connection);
            if (!connection.identity)
                return cb("Errors.IdentityMissing");
            if (connections[connection.identity])
                return cb("Errors.IdentityDuplicate");
            var client = Knex({client: connection.dbType, connection: connection,
                pool: {
                    min: 0,
                    max: 10
                }, debug: LOG_QUERIES
            });
            // Store the connection
            connections[connection.identity] = {
                config: connection,
                collections: collections,
                client: client
            };

            return cb();
        },
        commit:function(connectionName,cb){
            var connectionObject = connections[connectionName];
            var client = connectionObject.client;
            client.raw('commit').then(function(){
                cb();
            }).catch(function(e){
                cb(e);
            });
        },
        define: function (connectionName, collectionName, definition, cb, connection) {

            // Define a new "table" or "collection" schema in the data store
            var self = this;
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            if (!collection) {
                return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
            }
            var client = connectionObject.client;
            var tableName = collectionName;
            // TODO logic here
            client.schema.createTable(tableName, function (table) {
                _.keys(definition).forEach(function (attrName) {
                    var attr = definition[attrName];
                    if (attr.autoIncrement && attr.primaryKey)
                        table.increments(attrName).primary();
                    else
                        SQL.defineColumn(table, attrName, attr);
                });
            }).then(function () {
                self.describe(connectionName, collectionName, function (err) {
                    cb(err, null);
                });
            }).catch(function (e) {
                if (LOG_ERRORS)
                    console.log('#Error :', collectionName, e);
                cb(e);
            });
        },
        describe: function (connectionName, collectionName, cb, connection) {
            var self = this;
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            if (!collection) {
                return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
            }

            var client = connectionObject.client;
            var tableName = collectionName;
            self.dialect.describe(client, tableName, function (err, schema) {
                if (err && err.code === 'ER_NO_SUCH_TABLE'){
                        if(LOG_QUERIES) console.log('Table',collectionName,'doesn\'t exist, creating it ...');
                        return cb();
                }
                if(err) {
                    if (LOG_ERRORS)
                        console.log('#Error :', err);
                    return cb(err);
                }
                var normalizedSchema = self.dialect.normalizeSchema(schema, collection.attributes);
                collection.schema = normalizedSchema;
                cb(null, normalizedSchema);
            }, LOG_QUERIES);
        }
        ,
        find: function (connectionName, collectionName, options, cb) {

            if (options.groupBy || options.sum || options.average || options.min || options.max) {
                if (!options.sum && !options.average && !options.min && !options.max) {
                    return cb(Errors.InvalidGroupBy);
                }
            }
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            var client = connectionObject.client;
            var schema = collection.waterline.schema;
            var attributes = collection.attributes;
            var criteria = SQL.normalizeCriteria(options, attributes);
            /************* surpassing the binary types probleme *******************/
            console.log("dbType: ",connectionObject.config.dbType);
            if(connectionObject.config.dbType === 'oracle'){
            criteria.select = _.filter(_.keys(collection.definition),function(attr){
                var type = _.isObject(collection.definition[attr])?collection.definition[attr].type:collection.definition[attr];
                if(['binary','array','json'].indexOf(type.toLowerCase())<0)
                    return attr;
            });
            console.log(criteria.select);
            }
            /************* ******************** *******************/
            /* replace attributes names by columnNames */
            SQL.select(client, collectionName, schema, criteria).then(function (result) {
                cb(null, result);
            }).catch(function (e) {
                if (LOG_ERRORS)
                    console.log('#Error :', e);
                cb(e);
            });
        },
        drop: function (connectionName, collectionName, relations, cb, connection) {
            if (typeof relations === 'function') {
                cb = relations;
                relations = [];
            }
            var connectionObject = connections[connectionName];
            var client = connectionObject.client;

            function dropTable(item, callback) {
                client.schema.dropTableIfExists(collectionName).then(function (result) {
                    callback(null, result);
                }).catch(function (e) {
                    if (LOG_ERRORS)
                        console.log('#Error :', e);
                    callback(e);
                });
            }

            asynk.each(relations, dropTable).args(asynk.item, asynk.callback).serie(function (err, result) {
                if (err)
                    return cb(err);
                dropTable(collectionName, cb);
            }, [null]);

        },
        createEach: function (connectionName, collectionName, valuesList, cb, connection) {
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            var client = connectionObject.client;
            var records = [];
            asynk.each(valuesList, function (data, cb) {

                _.keys(data).forEach(function (value) {
                    data[value] = SQL.prepareValue(data[value]);
                });
                var schema = collection.waterline.schema;

                client(collectionName).insert(data).then(function (results) {
                    records.push(results.insertId);
                    cb();
                }).catch(function (e) {
                    if (LOG_ERRORS)
                        console.log('#Error :', e);
                    return cb(e);
                });
            }).args(asynk.item, asynk.callback).parallel(function (err) {
                if (err)
                    return cb(err);

                var pk = 'id';

                Object.keys(collection.definition).forEach(function (key) {
                    if (!_.has(collection.definition[key], 'primaryKey'))
                        return;
                    pk = key;
                });

                if (!records.length) {
                    return cb(null, []);
                }

                cb(null, null);
            }, [null]);
        },
        create: function (connectionName, collectionName, data, cb, connection) {
            var self = this;
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            var tableName = collectionName;
            var client = connectionObject.client;
            //var _insertData = lodash.cloneDeep(data);
            var _insertData = _.clone(data);

            _.keys(_insertData).forEach(function (key) {
                _insertData[key] = SQL.prepareValue(_insertData[key]);
            });
            var autoInc = null;
            
            _.keys(collection.definition).forEach(function (key) {
                if (!_.has(collection.definition[key], 'autoIncrement'))
                    return;
                autoInc = key;
            });

            var insertQuery = client(tableName).insert(_insertData);
            if(connectionObject.config.dbType === 'oracle' && autoInc) insertQuery.returning(autoInc);
            insertQuery.then(function (result) {
                var autoIncData = {};
                if (autoInc) {
                    autoIncData[autoInc] = result[0];
                }
                var values = _.extend({}, _insertData, autoIncData);
                self.commit(connectionName,function(err){
                    if(!err){
                        cb(null, values);
                    }
                    else{
                        cb(err);
                    }
                });
            }).catch(function (e) {
             if (LOG_ERRORS)
                console.log('#Error', e);
             return cb(e);
            });

        },
        destroy: function (connectionName, collectionName, options, cb, connection) {
            var self = this;
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            var tableName = collectionName;
            var client = connectionObject.client;
            var criteria = SQL.normalizeCriteria(options, collection.attributes);
            /************* surpassing the binary types probleme *******************/
            if(connectionObject.config.dbType === 'oracle'){
            criteria.select = _.filter(_.keys(collection.definition),function(attr){
                var type = _.isObject(collection.definition[attr])?collection.definition[attr].type:collection.definition[attr];
                if(['binary','array','json'].indexOf(type.toLowerCase())<0)
                    return attr;
            });
            console.log(criteria.select);
            }
            /************* ******************** *******************/
            asynk.add(function (callback) {
                SQL.select(client, collectionName, null, criteria).then(function (result) {
                    callback(null, result);
                }).catch(function (e) {
                    if (LOG_ERRORS)
                        console.log('#Error :', e);
                    callback(e);
                });
            }).args(asynk.callback).alias('findRecords')
                    .add(function (callback) {
                        SQL.destroy(client, collectionName, criteria).then(function (result) {
                            self.commit(connectionName,function(err){
                                if(!err){
                                    callback(null,result);
                                }
                                else{
                                    callback(null);
                                }
                            });
                        }).catch(function (e) {
                            if (LOG_ERRORS)
                                console.log('#Error :', e);
                            callback(e);
                        });
                    }).args(asynk.callback)
                    .serie(cb, [null, asynk.data('findRecords')]);

        },
        update: function (connectionName, collectionName, options, values, cb, connection) {
            var connectionObject = connections[connectionName];
            var collection = connectionObject.collections[collectionName];
            var client = connectionObject.client;
            var schema = collection.waterline.schema;
            var criteria = SQL.normalizeCriteria(options, collection.attributes);
            var ids = [];
            var pk = 'id';
            SQL.select(client, collectionName, schema, criteria).then(function (results) {
                _.keys(collection.definition).forEach(function (key) {
                    if (!_.has(collection.definition[key], 'primaryKey'))
                        return;
                    pk = key;
                });

                if (results.length === 0) {
                    return cb(null, []);
                }
                results.forEach(function (result) {
                    ids.push(result[pk]);
                });

                _.keys(values).forEach(function (value) {
                    values[value] = SQL.prepareValue(values[value]);
                });
                SQL.update(client, collectionName, criteria, values).then(function () {
                var resultCriteria;
                resultCriteria = {where: {}};
                resultCriteria.where[pk] = ids;
                SQL.select(client, collectionName, schema, resultCriteria).then(function (updatedRecords) {
                    cb(null, updatedRecords);
                });
            });/*.catch(function (e) {
                if (LOG_ERRORS)
                    console.log('#Error :', e);
                cb(e);
            })*/;

        });
        },
        query: function(connectionName, collectionName, query, data, cb, connection) {
            var self = this;
            var connectionObject = connections[connectionName];
            var client = connectionObject.client;
            if (_.isFunction(data)) {
                cb = data;
                data = null;
            }
            data = data || [];
            data.forEach(function(param,index){
                var pos = index + 1;
                var value = param;
                if(_.isString(param)) value = self.dialect.escapeString(param);
                query = query.replace('$'+pos,value);
            });
            if (LOG_QUERIES) {
                console.log('Executing QUERY query: ' + query);
            }
            client.raw(query).then(function(result){
                cb(null,result);
            }).catch(function(e){
                cb(e);
            });
        },
        join: function (connectionName, collectionName, options, cb, connection) {

            Cursor({
                instructions: options,
                nativeJoins: true,
                $find: function (collectionName, criteria, _cb) {
                    return adapter.find(connectionName, collectionName, criteria, _cb);
                },
                $getPK: function (collectionName) {
                    if (!collectionName)
                        return;
                    return _getPkColumnName(connectionName, collectionName);
                },
                $populateBuffers: function populateBuffers(options, next) {
                    var buffers = options.buffers;
                    var instructions = options.instructions;
                    var populationsInfos = instructions.instructions;
                    var connectionObject = connections[connectionName];
                    var client = connectionObject.client;
                    var collection = connectionObject.collections[collectionName];
                    var _schema = collection.waterline.schema;
                    var parentCriteria = SQL.normalizeCriteria(instructions, collection.attributes);
                    var parentPkColumnName = _getPkColumnName(connectionName, collectionName);
                    var parentPkAttributeName = _getPkAttributeName(connectionName, collectionName, parentPkColumnName);
                    var buffersHandler = new BuffersHandler(connectionObject, buffers, parentPkAttributeName);
                    var queries = {};
                    SQL.select(client, collectionName, _schema, parentCriteria).then(function (result) {
                        buffersHandler.setParents(result);
                        _.keys(populationsInfos).forEach(function (attributeToPopulate) {
                            var childCriteria;
                            var populationObject = populationsInfos[attributeToPopulate];
                            if (populationObject.strategy.strategy === 2 ) {
                                var childInfos = populationObject.instructions[0];
                                var schema = connectionObject.collections[childInfos.child].waterline.schema;
                                childCriteria = childInfos.criteria;
                                if (!childCriteria)
                                    childCriteria = {};
                                if (!childCriteria.where)
                                    childCriteria.where = {};
                                childCriteria = SQL.normalizeCriteria(childCriteria, connectionObject.collections[childInfos.child].attributes);
                                queries[attributeToPopulate] = {schema: schema, collectionName: childInfos.child, criteriasByParent: [], foreignKey: childInfos.childKey, strategy: populationObject.strategy.strategy};
                            }else if(populationObject.strategy.strategy === 3){
                               var schema = connectionObject.collections[populationObject.instructions[1].child].waterline.schema;
                               queries[attributeToPopulate] = {schema : schema,
                                                               parentCollection : populationObject.instructions[0].parent,
                                                               childCollection : populationObject.instructions[1].child,
                                                               jonctionCollection : populationObject.instructions[1].parent,
                                                               criteria : {select : populationObject.instructions[1].select, criteria : populationObject.instructions[1].criteria},
                                                               jonctionParentFK : populationObject.instructions[0].childKey,
                                                               jonctionChildFK : populationObject.instructions[1].parentKey,
                                                               parentPk : populationObject.instructions[0].parentKey,
                                                               childPK : populationObject.instructions[1].childKey,
                                                               parentIds : []
                                };
                            }
                            buffers.parents.forEach(function (parent) {
                                var splitedChildren = SQL.splitStrategyOneChildren(parent);
                                var buffer = buffersHandler.createBuffer(parent, attributeToPopulate, populationObject);
                                /* if parent has a reference to the child and there is a child result splitted */
                                if (parent[buffer.keyName] && splitedChildren && splitedChildren[buffer.keyName]) {
                                    var splitedChild = splitedChildren[buffer.keyName];
                                    /* if this child belongs really to this parent */
                                    if (splitedChild[buffer.childKey] && splitedChild[buffer.childKey] === parent[buffer.keyName])
                                        buffer = buffersHandler.addChildToBuffer(buffer, splitedChild);
                                }
                                buffersHandler.saveBuffer(buffer);
                                if (populationObject.strategy.strategy === 2) {
                                    var criteria = _.clone(childCriteria);
                                    criteria.where = _.clone(childCriteria.where);
                                    criteria.where[childInfos.childKey] = parent[parentPkAttributeName];
                                    queries[attributeToPopulate].criteriasByParent.push(criteria);
                                }else if(populationObject.strategy.strategy === 3){
                                    queries[attributeToPopulate].parentIds.push(parent[parentPkAttributeName]);
                                }
                            });
                        });
                        asynk.each(_.keys(queries), function (toPopulate, nextAttr) {
                                if(queries[toPopulate].strategy === 2){
                                SQL.makeUnion(client, queries[toPopulate]).then(function (result) {
                                    var childRecords = result;
                                    childRecords.forEach(function (childRecord) {
                                        buffersHandler.searchBufferAndAddChild(childRecord, queries[toPopulate].foreignKey, toPopulate);
                                    });
                                    nextAttr();
                                });
                                }else{
                                    SQL.manyToManyUnion(client ,queries[toPopulate]).then(function(result){
                                        var childRecords = result;
                                        childRecords.forEach(function (childRecord) {
                                            buffersHandler.searchBufferAndAddChild(childRecord, '___'+queries[toPopulate].jonctionParentFK, toPopulate);
                                        });
                                        nextAttr();
                                    });
                                }
                        }).args(asynk.item, asynk.callback).serie(function () {
                            next();
                        });
                    });
                }

            }, cb);

        }


    };

    function _getPkColumnName(connectionIdentity, collectionIdentity) {
        var collectionDefinition;
        try {
            collectionDefinition = connections[connectionIdentity].collections[collectionIdentity].definition;

            //return lodash.find(Object.keys(collectionDefinition), function _findPK(key) {
            return _.find(_.keys(collectionDefinition), function _findPK(key) {
                var attrDef = collectionDefinition[key];
                if (attrDef && attrDef.primaryKey)
                    return key;
                else
                    return false;
            }) || 'id';
        }
        catch (e) {
            throw new Error('Unable to determine primary key for collection `' + collectionIdentity + '` because ' +
                    'an error was encountered acquiring the collection definition:\n' + util.inspect(e, false, null));
        }
    }

    function _getPkAttributeName(connectionIdentity, collectionIdentity, pk) {

        var attributes;
        try {
            attributes = connections[connectionIdentity].collections[collectionIdentity].attributes;
            var modelPk;
            _.keys(attributes).forEach(function (key) {
                var columnName = attributes[key].columnName || key;
                if (columnName === pk) {
                    //if (columnName.toUpperCase() === pk) {    
                    modelPk = key;
                }
                return;
            });
            return modelPk;
        }
        catch (e) {
            throw new Error('Unable to determine model primary key for collection `' + collectionIdentity + '` because ' +
                    'an error was encountered acquiring the collection definition:\n' + require('util').inspect(e, false, null));
        }
    }




    return adapter;

})();





