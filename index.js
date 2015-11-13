var Knex = require('./node_modules/knex');
var asynk = require('asynk');
//var SQL = require('./lib/sql');
var Cursor = require('./lib/cursor');
var Utils = require('./lib/utils');
var _ = require('underscore');
var Errors = require('waterline-errors').adapter;
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
            var dialect;
            var knexClient;
            switch(connection.dbType) {
                case 'mysql':
                    knexClient = 'mysql';
                    dialect = new mysqlDialect();
                    break;
                case 'oracle':
                    knexClient = 'oracledb';
                    dialect = new oracleDialect();
            }
            console.log('connection',connection);
            if (!connection.identity)
                return cb("Errors.IdentityMissing");
            if (connections[connection.identity])
                return cb("Errors.IdentityDuplicate");
            var client = Knex({client: knexClient, connection: connection,
                pool: {
                    min: 0,
                    max: 10
                }, debug: LOG_QUERIES
            });
            // Store the connection
            connections[connection.identity] = {
                dialect: dialect,
                config: connection,
                collections: _.clone(collections),
                client: client,
                getCollection: function(tableName){
                    return this.collections[tableName];
                },
                getPk: function(tableName){
                    var definition = this.collections[tableName].definition;
                    var pk;
                    _.keys(definition).forEach(function (attrName) {
                        var attr = definition[attrName];
                        if (attr.primaryKey) {
                            pk = attrName;
                        }
                    });
                    return pk;
                }
            };

            return cb();
        },
        define: function (connectionName, tableName, definition, cb) {
            // Define a new "table" or return connection.collections[tableName];"collection" schema in the data store
            var self = this;
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }            
            var collection = connection.collections[tableName];
            if (!collection) {
                return cb(util.format('Unknown tableName `%s` in connection `%s`', tableName, connectionName));
            }
            connection.dialect.createTable(connection,collection,definition).asCallback(function (err,data) {
                if (err) {
                    return cb(err);
                }
                self.describe(connectionName, tableName, function (err) {
                    cb(err, null);
                });
            });
        },
        describe: function (connectionName, tableName, cb) {
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }  
            var collection = connection.collections[tableName];
            if (!collection) {
                return cb(util.format('Unknown collection `%s` in connection `%s`', tableName, connectionName));
            }
            
            var tableName = connection.dialect.normalizeTableName(tableName);
            connection.collections[tableName] = collection;
            
            connection.dialect.describe(connection, collection, function (err, schema) {
                if (err && err.code === 'ER_NO_SUCH_TABLE'){
                        if(LOG_QUERIES) console.log('Table',tableName,'doesn\'t exist, creating it ...');
                        return cb();
                }
                if(err) {
                    if (LOG_ERRORS)
                        console.log('#Error :', err);
                    return cb(err);
                }
                var normalizedSchema = connection.dialect.normalizeSchema(schema, collection.attributes);
                collection.schema = normalizedSchema;
                cb(null, normalizedSchema);
            }, LOG_QUERIES);
        }
        ,
        find: function (connectionName, tableName, options, cb) {
            if (options.groupBy || options.sum || options.average || options.min || options.max) {
                if (!options.sum && !options.average && !options.min && !options.max) {
                    return cb(Errors.InvalidGroupBy);
                }
            }
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            var collection = connection.collections[tableName];
            /* replace attributes names by columnNames */
            connection.dialect.select(connection, collection, options).asCallback(cb);
        },
        drop: function (connectionName, tableName, relations, cb) {
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            if (typeof relations === 'function') {
                cb = relations;
                relations = [];
            }

            function dropTable(tableName, callback) {
                var collection = connection.collections[tableName];
                connection.dialect.dropTable(connection,collection).asCallback(callback);
            }

            asynk.each(relations, dropTable).args(asynk.item, asynk.callback).serie(function (err, result) {
                if (err) {
                    return cb(err);
                }
                dropTable(tableName, cb);
            }, [null]);

        },
        createEach: function (connectionName, tableName, valuesList, cb) {
            var self = this;
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            var collection = connection.collections[tableName];
            var records = [];
            asynk.each(valuesList, function (data, cb) {
                connection.dialect.insert(connection,collection,Utils.prepareValues(data)).asCallback(function(err,record) {
                    if (err) {
                        return cb(err);
                    }
                    records.push(record);
                    cb(null,record);
                });
            }).args(asynk.item, asynk.callback).parallel(function (err) {
                if (err) {
                    return cb(err);
                }
                
                if (!records.length) {
                    return cb(null, []);
                }
                
                cb(null, records);
            }, [null]);
        },
        create: function (connectionName, tableName, data, cb) {
            var self = this;
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            var collection = connection.collections[tableName];
            var _insertData = Utils.prepareValues(_.clone(data));
            connection.dialect.insert(connection,collection,_insertData).asCallback(cb);
        },
        destroy: function (connectionName, collectionName, options, cb) {
            var self = this;
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            var collection = connection.collections[collectionName];
            asynk.add(function (callback) { connection.dialect.select(connection, collection, options).asCallback(callback); }).args(asynk.callback).alias('select')
                .add(function (callback) { connection.dialect.delete(connection, collection, options).asCallback(callback); }).args(asynk.callback)
                .serie(cb, [null, asynk.data('select')]);

        },
        update: function (connectionName, collectionName, options, values, cb) {
            var self = this;
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            var collection = connection.collections[collectionName];
            
            var values = Utils.prepareValues(values);

            asynk.add(function(callback){
                connection.dialect.select(connection, collection, options).asCallback(function(err, data){
                    if (err) {
                        return callback(err);
                    }
                    var pk = connections[connectionName].getPk(collectionName);
                    var ids = _.pluck(data, pk);
                    var idsoptions = {where: {}};
                    idsoptions.where[pk] = ids;
                    callback(null,idsoptions);
                });
            }).args(asynk.callback).alias('ids')
                .add(function(idsoptions,callback){connection.dialect.update(connection, collection, idsoptions, values).asCallback(callback);}).args(asynk.data('ids'), asynk.callback)
                .add(function(idsoptions,callback){connection.dialect.select(connection, collection, idsoptions).asCallback(callback);}).args(asynk.data('ids'), asynk.callback)
                .serie(function(err,data){ cb(err,data[2]); }, [null, asynk.data('all')]);
        },
        query: function(connectionName, collectionName, query, data, cb, connection) {
            var connection = connections[connectionName];
            if (!connection) {
                return cb(util.format('Unknown connection `%s`', connectionName));
            }
            if (_.isFunction(data)) {
                cb = data;
                data = null;
            }
            data = data || [];
            data.forEach(function(param,index){
                var pos = index + 1;
                var value = param;
                if(_.isString(param)) value = connection.dialect.sqlEscapeString(param);
                query = query.replace('$'+pos,value);
            });
            if (LOG_QUERIES) {
                console.log('Executing QUERY query: ' + query);
            }
            connection.client.raw(query).asCallback(cb);
        },
        join: function (connectionName, tableName, options, cb) {
            var connection = connections[connectionName];
            var collection = connection.getCollection(tableName);
            var cursor = new Cursor(tableName,connection,options.joins);
            connection.dialect.select(connection, collection, options).then(function(results){
                return cursor.process(results);
            }).asCallback(cb);
        }
    };
    
    return adapter;

})();





