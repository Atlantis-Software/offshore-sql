var Knex = require('knex');
var asynk = require('asynk');
var Cursor = require('./lib/cursor');
var Utils = require('./lib/utils');
var _ = require('lodash');
var Errors = require('offshore-errors').adapter;
var util = require('util');
var LOG_QUERIES = false;
var LOG_ERRORS = false;

var warn = function(warning) {
  if (LOG_ERRORS) {
    console.warning(warning);
  }
};

var oracleDialect = require('./lib/dialects/oracle');
var mysqlDialect = require('./lib/dialects/mysql');
var sqlite3Dialect = require('./lib/dialects/sqlite3');
var postgresDialect = require('./lib/dialects/postgres');

module.exports = (function() {
  var connections = {};
  var transactions = {};
  var trxIdCount = 1;

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
    registerConnection: function(connection, collections, cb) {
      var dialect;
      var client;
      switch (connection.dbType) {
        case 'mariadb':
          connection.db = connection.database;
          client = Knex({client: 'mariadb', connection: connection, debug: LOG_QUERIES, log: { warn }});
          dialect = new mysqlDialect();
          break;
        case 'mysql':
          client = Knex({client: 'mysql', connection: connection, debug: LOG_QUERIES, log: { warn }});
          dialect = new mysqlDialect();
          break;
        case 'oracle':
          client = Knex({client: 'oracledb', connection: connection, debug: LOG_QUERIES, log: { warn }});
          dialect = new oracleDialect();
          break;
        case 'sqlite3':
          client = Knex({client: 'sqlite3', connection: connection, debug: LOG_QUERIES, log: { warn }, useNullAsDefault: true});
          dialect = new sqlite3Dialect();
          break;
        case 'postgres':
          client = Knex({client: 'postgres', connection: connection, debug: LOG_QUERIES, log: { warn }});
          dialect = new postgresDialect();
          break;
      }
      if (!connection.identity)
        return cb(new Error("Errors.ConnectionIdentityMissing"));
      if (connections[connection.identity])
        return cb(new Error("Errors.ConnectionIdentityDuplicate"));
      // Store the connection
      connections[connection.identity] = {
        dialect: dialect,
        config: connection,
        collections: _.clone(collections),
        client: client,
        getCollection: function(tableName) {
          return this.collections[tableName];
        },
        getPk: function(tableName) {
          var definition = this.collections[tableName].definition;
          var pk;
          _.keys(definition).forEach(function(attrName) {
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
    registerTransaction: function(connection, collections, cb) {
      var cnx = connections[connection];
      var trxId = 'offshore-sql-trx-' + trxIdCount++;
      transactions[trxId] = {};
      transactions[trxId].connection = cnx;
      transactions[trxId].resolver = cnx.client.transaction(function(trx) {
        transactions[trxId].transaction = trx;
        return cb(null, trxId);
      });
    },
    commit: function(trxId, collections, cb) {
      if (!transactions[trxId]) {
        return cb(new Error('No transaction with this id'));
      }
      transactions[trxId].transaction.commit();
      transactions[trxId].resolver.asCallback(function(err) {
        if (err) {
          return cb(err);
        }
        delete transactions[trxId];
        cb();
      });
    },
    rollback: function(trxId, collections, cb) {
      if (!transactions[trxId]) {
        return cb(new Error('No transaction with this id'));
      }
      var default_err = new Error('Rollback');
      transactions[trxId].transaction.rollback(default_err);
      transactions[trxId].resolver.asCallback(function(err) {
        if (err && err !== default_err) {
          return cb(err);
        }
        delete transactions[trxId];
        cb();
      });
    },
    define: function(connectionName, tableName, definition, cb) {
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
      connection.dialect.createTable(connection, collection, definition).asCallback(function(err, data) {
        if (err) {
          return cb(err);
        }
        self.describe(connectionName, tableName, function(err) {
          cb(err, null);
        });
      });
    },
    describe: function(connectionName, tableName, cb) {
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

      connection.dialect.describe(connection, collection, function(err, schema) {
        if (err && err.code === 'ER_NO_SUCH_TABLE') {
          if (LOG_QUERIES)
            console.log('Table', tableName, 'doesn\'t exist, creating it ...');
          return cb();
        }
        if (err) {
          if (LOG_ERRORS)
            console.log('#Error :', err);
          return cb(err);
        }
        var normalizedSchema = connection.dialect.normalizeSchema(schema, collection.attributes);
        collection.schema = normalizedSchema;
        cb(null, normalizedSchema);
      }, LOG_QUERIES);
    },
    find: function(connectionName, tableName, options, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (options.groupBy) {
        if (!options.sum && !options.average && !options.min && !options.max) {
          return cb(Errors.InvalidGroupBy);
        }
      }

      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      var collection = connection.collections[tableName];
      /* replace attributes names by columnNames */
      var select = connection.dialect.select(connection, collection, options);
      var cursor = new Cursor(select);
      if (transaction) {
        select.query.transacting(transaction);
      }
      select.query.asCallback(function(err, results) {
        if (err) {
          return cb(err);
        }
        cb(null, cursor.process(results));
      });
    },
    count: function(connectionName, tableName, options, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      var collection = connection.collections[tableName];
      var query = connection.dialect.count(connection, collection, options);
      if (transaction) {
        query.transacting(transaction);
      }
      if (options.groupBy) {
        var groupBy = options.groupBy;
        if (!_.isArray(groupBy)) {
          groupBy = [groupBy];
        }
        query.asCallback(function(err, cnt) {
          if (err) {
            return cb(err);
          }
          var reduce_count = function(data, lvl) {
            data = _.groupBy(data, groupBy[lvl]);
            _.keys(data).forEach(function(key) {
              if (lvl === groupBy.length - 1) {
                data[key] = data[key][0]['cnt'];
              } else {
                data[key] = reduce_count(data[key], lvl+1);
              }
            });
            return data;
          };
          cb(null, reduce_count(cnt, 0));
        });
      } else {
        query.asCallback(function(err, record) {
          if (err) {
            return cb(err);
          }
          cb(null, Utils.cast({type: 'integer'}, record[0]['cnt'], options));
        });
      }
    },
    drop: function(connectionName, tableName, relations, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      if (typeof relations === 'function') {
        cb = relations;
        relations = [];
      }

      function dropTable(tableName, callback) {
        var collection = connection.collections[tableName];
        var query = connection.dialect.dropTable(connection, collection);
        if (transaction) {
          query.transacting(transaction);
        }
        query.asCallback(callback);
      }

      asynk.each(relations, dropTable).serie().done(function() {
        dropTable(tableName, cb);
      }).fail(cb);
    },
    createEach: function(connectionName, tableName, valuesList, cb) {
      var self = this;
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      var collection = connection.collections[tableName];
      var records = [];
      asynk.each(valuesList, function(data, cb) {
        var query = connection.dialect.insert(connection, collection, _.cloneDeep(data));
        if (transaction) {
          query.transacting(transaction);
        }
        query.asCallback(function(err, record) {
          if (err) {
            return cb(err);
          }

          records.push(Utils.castAll(collection.definition, records, options));
          cb(null, record);
        });
      }).parallel().done(function() {
        if (!records.length) {
          return cb(null, []);
        }
        cb(null, records);
      }).fail(cb);
    },
    create: function(connectionName, tableName, data, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      var collection = connection.collections[tableName];
      var query = connection.dialect.insert(connection, collection, _.cloneDeep(data));
      if (transaction) {
        query.transacting(transaction);
      }
      query.asCallback(function(err, result) {
        if (err) {
          return cb(err);
        }
        var pkval = {};
        var pk = connection.getPk(tableName);
        if (collection.definition[pk].autoIncrement && result) {
          pkval[pk] = Utils.cast(collection.definition[pk].type, result[0], pk);
        }
        cb(null, _.extend({}, data, pkval));
      });
    },
    destroy: function(connectionName, collectionName, options, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      var collection = connection.collections[collectionName];
      asynk.add(function(callback) {
        var select = connection.dialect.select(connection, collection, options);
        if (transaction) {
          select.query.transacting(transaction);
        }
        select.query.asCallback(callback);
      }).alias('select')
        .add(function(select, callback) {
          var pk = connection.getPk(collectionName);
          var ids = _.map(select, pk);
          var idsoptions = {where: {}};
          idsoptions.where[pk] = ids;
          var query = connection.dialect.delete(connection, collection, idsoptions);
          if (transaction) {
            query.transacting(transaction);
          }
          query.asCallback(callback);
        }).args(asynk.data('select'), asynk.callback)
        .serie([asynk.data('select')]).done(function(select) {
        cb(null, Utils.castAll(collection.definition, select, options));
      }).fail(cb);

    },
    update: function(connectionName, collectionName, options, values, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      var collection = connection.collections[collectionName];

      asynk.add(function(callback) {
        var select = connection.dialect.select(connection, collection, options);
        if (transaction) {
            select.query.transacting(transaction);
          }
        select.query.asCallback(function(err, data) {
          if (err) {
            return callback(err);
          }
          var pk = connection.getPk(collectionName);
          var ids = _.map(data, pk);
          var idsoptions = {where: {}};
          idsoptions.where[pk] = ids;
          callback(null, idsoptions);
        });
      }).alias('ids')
        .add(function(idsoptions, callback) {
          var query = connection.dialect.update(connection, collection, idsoptions, values);
          if (transaction) {
            query.transacting(transaction);
          }
          query.asCallback(callback);
        }).args(asynk.data('ids'), asynk.callback)
        .add(function(idsoptions, callback) {
          var secondSelect = connection.dialect.select(connection, collection, idsoptions);
          if (transaction) {
            secondSelect.query.transacting(transaction);
          }
          secondSelect.query.asCallback(callback);
        }).args(asynk.data('ids'), asynk.callback)
        .serie().done(function(data) {
        cb(null, Utils.castAll(collection.definition, data[2], options));
      }).fail(cb);
    },
    query: function(connectionName, collectionName, query, data, cb, connection) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      if (!connection) {
        return cb(util.format('Unknown connection `%s`', connectionName));
      }
      if (_.isFunction(data)) {
        cb = data;
        data = null;
      }
      data = data || [];
      data.forEach(function(param, index) {
        var pos = index + 1;
        var value = param;
        if (_.isString(param))
          value = connection.dialect.sqlEscapeString(param);
        query = query.replace('$' + pos, value);
      });
      if (LOG_QUERIES) {
        console.log('Executing QUERY query: ' + query);
      }
      connection.client.raw(query).asCallback(cb);
    },
    join: function(connectionName, tableName, options, cb) {
      var connection;
      var transaction;
      if (transactions[connectionName]) {
        connection = transactions[connectionName].connection;
        transaction = transactions[connectionName].transaction;
      } else {
        connection = connections[connectionName];
      }
      var collection = connection.getCollection(tableName);
      var select = connection.dialect.select(connection, collection, options);
      var cursor = new Cursor(select);
      if (transaction) {
        select.query.transacting(transaction);
      }
      select.query.asCallback(function(err, results) {
        if (err) {
          return cb(err);
        }
        cb(null, cursor.process(results));
      });
    },
    teardown: function(connectionName, cb) {
      if(!connections[connectionName]) {
        return cb('Connection ' + connectionName + ' not found');
      }
      var cnx = connections[connectionName];
      cnx.client.destroy(function(err) {
        if(err) {
          return cb(err);
        }
        delete connections[connectionName];
        return cb();
      });
    }
  };

  return adapter;

})();
