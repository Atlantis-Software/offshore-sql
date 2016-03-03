/**
 * Run integration tests
 *
 * Uses the `offshore-adapter-tests` module to
 * run mocha tests against the appropriate version
 * of Offshore.  Only the interfaces explicitly
 * declared in this adapter's `package.json` file
 * are tested. (e.g. `queryable`, `semantic`, etc.)
 */


/**
 * Module dependencies
 */

var util = require('util');
var mocha = require('../../node_modules/mocha');
var log = require('../../node_modules/captains-log')();
var TestRunner = require('../../node_modules/offshore-adapter-tests');
var Adapter = require('../../index.js');



// Grab targeted interfaces from this adapter's `package.json` file:
var package = {},
  interfaces = [];
try {
  package = require('../../package.json');
  interfaces = package.offshoreAdapter.interfaces;
} catch (e) {
  throw new Error(
    '\n' +
    'Could not read supported interfaces from `offshoreAdapter.interfaces`' + '\n' +
    'in this adapter\'s `package.json` file ::' + '\n' +
    util.inspect(e)
  );
}



log.info('Testing `' + package.name + '`, an Offshore adapter.');
log.info('Running `offshore-adapter-tests` against ' + interfaces.length + ' interfaces...');
log.info('( ' + interfaces.join(', ') + ' )');
console.log();



/**
 * Integration Test Runner
 *
 * Uses the `offshore-adapter-tests` module to
 * run mocha tests against the specified interfaces
 * of the currently-implemented Offshore adapter API.
 */
new TestRunner({

  // Mocha opts
  mocha: {
    bail: false,
	timeout: 40000
  },

  // Load the adapter module.
  adapter: Adapter,

  // Default connection config to use.
  config: {
    dbType: process.env.OFFSHORE_ADAPTER_TESTS_OFFSHORE_SQL_DB_TYPE || 'mysql',   
    host: process.env.OFFSHORE_ADAPTER_TESTS_OFFSHORE_SQL_HOST || '127.0.0.1',
    user: process.env.MYSQL_ENV_MYSQL_USER || process.env.OFFSHORE_ADAPTER_TESTS_OFFSHORE_SQL_USER || 'root',
    password: process.env.MYSQL_ENV_MYSQL_PASSWORD || process.env.OFFSHORE_ADAPTER_TESTS_OFFSHORE_SQL_PASSWORD || '',
    database: process.env.MYSQL_ENV_MYSQL_DATABASE || process.env.OFFSHORE_ADAPTER_TESTS_OFFSHORE_SQL_DB|| 'offshoreSql'
  },

  // The set of adapter interfaces to test against.
  // (grabbed these from this adapter's package.json file above)
  interfaces: interfaces
});
