/**
 * Run integration tests
 *
 * Uses the `waterline-adapter-tests` module to
 * run mocha tests against the appropriate version
 * of Waterline.  Only the interfaces explicitly
 * declared in this adapter's `package.json` file
 * are tested. (e.g. `queryable`, `semantic`, etc.)
 */


/**
 * Module dependencies
 */

var util = require('util');
var mocha = require('../../node_modules/mocha');
var log = require('../../node_modules/captains-log')();
var TestRunner = require('../../node_modules/waterline-adapter-tests');
var Adapter = require('../../index.js');



// Grab targeted interfaces from this adapter's `package.json` file:
var package = {},
  interfaces = [];
try {
  package = require('../../package.json');
  interfaces = package.waterlineAdapter.interfaces;
} catch (e) {
  throw new Error(
    '\n' +
    'Could not read supported interfaces from `waterlineAdapter.interfaces`' + '\n' +
    'in this adapter\'s `package.json` file ::' + '\n' +
    util.inspect(e)
  );
}



log.info('Testing `' + package.name + '`, a Sails/Waterline adapter.');
log.info('Running `waterline-adapter-tests` against ' + interfaces.length + ' interfaces...');
log.info('( ' + interfaces.join(', ') + ' )');
console.log();
log('Latest draft of Waterline adapter interface spec:');
log('http://links.sailsjs.org/docs/plugins/adapters/interfaces');
console.log();



/**
 * Integration Test Runner
 *
 * Uses the `waterline-adapter-tests` module to
 * run mocha tests against the specified interfaces
 * of the currently-implemented Waterline adapter API.
 */
new TestRunner({

  // Mocha opts
  mocha: {
    bail: false,
	timeout: 6000
  },

  // Load the adapter module.
  adapter: Adapter,

  // Default connection config to use.
  config: {
    dbType: process.env.WATERLINE_ADAPTER_TESTS_SAILS_SQL_DB_TYPE || 'mysql',   
    host: process.env.WATERLINE_ADAPTER_TESTS_SAILS_SQL_HOST || '127.0.0.1',
    user: process.env.WATERLINE_ADAPTER_TESTS_SAILS_SQL_USER || 'root',
    password: process.env.WATERLINE_ADAPTER_TESTS_SAILS_SQL_PASSWORD || '',
    database: process.env.WATERLINE_ADAPTER_TESTS_SAILS_SQL_DB||'sailssql'
  },

  // The set of adapter interfaces to test against.
  // (grabbed these from this adapter's package.json file above)
  interfaces: interfaces

  // Most databases implement 'semantic' and 'queryable'.
  //
  // As of Sails/Waterline v0.10, the 'associations' interface
  // is also available.  If you don't implement 'associations',
  // it will be polyfilled for you by Waterline core.  The core
  // implementation will always be used for cross-adapter / cross-connection
  // joins.
  //
  // In future versions of Sails/Waterline, 'queryable' may be also
  // be polyfilled by core.
  //
  // These polyfilled implementations can usually be further optimized at the
  // adapter level, since most databases provide optimizations for internal
  // operations.
  //
  // Full interface reference:
  // https://github.com/balderdashy/sails-docs/blob/master/adapter-specification.md
});
