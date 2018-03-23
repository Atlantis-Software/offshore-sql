# offshore-sql

Offshore-sql is an adapter for sql databases, created for [Offshore](https://github.com/Atlantis-Software/offshore)

## Installation

### Install from NPM.

```bash
$ npm install offshore-sql
```
### Install the database driver

#### for mysql
```bash
$ npm install mysql --save
```
#### for mariadb
```bash
$ npm install mariasql --save
```
#### for sqlite3
```bash
$ npm install sqlite3 --save
```
#### for postgres
```bash
$ npm install pg --save
```
#### for Oracle

This package require installing the oracle instant client and instant client devel downloadable at url :

http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html

for rpm linux :

```bash
$ su -
# yum localinstall oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm
# yum localinstall oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm
# vi /etc/ld.so.conf.d/oracledb.conf
```

add the following line :

```bash
/usr/lib/oracle/12.1/client64/lib
```

save and exit, then :

```bash
# exit
$ vi ~/.bash_profile
```

add the following lines :

```bash
export OCI_LIB_DIR=/usr/lib/oracle/12.1/client64/lib/
export OCI_INCLUDE_DIR=/usr/include/oracle/12.1/client64/
export OCI_VERSION=12
export ORACLE_HOME=/usr/lib/oracle/12.1/client64
export PATH=$PATH:$ORACLE_HOME/bin
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
```

save and exit, then :

```bash
$ source ~/.bash_profile
$ npm install oracledb --save
```

## Offshore-sql Configuration

Connections are defined by the following attributes :

Property | Value | Description
:---: | :---: | ---
`dbType` | `string` | Database type (`mysql`, `mariadb`, `sqlite3`, `postgres`, `oracle`).
`host` | `string` | Database server host address.
`port` | `integer` | Database server port.
`user` | `string` | Database user.
`password` | `string` | Database user password.
`database` | `string` | Database name.
`adapter` | `string` | Adapter used by this connection. It must correspond to one of the `adapters` defined before.

```javascript
var Offshore = require('offshore');
var Adapter = require('offshore-sql');

var offshore = new Offshore();

// Define configuration
var config = {
  adapters: {
    // Here we define an adapter name and assign it our adapter module
    'offshoreAdapter': Adapter
  },
  connections: {
    mySqlConnection: {
      // adapter is a reference to the adapter name defined above
      adapter: 'offshoreAdapter',
      host: '127.0.0.1',
      port: 13306,
      user: 'root',
      password: 'itsSecret',
      database: 'offshoreSql',
      dbType: 'mysql'
    }
  }
};

// Extend the collections
var User = Offshore.Collection.extend({
  tableName: 'userTable',
  identity: 'user',
  connection: 'mySqlConnection', // Our connection is assigned here
  migrate: 'alter',
  attributes: {
    id: {
      columnName: 'ID',
      type: 'integer',
      primaryKey: true,
      unique: true,
      autoIncrement: true
    },
    name: {
      columnName: "NAME",
      type: 'string'
    }
  }
});

// Load the collections
offshore.loadCollection(User);

// Offshore initialization with the config object
offshore.initialize(config, function(err, ontology) {
  
  User = ontology.collections.user;
  // We can now query our model
  // https://github.com/Atlantis-Software/offshore-docs/blob/master/queries/query-methods.md
});
```

#### License

**[MIT](./LICENSE)**
&copy; 2016
[Atlantis Software](http://www.atlantis-software.net/) & contributors

[offshore](https://github.com/Atlantis-Software/offshore/) is free and open-source under the [MIT License](https://opensource.org/licenses/MIT/).
