# offshore-sql

[offshore](https://github.com/Atlantis-Software/offshore) adapter for sql databases.

## Installation

Install from NPM.

```bash
$ npm install offshore-sql
```
Install the database driver.

for Mysql
```bash
$ npm install mysql --save
```

Oracledb is currently in beta ...

More to come later

## Offshore Configuration

Offshore is configured with a JSON object passed during initialization.
This object contains two keys : 
+ ```adapters``` : An object containing the different adapters you wish to use.
+ ```connections``` : An object containing the different connections you want to use.

```javascript
var Offshore = require('offshore');
var Adapter = require('offshore-sql');

var offshore = new Offshore();

var config = {
  adapters: {
    'offshoreAdapter': Adapter
  },
  connections: {
    mySqlConnection: {
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
```

Connections are defined by the following attributes :

Property | Value | Description
:---: | :---: | ---
`dbType` | `string` | Database type (currently, only `mysql` and `oracledb` are supported).
`host` | `string` | Database host (URL or IP).
`port` | `integer` | Database port.
`user` | `string` | Database user.
`password` | `string` | Database password.
`database` | `string` | Database name.
`adapter` | `string` | Adapter used by this connection. It must correspond to one of the `adapters` defined before.

You can then assign connection to models when extending them to Offshore :

```javascript
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
```

Last steps : load the collections and initialize offshore with our config object

```javascript
offshore.loadCollection(User);

offshore.initialize(config, function(err, ontology) {
  
  User = ontology.collections.user;
  // We can now query our model
});
```

#### License

**[MIT](./LICENSE)**
&copy; 2016
[Atlantis Software](http://www.atlantis-software.net/) & contributors

[offshore](https://github.com/Atlantis-Software/offshore/) is free and open-source under the [MIT License](https://opensource.org/licenses/MIT/).
