Database Utilities
====================================================================
____________________________________________________________________

## Configuration Options

    // get the settings options from the config module.
    var options = {
      adapterName: String,
      server: String,
      port: String,
      dbName: String,
      username: String,
      password: String,
      connectionPoolLimit: Number,
      sessionTableName: String,
      sessionSecret: String
      
      // MS SQL specific
      instanceName: String,
      sessionDatabaseName: String,
      ssl: Object or String. See node-mysql for object specs on this.
      
      // MySQL specific
      multipleStatements: Boolean
    };
    
## Session Store Setup
    
#### MS SQL Server Setup:

    CREATE TABLE [dbo].[sessions](
        [sid] [varchar](255) NOT NULL PRIMARY KEY,
        [session] [varchar](max) NOT NULL,
        [expires] [datetime] NOT NULL
    )
    
#### MySQL Setup:

    No setup required.
