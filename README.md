Database Utilities
====================================================================
____________________________________________________________________

##Configuration Options

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
      instanceName: String
    };
    
##Session Store Setup

#### PostreSQL Setup:

    CREATE TABLE "session" (
      "sid" varchar NOT NULL COLLATE "default",
    	"sess" json NOT NULL,
    	"expire" timestamp(6) NOT NULL
    )
    WITH (OIDS=FALSE);
    ALTER TABLE "session" ADD CONSTRAINT "session_pkey" PRIMARY KEY ("sid") NOT DEFERRABLE INITIALLY IMMEDIATE;
    
#### MS SQL Server Setup:

    CREATE TABLE [dbo].[sessions](
        [sid] [varchar](255) NOT NULL PRIMARY KEY,
        [session] [varchar](max) NOT NULL,
        [expires] [datetime] NOT NULL
    )
    
#### MySQL Setup:

    No setup required.