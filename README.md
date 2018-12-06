# ExaDatabase
```
Help on package ExasolDatabaseConnector:

NAME
    ExasolDatabaseConnector - # -*- coding: utf-8 -*-

PACKAGE CONTENTS
    ExaDatabaseAbstract (package)
    ExaOdbcDriver (package)
    ExaWebSockets (package)

CLASSES
    ExasolDatabaseConnector.ExaWebSockets.Database(ExasolDatabaseConnector.ExaDatabaseAbstract.DatabaseAbstract)
        Database
    
    class Database(ExasolDatabaseConnector.ExaWebSockets.Database)
     |  Database(connectionString, user, password, autocommit=False)
     |  
     |  The Database class easifies the access to your DB instance
     |  
     |  Using the Database class is quite easy:
     |  
     |      #create a new instance
     |      db = Database(username, password, autocommit = False)
     |  
     |      #do a single query (prepared statement)
     |      result = db.execute('SELECT USER_NAME FROM SYS.EXA_DBA_USERS WHERE USER_NAME = ?;', username)
     |      for row in result:
     |          pprint(row)
     |  
     |      #execute multiple SQL statements:
     |      db.addToBuffer('CREATE SCHEMA "test123";')
     |      db.addToBuffer('DROM SCHEMA "test_old";')
     |      db.executeBuffer()
     |  
     |      #close the connection
     |      db.close()
     |  
     |      Args:
     |          connectionString (str): Exasol database connection string
     |          user (str):         username used to login into DB instance
     |          password (str):     password used to login into DB instance
     |          autocommit (bool, optional): enables or disables autocommit
     |  
     |  Method resolution order:
     |      Database
     |      ExasolDatabaseConnector.ExaWebSockets.Database
     |      ExasolDatabaseConnector.ExaDatabaseAbstract.DatabaseAbstract
     |      builtins.object
     |  
     |  Methods defined here:
     |  
     |  __init__(self, connectionString, user, password, autocommit=False)
     |      Initialize self.  See help(type(self)) for accurate signature.
     |  
     |  ----------------------------------------------------------------------
     |  Methods inherited from ExasolDatabaseConnector.ExaWebSockets.Database:
     |  
     |  close(self)
     |      Closes the connection to the database instance
     |      
     |      Args:
     |          None
     |      
     |      Returns:
     |          None
     |  
     |  execute(self, sqlText, *args)
     |      Executes a single SQL statement
     |      
     |      Note:
     |          This method is to execute a single SQL statement and retrieving the result. If you try
     |          to execute more than one statement use .addToBuffer() and .executeBuffer() instead.
     |      
     |      Args:
     |          sqlText:    The SQL statement which should be executed on the DB instance
     |          *args:      All variables which are necessary to execute a prepared statement; 
     |      
     |      Returns:
     |          None:       If no result is present
     |          List:       A list of all result rows
     |  
     |  executeBuffer(self)
     |      Executes the content of the internal SQL buffer on the DB instance
     |      
     |      Args:
     |          None
     |      
     |      Return:
     |          A list with all results for each line
     |  
     |  ----------------------------------------------------------------------
     |  Methods inherited from ExasolDatabaseConnector.ExaDatabaseAbstract.DatabaseAbstract:
     |  
     |  addToBuffer(self, sqlText)
     |      Appends a SQL statement to the internal SQL buffer
     |      
     |      Args:
     |          sqlText:    The statement which will be appended
     |      
     |      Returns:
     |          None
     |  
     |  cleanBuffer(self)
     |      Purges the SQL buffer content
     |      
     |      Args:
     |          None
     |      
     |      Returns:
     |          None
     |  
     |  escapeIdent(self, text)
     |      Escapes SQL identfiers
     |      
     |      Args:
     |          text:       SQL identifier that needs to be escaped
     |      
     |      Returns:
     |          The provided identifier, escaped
     |  
     |  escapeString(self, text)
     |      Escapes SQL strings
     |      
     |      Args:
     |          text:       SQL string that needs to be escaped
     |      
     |      Returns:
     |          The provided string, escaped
     |  
     |  getBuffer(self)
     |      Creates a string containing the current SQL buffer
     |      
     |      Args:
     |          None
     |      
     |      Returns:
     |          A string containing all SQL commands on the SQL buffer
     |  
     |  getMethod(self)
     |  
     |  ipFromConnectionString(self, connectionString)
     |      chooses an usable IP address from the connection string
     |      
     |      Note:
     |          This method chooses an IP address from the connection string and checks if 
     |          the port on the given address is open. If the port is closed the next address
     |          will be checked.
     |      
     |      Args:
     |          connectionString:   an Exasol database connection string
     |      
     |      Returns:
     |          Tuple (ip, port) with valid address or None if no ip is usable
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors inherited from ExasolDatabaseConnector.ExaDatabaseAbstract.DatabaseAbstract:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)

DATA
    driverName = None

FILE
    /home/fr/dev/ExaDatabase/ExasolDatabaseConnector/__init__.py

```
