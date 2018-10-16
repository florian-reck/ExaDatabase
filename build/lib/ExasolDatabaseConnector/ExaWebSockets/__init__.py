#!/usr/bin/python
import re
import socket
import ssl
from random import shuffle
from contextlib import closing
from EXASOL     import connect, cursor

class Database:
    """The Database class easifies the access to your DB instance

    Using the Database class is quite easy:

        #create a new instance
        db = Database(username, password, autocommit = False)

        #do a single query (prepared statement)
        result = db.execute('SELECT USER_NAME FROM SYS.EXA_DBA_USERS WHERE USER_NAME = ?;', username)
        for row in result:
            pprint(row)

        #execute multiple SQL statements:
        db.addToBuffer('CREATE SCHEMA "test123";')
        db.addToBuffer('DROM SCHEMA "test_old";')
        db.executeBuffer()
    
        #close the connection
        db.close()

        Args:
            connectionString (str): Exasol database connection string
            user (str):         username used to login into DB instance
            password (str):     password used to login into DB instance
            autocommit (bool, optional): enables or disables autocommit
    """

    __conn = None
    __connectionTuple = None
    __buffer = None
    __ip4RangePattern = re.compile(r'^(\d+\.\d+\.\d+)\.(\d+)\.\.(\d+)')

    def __init__(self, connectionString, user, password, autocommit = False):
        self.__connectionTuple = self.ipFromConnectionString(connectionString)
        if self.__connectionTuple:
            self.__conn = connect(
                'wss://%s:%s' % self.__connectionTuple,
                user,
                password,
                autocommit,
                sslopt={"cert_reqs": ssl.CERT_NONE}
            )
            self.__buffer = []


    def ipFromConnectionString(self, connectionString):
        """chooses an usable IP address from the connection string
            
            Note:
                This method chooses an IP address from the connection string and checks if 
                the port on the given address is open. If the port is closed the next address
                will be checked.

            Args:
                connectionString:   an Exasol database connection string

            Returns:
                Tuple (ip, port) with valid address or None if no ip is usable

        """
        connectionSplit = connectionString.split(':')
        ipString = connectionSplit[0]
        port = int(connectionSplit[1])

        ipItems = []
        for ipRange in ipString.split(','):
            if not '..' in ipRange: #not a range, just an single IP
                ipItems.append(ipRange)
            else:
                match = self.__ip4RangePattern.match(ipRange)
                for i in range(int(match.group(2)), int(match.group(3)) + 1):
                    ipItems.append('%s.%i' % (match.group(1), i))
        shuffle(ipItems)
        for ip in ipItems:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                if s.connect_ex((ip, port)) == 0:
                    return (ip, port)
        return None


    def execute(self, sqlText, *args):
        """Executes a single SQL statement

            Note:
                This method is to execute a single SQL statement and retrieving the result. If you try
                to execute more than one statement use .addToBuffer() and .executeBuffer() instead.

            Args:
                sqlText:    The SQL statement which should be executed on the DB instance
                *args:      All variables which are necessary to execute a prepared statement; 

            Returns:
                None:       If no result is present
                List:       A list of all result rows
        """
        result = []
        with self.__conn.cursor() as c:
            if c.execute(sqlText, *args) > 0:
                for row in c:
                    result.append(row)
            return result
        return None


    def escapeIdent(self, text):
        """Escapes SQL identfiers
            
            Args:
                text:       SQL identifier that needs to be escaped

            Returns:
                The provided identifier, escaped
        """
        return '"' + text.replace('"', '""') + '"'

    def escapeString(self, text):
        """Escapes SQL strings
            
            Args:
                text:       SQL string that needs to be escaped

            Returns:
                The provided string, escaped
        """
        return "'" + text.replace("'", "''") + "'"
        


    def close(self):
        """Closes the connection to the database instance
            
            Args:
                None

            Returns:
                None
        """
        self.__conn.close()


    def getBuffer(self):
        """Creates a string containing the current SQL buffer
            
            Args:
                None

            Returns:
                A string containing all SQL commands on the SQL buffer
        """
        return '\n'.join(self.__buffer)


    def cleanBuffer(self):
        """Purges the SQL buffer content
            
            Args:
                None

            Returns:
                None
        """
        self.__buffer = [] 


    def addToBuffer(self, sqlText):
        """Appends a SQL statement to the internal SQL buffer

            Args:
                sqlText:    The statement which will be appended

            Returns:
                None
        """
        self.__buffer.append(sqlText.strip())


    def executeBuffer(self):
        """Executes the content of the internal SQL buffer on the DB instance

            Args:
                None

            Return:
                A list with all results for each line
        """
        results = []
        for sql in self.__buffer:
            results.append(self.execute(sql))
        self.cleanBuffer()
        return results

