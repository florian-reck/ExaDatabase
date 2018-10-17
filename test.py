#!/usr/bin/python3
import ExasolDatabaseConnector
from pprint import pprint

db = ExasolDatabaseConnector.Database('10.70.0.51..55:8563', 'sys', 'exasol', autocommit = True)
pprint(db.execute('SELECT * FROM EXA_DBA_USERS WHERE USER_NAME LIKE ? ORDER BY 1;', '%SYS%'))
db.addToBuffer('SELECT * FROM EXA_DBA_USERS WHERE USER_NAME = %s ORDER BY 1 LIMIT 3;' % (db.escapeString('SYS')))
pprint(db.getBuffer())
pprint(db.executeBuffer())
db.close()

