# -*- coding: utf-8 -*-
import ExasolDatabaseConnector.ExaWebSockets

class Database(ExasolDatabaseConnector.ExaWebSockets.Database):
    def __init__(self, connectionString, user, password, autocommit = False):
        super().__init__(self, connectionString = conenctionString, user = user, password = password, autocommit = autocommit)

