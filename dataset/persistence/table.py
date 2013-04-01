

class Table(object):

    def __init__(self, database, table):
        self.database = database
        self.table = table

    def drop(self):
        with self.database.lock:
            self.database.tables.pop(self.table.name, None)
            self.table.drop(engine)


