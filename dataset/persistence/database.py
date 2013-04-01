import logging
from threading import RLock

from sqlalchemy import create_engine
from migrate.versioning.util import construct_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import MetaData, Column, Index
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy import Integer

from dataset.persistence.table import Table
from dataset.persistence.util import resultiter


log = logging.getLogger(__name__)


class Database(object):

    def __init__(self, url):
        kw = {}
        if url.startswith('postgres'):
            kw['poolclass'] = NullPool
        engine = create_engine(url, **kw)
        self.lock = RLock()
        self.url = url
        self.engine = construct_engine(engine)
        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.tables = {}

    def create_table(self, table_name):
        """ Creates a new table. Returns a :py:class:`dataset.Table` instance."""
        with self.lock:
            log.debug("Creating table: %s on %r" % (table_name, self.engine))
            table = SQLATable(table_name, self.metadata)
            col = Column('id', Integer, primary_key=True)
            table.append_column(col)
            table.create(self.engine)
            self.tables[table_name] = table
            return Table(self, table)

    def load_table(self, table_name):
        """ Loads a table. Returns a :py:class:`dataset.Table` instance."""
        with self.lock:
            log.debug("Loading table: %s on %r" % (table_name, self))
            table = SQLATable(table_name, self.metadata, autoload=True)
            self.tables[table_name] = table
            return Table(self, table)

    def get_table(self, table_name):
        """ Loads a table or creates it if it doesn't exist yet.
        Returns a :py:class:`dataset.Table` instance. Alternatively to *get_table*
        you can also get tables using the dict syntax."""
        with self.lock:
            if table_name in self.tables:
                return Table(self, self.tables[table_name])
            if self.engine.has_table(table_name):
                return self.load_table(table_name)
            else:
                return self.create_table(table_name)

    def __getitem__(self, table_name):
        return self.get_table(table_name)

    def query(self, query):
        """ Performs SQL queries on the database. You can iterate over the result.

        .. code-block:: python

            result = db.query('SELECT * FROM population WHERE population > 10000000')
            for row in result:
                print row
        """
        return resultiter(self.engine.execute(query))

    def __repr__(self):
        return '<Database(%s)>' % self.url

