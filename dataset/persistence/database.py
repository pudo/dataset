import logging
from threading import RLock

from sqlalchemy import create_engine
from migrate.versioning.util import construct_engine
from sqlalchemy.pool import NullPool

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
        self.indexes = {}

    @classmethod
    def connect(self, url):
        return Database(url)


    def __repr__(self):
        return '<Database(%s)>' % self.url



