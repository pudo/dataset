from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

from dataset.util import FreezeException

class Query(object):

    def __init__(self, query, rp):
        self.query = query
        self.rp = rp

    def __len__(self):
        return self.rp.rowcount

    def __iter__(self):
        keys = self.rp.keys()
        while True:
            row = self.rp.fetchone()
            if row is None:
                return
            yield dict(zip(keys, row))


class ExportEngine(object):

    def __init__(self, config):
        self.config = config

    @property
    def engine(self):
        if not hasattr(self, '_engine'):
            self._engine = create_engine(self.config.get('database'))
        return self._engine

    def query(self):
        try:
            q = self.config.get('query')
            rp = self.engine.execute(q)
            return Query(q, rp)
        except ProgrammingError, pe:
            raise FreezeException("Invalid query: %s - %s" % (q, pe))

