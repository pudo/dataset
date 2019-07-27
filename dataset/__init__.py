import os
import warnings
from dataset.database import Database
from dataset.table import Table
from dataset.util import row_type

# shut up useless SA warning:
warnings.filterwarnings(
    'ignore', 'Unicode type received non-unicode bind param value.')
warnings.filterwarnings(
    'ignore', 'Skipping unsupported ALTER for creation of implicit constraint')

__all__ = ['Database', 'Table', 'freeze', 'connect']
__version__ = '1.1.2'


def connect(url=None, schema=None, reflect_metadata=True, engine_kwargs=None,
            reflect_views=True, ensure_schema=True, row_type=row_type,
            autocreate_db=False):
    """ Opens a new connection to a database.

    *url* can be any valid `SQLAlchemy engine URL`_.  If *url* is not defined
    it will try to use *DATABASE_URL* from environment variable.  Returns an
    instance of :py:class:`Database <dataset.Database>`. Set *reflect_metadata*
    to False if you don't want the entire database schema to be pre-loaded.
    This significantly speeds up connecting to large databases with lots of
    tables. *reflect_views* can be set to False if you don't want views to be
    loaded.  Additionally, *engine_kwargs* will be directly passed to
    SQLAlchemy, e.g.  set *engine_kwargs={'pool_recycle': 3600}* will avoid `DB
    connection timeout`_. Set *row_type* to an alternate dict-like class to
    change the type of container rows are stored in. For non-SQLite DBs, if you
    want the DB to be automatically created, set *autocreate_db* to True.::

        db = dataset.connect('sqlite:///factbook.db')

    .. _SQLAlchemy Engine URL: http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
    .. _DB connection timeout: http://docs.sqlalchemy.org/en/latest/core/pooling.html#setting-pool-recycle
    """
    if url is None:
        url = os.environ.get('DATABASE_URL', 'sqlite://')

    return Database(url, schema=schema, reflect_metadata=reflect_metadata,
                    engine_kwargs=engine_kwargs, reflect_views=reflect_views,
                    ensure_schema=ensure_schema, row_type=row_type,
                    autocreate_db=autocreate_db)
