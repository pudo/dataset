import os
import warnings
from dataset.database import Database
from dataset.table import Table
from dataset.util import row_type

# shut up useless SA warning:
warnings.filterwarnings("ignore", "Unicode type received non-unicode bind param value.")
warnings.filterwarnings(
    "ignore", "Skipping unsupported ALTER for creation of implicit constraint"
)

__all__ = ["Database", "Table", "connect"]
__version__ = "1.3.2"


def connect(
    url=None,
    schema=None,
    reflect_metadata=True,
    engine_kwargs=None,
    reflect_views=True,
    ensure_schema=True,
    row_type=row_type,
):
    """ Opens a new connection to a database.

    *url* can be any valid `SQLAlchemy engine URL`_.  If *url* is not defined
    it will try to use *DATABASE_URL* from environment variable.  Returns an
    instance of :py:class:`Database <dataset.Database>`. Set *reflect_metadata*
    to False if you don't want the entire database schema to be pre-loaded.
    This significantly speeds up connecting to large databases with lots of
    tables. *reflect_views* can be set to False if you don't want views to be
    loaded. Additionally, *engine_kwargs* will be directly passed to
    SQLAlchemy, e.g.  set *engine_kwargs={'pool_recycle': 3600}* will avoid `DB
    connection timeout`_. Set *row_type* to an alternate dict-like class to
    change the type of container rows are stored in.::

        db = dataset.connect('sqlite:///factbook.db')

    One of the main features of `dataset` is to automatically create tables and
    columns as data is inserted. This behaviour can optionally be disabled via
    the `ensure_schema` argument. It can also be overridden in a lot of the
    data manipulation methods using the `ensure` flag.

    .. _SQLAlchemy Engine URL: http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
    .. _DB connection timeout: http://docs.sqlalchemy.org/en/latest/core/pooling.html#setting-pool-recycle
    """
    if url is None:
        url = os.environ.get("DATABASE_URL", "sqlite://")

    return Database(
        url,
        schema=schema,
        reflect_metadata=reflect_metadata,
        engine_kwargs=engine_kwargs,
        reflect_views=reflect_views,
        ensure_schema=ensure_schema,
        row_type=row_type,
    )


def get_or_create(url: str, schema: dict) -> Database:
    """
    A helper function to succinctly open a database, and create explicitly-typed columns if needed.

    Sample usage:
        db = dataset.get_or_create('sqlite:///my_website.sqlite3', schema={
            'users': {
                'primary': ('username', 'string'),
                'columns': [('age', 'integer'), ('upvotes', 'integer'), ('tagline', 'text'), ('bio', 'text'), ('url', 'text')],
                'index': ['url', ['username', 'upvotes'], ['username', 'age']]
            }
        })

    Instead of:
        db = dataset.connect('sqlite:///my_website.sqlite3')
        if 'users' not in db:
            table = db.create_table(users, primary_id='username', primary_type=db.types.string)
            table.create_column('age', db.types.integer)
            table.create_column('upvotes', db.types.integer)
            table.create_column('tagline', db.types.text)
            table.create_column('bio', db.types.text)
            table.create_column('url', db.types.text)
            table.create_index([url])
            table.create_index(['username', 'upvotes'])
            table.create_index(['username', 'age'])
    """
    db = connect(url)

    def get_type(type_):
        return getattr(db.types, type_)

    for table_name in schema.keys():
        if table_name not in db:
            # Create table
            ct = schema[table_name]
            primary_id = None
            primary_type = None
            if 'primary' in ct:
                primary_id, primary_type = ct['primary']
                primary_type = get_type(primary_type)
            assert 'columns' in ct
            current_table = db.create_table(table_name, primary_id=primary_id, primary_type=primary_type)
            for col in ct['columns']:
                col_name, col_type = col
                current_table.create_column(col_name, get_type(col_type))
            if 'index' in ct:
                for cc in ct['index']:
                    if not isinstance(cc, list):
                        current_table.create_index([cc])
                    else:
                        current_table.create_index(cc)
    return db
