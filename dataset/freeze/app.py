import logging
import argparse

from sqlalchemy.exc import ProgrammingError
from dataset.util import FreezeException
from dataset.persistence.table import Table
from dataset.persistence.database import Database
from dataset.freeze.config import Configuration, Export
from dataset.freeze.format import get_serializer


log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description='Generate static JSON and CSV extracts from a SQL database.',
    epilog='For further information, please check the documentation.')
parser.add_argument('config', metavar='CONFIG', type=str,
                    help='freeze file cofiguration')
parser.add_argument('--db', default=None,
                    help='Override the freezefile database URI')


def freeze(result, format='csv', filename='freeze.csv',
           prefix='.', meta={}, indent=2, mode='list',
           wrap=True, callback=None, **kw):
    """
    Perform a data export of a given result set. This is a very
    flexible exporter, allowing for various output formats, metadata
    assignment, and file name templating to dump each record (or a set
    of records) into individual files.

    ::

        result = db['person'].all()
        dataset.freeze(result, format='json', filename='all-persons.json')
        
    If ``result`` is a table (rather than a result set), all records in
    the table are exported (as if ``result.all()`` had been called).


    freeze supports two values for ``mode``:

        *list* (default)
            The entire result set is dumped into a single file.

        *item*
            One file is created for each row in the result set.

    You should set a ``filename`` for the exported file(s). If ``mode``
    is set to *item* the function would generate one file per row. In
    that case you can  use values as placeholders in filenames::

            dataset.freeze(res, mode='item', format='json', filename='item-{{id}}.json')

    The following output ``format`` s are supported:

        *csv*
            Comma-separated values, first line contains column names.

        *json*
            A JSON file containing a list of dictionaries for each row
            in the table. If a ``callback`` is given, JSON with padding 
            (JSONP) will be generated.

        *tabson*
            Tabson is a smart combination of the space-efficiency of the
            CSV and the parsability and structure of JSON.

    """
    kw.update({
        'format': format,
        'filename': filename,
        'prefix': prefix,
        'meta': meta,
        'indent': indent,
        'callback': callback,
        'mode': mode,
        'wrap': wrap
    })
    records = result.all() if isinstance(result, Table) else result
    return freeze_export(Export({}, kw), result=records)


def freeze_export(export, result=None):
    try:
        if result is None:
            database = Database(export.get('database'))
            query = database.query(export.get('query'))
        else:
            query = result
        serializer_cls = get_serializer(export)
        serializer = serializer_cls(export, query)
        serializer.serialize()
    except ProgrammingError, pe:
        raise FreezeException("Invalid query: %s" % pe)


def main():
    # Set up default logger.
    logging.basicConfig(level=logging.INFO)

    try:
        args = parser.parse_args()
        config = Configuration(args.config)
        for export in config.exports:
            if args.db is not None:
                export.data['database'] = args.db
            if export.skip:
                log.info("Skipping: %s", export.name)
                continue
            log.info("Running: %s", export.name)
            freeze_export(export)
    except FreezeException, fe:
        log.error(fe)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
