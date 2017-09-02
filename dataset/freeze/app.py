import logging
import argparse

from sqlalchemy.exc import ProgrammingError, OperationalError
from dataset.util import FreezeException
from dataset.persistence.table import Table
from dataset.persistence.database import Database
from dataset.freeze.config import Configuration, Export
from dataset.freeze.format import get_serializer


log = logging.getLogger(__name__)


def create_parser():
    parser = argparse.ArgumentParser(
        prog='datafreeze',
        description='Generate static JSON and CSV extracts from a SQL database.',
        epilog='For further information, please check the documentation.')
    parser.add_argument('config', metavar='CONFIG', type=str,
                        help='freeze file cofiguration')
    parser.add_argument('--db', default=None,
                        help='Override the freezefile database URI')
    return parser


def freeze(result, format='csv', filename='freeze.csv', fileobj=None,
           prefix='.', mode='list', **kw):
    """
    Perform a data export of a given result set. This is a very
    flexible exporter, allowing for various output formats, metadata
    assignment, and file name templating to dump each record (or a set
    of records) into individual files.

    ::

        result = db['person'].all()
        dataset.freeze(result, format='json', filename='all-persons.json')

    Instead of passing in the file name, you can also pass a file object::

        result = db['person'].all()
        fh = open('/dev/null', 'wb')
        dataset.freeze(result, format='json', fileobj=fh)

    Be aware that this will disable file name templating and store all
    results to the same file.

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

            dataset.freeze(res, mode='item', format='json',
                           filename='item-{{id}}.json')

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

    You can pass additional named parameters specific to the used format.

        As an example, you can freeze to minified JSON with the following:

            dataset.freeze(res, format='json', indent=4, wrap=False,
                           filename='output.json')

        *json* and *tabson*
            *callback*:
                if provided, generate a JSONP string using the given callback
                function, i.e. something like `callback && callback({...})`

            *indent*:
                if *indent* is a non-negative integer (it is ``2`` by default
                when you call `dataset.freeze`, and ``None`` via the
                ``datafreeze`` command), then JSON array elements and object
                members will be pretty-printed with that indent level.
                An indent level of 0 will only insert newlines.
                ``None`` is the most compact representation.

            *meta*:
                if *meta* is not ``None`` (default: ``{}``), it will be included
                in the JSON output (for *json*, only if *wrap* is ``True``).

            *wrap* (only for *json*):
                if *wrap* is ``True`` (default), the JSON output is an object
                of the form ``{"count": 2, "results": [...]}``.
                if ``meta`` is not ``None``, a third property ``meta`` is added
                to the wrapping object, with this value.
    """
    kw.update({
        'format': format,
        'filename': filename,
        'fileobj': fileobj,
        'prefix': prefix,
        'mode': mode
    })

    # Special cases when freezing comes from dataset.freeze
    if format in ['json', 'tabson'] and 'indent' not in kw:
        kw['indent'] = 2

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
    except (OperationalError, ProgrammingError) as e:
        raise FreezeException("Invalid query: %s" % e)


def freeze_with_config(config, db=None):
    for export in config.exports:
        if db is not None:
            export.data['database'] = db
        if export.skip:
            log.info("Skipping: %s", export.name)
            continue
        log.info("Running: %s", export.name)
        freeze_export(export)


def main():  # pragma: no cover
    # Set up default logger.
    logging.basicConfig(level=logging.INFO)

    try:
        parser = create_parser()
        args = parser.parse_args()
        freeze_with_config(Configuration(args.config), args.db)
    except FreezeException as fe:
        log.error(fe)


if __name__ == '__main__':  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG)
    main()
