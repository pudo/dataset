import itertools
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from dataset.util import MutableRow, WriteRow

if TYPE_CHECKING:
    from dataset.table import Table

_Callback = Callable[[list[MutableRow]], None]


class InvalidCallbackError(ValueError):
    pass


class _Chunker:
    def __init__(
        self,
        table: "Table",
        chunksize: int,
        callback: _Callback | None,
    ) -> None:
        self.queue: list[MutableRow] = []
        self.table: Table = table
        self.chunksize: int = chunksize
        if callback and not callable(callback):
            raise InvalidCallbackError
        self.callback: _Callback | None = callback

    def flush(self) -> None:
        self.queue.clear()

    def _queue_add(self, item: WriteRow) -> None:
        self.queue.append(dict(item))
        if len(self.queue) >= self.chunksize:
            self.flush()

    def __enter__(self) -> "_Chunker":
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.flush()


class ChunkedInsert(_Chunker):
    """Batch up insert operations
    with ChunkedInsert(my_table) as inserter:
        inserter(row)

    Rows will be inserted in groups of `chunksize` (defaulting to 1000). An
    optional callback can be provided that will be called before the insert.
    This callback takes one parameter which is the queue which is about to be
    inserted into the database
    """

    def __init__(
        self,
        table: "Table",
        chunksize: int = 1000,
        callback: _Callback | None = None,
    ) -> None:
        self.fields: set[str] = set()
        super().__init__(table, chunksize, callback)

    def insert(self, item: WriteRow) -> None:
        self.fields.update(item.keys())
        super()._queue_add(item)

    def flush(self) -> None:
        for item in self.queue:
            for field in self.fields:
                item[field] = item.get(field)
        if self.callback is not None:
            self.callback(self.queue)
        self.table.insert_many(self.queue)
        super().flush()


class ChunkedUpdate(_Chunker):
    """Batch up update operations
    with ChunkedUpdate(my_table) as updater:
        updater(row)

    Rows will be updated in groups of `chunksize` (defaulting to 1000). An
    optional callback can be provided that will be called before the update.
    This callback takes one parameter which is the queue which is about to be
    updated into the database
    """

    def __init__(
        self,
        table: "Table",
        keys: Sequence[str],
        chunksize: int = 1000,
        callback: _Callback | None = None,
    ) -> None:
        self.keys: Sequence[str] = keys
        super().__init__(table, chunksize, callback)

    def update(self, item: WriteRow) -> None:
        super()._queue_add(item)

    def flush(self) -> None:
        if self.callback is not None:
            self.callback(self.queue)
        self.queue.sort(key=dict.keys)
        for _fields, items in itertools.groupby(self.queue, key=dict.keys):
            self.table.update_many(list(items), self.keys)
        super().flush()
