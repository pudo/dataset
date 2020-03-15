

class InvalidCallback(ValueError):
    pass


class ChunkedInsert(object):
    """Batch up insert operations
    with ChunkedStorer(my_table) as storer:
        table.insert(row)

    Rows will be inserted in groups of `chunksize` (defaulting to 1000). An
    optional callback can be provided that will be called before the insert.
    This callback takes one parameter which is the queue which is about to be
    inserted into the database
    """

    def __init__(self, table, chunksize=1000, callback=None):
        self.queue = []
        self.fields = set()
        self.table = table
        self.chunksize = chunksize
        if callback and not callable(callback):
            raise InvalidCallback
        self.callback = callback

    def flush(self):
        for item in self.queue:
            for field in self.fields:
                item[field] = item.get(field)
        if self.callback is not None:
            self.callback(self.queue)
        self.table.insert_many(self.queue)
        self.queue = []

    def insert(self, item):
        self.fields.update(item.keys())
        self.queue.append(item)
        if len(self.queue) >= self.chunksize:
            self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
