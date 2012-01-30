import csv
from datetime import datetime

def _convert_cell(v):
    if isinstance(v, unicode):
        return v.encode('utf-8')
    elif isinstance(v, datetime):
        return v.isoformat()
    return v

def dump_csv(query_iter, fh):
    writer, columns = None, None
    for row in query_iter:
        if writer is None:
            writer = csv.writer(fh)
            columns = row.keys()
            writer.writerow(columns)
        writer.writerow([_convert_cell(row.get(c)) \
                for c in columns])
    fh.close()




