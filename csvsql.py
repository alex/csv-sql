import csv
import sqlite3
import sys
from contextlib import closing

from terminaltables import AsciiTable


class Row(object):
    def __init__(self, original, normalized):
        self.original = original
        self.normalized = normalized


def _normalize_cols(fieldnames):
    return [
        Row(f, f.lower().replace(" ", "_"))
        for f in fieldnames
    ]


def _create_table(db, cols):
    with closing(db.cursor()) as c:
        create_columns = ["{} varchar".format(col.normalized) for col in cols]
        c.execute("""
        CREATE TABLE t (
            {create_columns}
        )
        """.format(create_columns=", ".join(create_columns)))


def _insert_row(db, row, cols):
    with closing(db.cursor()) as c:
        c.execute(
            """INSERT INTO t VALUES ({})""".format(
                ",".join(["?"] * len(cols))
            ),
            [row[col.original] for col in cols],
        )


def main(argv):
    [_, path] = argv
    db = sqlite3.connect(":memory:")
    num_rows = 0
    with open(path) as f:
        d = csv.DictReader(f)
        normalized_cols = _normalize_cols(d.fieldnames)
        _create_table(db, normalized_cols)
        for row in d:
            # TODO: more intelligent bulk insertions
            _insert_row(db, row, normalized_cols)
            num_rows += 1

    print("Loaded {} rows into t({})".format(
        num_rows,
        ", ".join(c.normalized for c in normalized_cols)
    ))

    # TODO: input that's not garbage
    while True:
        sys.stdout.write("> ")
        query = sys.stdin.readline()
        with closing(db.cursor()) as c:
            c.execute(query)
            header = [name for name, _, _, _, _, _, _ in c.description]
            table = AsciiTable([header] + [list(r) for r in c.fetchall()])
            print(table.table)

if __name__ == "__main__":
    main(sys.argv)
