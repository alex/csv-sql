import csv
import re
import sqlite3
import sys
from contextlib import closing

from terminaltables import AsciiTable


class Row(object):
    def __init__(self, original, normalized):
        self.original = original
        self.normalized = normalized


_PARENTHENTICAL_RE = re.compile(r"\(.*?\)")


def _normalize_col(f):
    f = _PARENTHENTICAL_RE.sub("", f)
    return f.lower().strip().replace(" ", "_").replace(".", "_").replace("?", "")


def _normalize_cols(fieldnames):
    return [
        Row(f, _normalize_col(f))
        for f in fieldnames
    ]


def _create_table(db, table_name, cols):
    with closing(db.cursor()) as c:
        create_columns = ["{} varchar".format(col.normalized) for col in cols]
        c.execute("""
        CREATE TABLE {table_name} (
            {create_columns}
        )
        """.format(
            table_name=table_name,
            create_columns=", ".join(create_columns)
        ))


def _insert_row(db, table_name, row, cols):
    with closing(db.cursor()) as c:
        c.execute(
            """INSERT INTO {} VALUES ({})""".format(
                table_name,
                ",".join(["?"] * len(cols))
            ),
            [row[col.original] for col in cols],
        )


def _load_table_from_path(db, table_name, path):
    num_rows = 0
    with open(path) as f:
        d = csv.DictReader(f)
        normalized_cols = _normalize_cols(d.fieldnames)
        _create_table(db, table_name, normalized_cols)
        for row in d:
            # TODO: more intelligent bulk insertions
            _insert_row(db, table_name, row, normalized_cols)
            num_rows += 1
    print("Loaded {} rows into {}({})".format(
        num_rows,
        table_name,
        ", ".join(c.normalized for c in normalized_cols)
    ))


def main(argv):
    paths = argv[1:]
    db = sqlite3.connect(":memory:")
    if len(paths) == 1:
        _load_table_from_path(db, "t", paths[0])
    else:
        for i, path in enumerate(paths, 1):
            _load_table_from_path(db, "t{}".format(i), path)

    # TODO: input that's not garbage
    while True:
        sys.stdout.write("> ")
        query = sys.stdin.readline()
        if query == "exit\n":
            sys.exit()
        with closing(db.cursor()) as c:
            # TODO: error handling
            c.execute(query)
            header = [name for name, _, _, _, _, _, _ in c.description]
            table = AsciiTable([header] + [list(r) for r in c.fetchall()])
            print(table.table)


if __name__ == "__main__":
    main(sys.argv)
