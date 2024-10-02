CSV SQL
=======

Take a CSV file, query it with SQL. Magic!

.. code-block:: console

    $ cargo run file.csv
    Loaded 3162 rows into t(domain, base_domain, agency, sslv2)
    > SELECT COUNT(*) FROM t
    +----------+
    | 3162     |
    +----------+

All your rows go into a table named ``t``. It's great!

You can also specify multiple files:

.. code-block:: console

    $ cargo run file1.csv file2.csv
    Loaded 12 rows into t1(some, schema)
    Loaded 74 rows into t2(some, other, schema)
    >

If you'd like to export the results of a query to a CSV file:

.. code-block:: console

    $ cargo run file.csv
    Loaded 3162 rows into t(domain, base_domain, agency, sslv2)
    > .export(results.csv) SELECT COUNT(*) from t;

If you have tab-, pipe-, or semicolon-delimited files you can specify `--tab`,
`--pipe`, or `--semicolon` respectively).

You can change the output style to be "vertical" instead of "table" with:

.. code-block:: console

    $ cargo run file.csv
    Loaded 3162 rows into t(domain, base_domain, agency, sslv2)
    > .style(table)
    > -- Or
    > .style(vertical)

UDFs
----

``csv-sql`` contains additional UDFs (User Defined Functions) to enable easier
data analysis. They are:

`regexp_extract(pattern, value, replacement)`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example: ``regexp_extract("abc-(\d+)", "abc-12345", "lol $1")`` returns ``"lol 12345"``

Binaries
--------

Binaries for macOS and Windows are automatically built in CI and can be
downloaded from Github Actions.
