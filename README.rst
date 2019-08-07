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
