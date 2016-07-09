CSV QL
======

Take a CSV file, query it with SQL. Magic!

.. code-block:: console

    $ python csvql.py file.csv
    Loaded 3162 rows into t(domain, base_domain, agency, sslv2)
    > SELECT COUNT(*) FROM t
    +----------+
    | count(*) |
    +----------+
    | 3162     |
    +----------+

All your rows go into a table named ``t``. It's great!

Error handling is bad, and the repl is super janky, no readline or anything
pleasant yet.
