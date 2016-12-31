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
