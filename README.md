# POST - Postgres Online Schema Tool

#### State of the art schema migrations for PostgreSQL

`postgres-ost` or POST is an online schema change solution for PostgreSQL. This allows online changes like changing column types from int to bigint, partitioning or re-partitioning a table, and any ALTER TABLE that would otherwise hold a long `ACCESS EXCLUSIVE` lock to be done with minimal disruption.

Currenly POST only supports a trigger and log table based approach inspired by tools like Facebook [osc](https://engineering.fb.com/2017/05/05/production-engineering/onlineschemachange-rebuilt-in-python/)

Future work includes a logical replication based approach like [gh-ost](https://github.com/github/gh-ost) and [spirit](https://github.com/block/spirit) as well as support for signalling the table swap so you can trigger the actual change in a e.g. Rails migration that runs in your normal deploy process just before code paired to the new table structure.

## Usage

To migrate a normal table to a partitioned table using this tool, you can run a single migration command with the appropriate SQL. For example, to convert `test_table` to a partitioned table:

```
postgres-ost migrate \
  --database-url postgresql://user:password@localhost/dbname \
  --sql "DROP TABLE test_table; \
CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT) PARTITION BY HASH (id); \
CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
CREATE TABLE test_table_p1 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1);"
```

This will:
- Backfill and replay changes to a shadow table with the new partitioned structure
- Atomically swap the new partitioned table into place
- Move the old table to the `post_migrations_old` schema for safety

You can adapt the SQL to your own table and partitioning scheme as needed. A singe migration should alter only one table but creating partitions is OK.
