# POST - Postgres Online Schema Tool

#### State of the art schema migrations for PostgreSQL

`post` is an online schema change solution for PostgreSQL. This allows online changes like changing column types from int to bigint, partitioning or re-partitioning a table, and any ALTER TABLE that would otherwise hold a long `ACCESS EXCLUSIVE` lock to be done with minimal disruption.

`post` is inspired by [gh-ost](https://github.com/github/gh-ost) and [spirit](https://github.com/block/spirit), using logical replication instead of triggers to propogate changes from the main table onto the shadow table.
