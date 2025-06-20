// Integration tests for postgres-ost

mod common;

#[cfg(test)]
mod integration {
    use super::common::setup_test_db;
    use postgres_ost::Backfill;
    use postgres_ost::Replay;

    #[test]
    fn test_replay_only_subcommand() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use std::time::Duration;
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client
            .simple_query("INSERT INTO test_table (assertable) VALUES ('before')")
            .unwrap();
        let stop_replay = Arc::new(AtomicBool::new(false));
        let runner = postgres_ost::migration_runner::MigrationRunner::from_pool(pool.clone());
        let handle = runner.run_replay_only(
            "ALTER TABLE test_table ADD COLUMN bar TEXT",
            false, // log-based replay
            stop_replay.clone(),
        );
        std::thread::sleep(Duration::from_secs(2));
        client
            .simple_query("INSERT INTO test_table (assertable) VALUES ('after')")
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));
        stop_replay.store(true, Ordering::Relaxed);
        handle.join().expect("Replay thread panicked").unwrap();
        let row = client
            .query_one(
                "SELECT assertable FROM post_migrations.test_table WHERE assertable = 'after'",
                &[],
            )
            .unwrap();
        let assertable: String = row.get("assertable");
        assert_eq!(
            assertable, "after",
            "Row should be present in shadow table after replay-only"
        );
    }

    // Helper to run the concurrent DML/backfill/replay test for any ALTER TABLE statement, always using 'assertable' as the expected column
    fn run_concurrent_change_test(alter_table_sql: &str) {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let runner = postgres_ost::migration_runner::MigrationRunner::from_pool(pool.clone());
        let mut client = pool.get().unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_backfilled', 'target_val')").unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_deleted', 'target_val')").unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_to_update', 'target_val')").unwrap();

        // Use MigrationRunner for schema migration
        let (migration, column_map) = runner.run_schema_migration(alter_table_sql).unwrap();
        runner.run_replay_setup(&migration, &column_map).unwrap();
        // Use MigrationRunner for backfill
        runner.run_backfill(&migration).unwrap();

        // DML
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_inserted', 'target_val')").unwrap();
        client.simple_query("UPDATE test_table SET assertable = 'expect_row_updated' WHERE assertable = 'expect_row_to_update'").unwrap();
        client
            .simple_query("DELETE FROM test_table WHERE assertable = 'expect_row_deleted'")
            .unwrap();
        // Use MigrationRunner for replay
        runner.run_replay(&migration, &column_map).unwrap();

        // Generic assertion logic (always on 'assertable')
        let rows = client.query(
            "SELECT assertable FROM post_migrations.test_table ORDER BY id",
            &[],
        );
        match rows {
            Ok(rows) => {
                let vals: Vec<String> = rows.iter().map(|row| row.get("assertable")).collect();
                assert!(
                    vals.contains(&"expect_backfilled".to_string()),
                    "Backfilled row should be present"
                );
                assert!(
                    vals.contains(&"expect_row_inserted".to_string()),
                    "Inserted row should have been replayed"
                );
                assert!(
                    vals.contains(&"expect_row_updated".to_string()),
                    "Updated row should have been replayed"
                );
                assert!(
                    !vals.contains(&"expect_row_to_update".to_string()),
                    "Row to update should not be present after update"
                );
                assert!(
                    !vals.contains(&"expect_row_deleted".to_string()),
                    "Deleted row should not be present after replay"
                );
            }
            Err(e) => {
                panic!("Unexpected error querying assertable: {}", e);
            }
        }
    }

    #[test]
    fn test_add_column_with_concurrent_changes() {
        run_concurrent_change_test("ALTER TABLE test_table ADD COLUMN bar TEXT");
    }

    #[test]
    fn test_drop_column_with_concurrent_changes() {
        run_concurrent_change_test("ALTER TABLE test_table DROP COLUMN target");
    }

    #[test]
    fn test_rename_column_with_concurrent_changes() {
        run_concurrent_change_test("ALTER TABLE test_table RENAME COLUMN target TO something_else");
    }

    #[test]
    fn test_migration_with_simple_add_column() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let runner = postgres_ost::migration_runner::MigrationRunner::from_pool(pool.clone());
        let migration_sql = "ALTER TABLE test_table ADD COLUMN foo TEXT;";
        let (migration, column_map) = runner.run_schema_migration(migration_sql).unwrap();
        runner.run_replay_setup(&migration, &column_map).unwrap();
        assert_eq!(migration.table.to_string(), "test_table");
        assert_eq!(
            migration.shadow_table.to_string(),
            "post_migrations.test_table"
        );
        // Now check if the shadow table has the new column
        let mut client = pool.get().unwrap();
        let row = client.query_one(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'post_migrations' AND table_name = 'test_table' AND column_name = 'foo'",
            &[],
        ).unwrap();
        let col: String = row.get("column_name");
        assert_eq!(col, "foo");
    }

    #[test]
    fn test_migration_with_partitioned_table_sql() {
        // Use the test DB helper to ensure permissions and schema
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let runner = postgres_ost::migration_runner::MigrationRunner::from_pool(pool.clone());
        let _client = pool.get().unwrap();
        // Ensure the table exists for PK detection
        let migration_sql = "DROP TABLE test_table; \
            CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT) PARTITION BY HASH (id); \
            CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
            CREATE TABLE test_table_p1 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1);";
        let (migration, column_map) = runner.run_schema_migration(migration_sql).unwrap();
        runner.run_replay_setup(&migration, &column_map).unwrap();
        assert_eq!(migration.table.to_string(), "test_table");
        assert_eq!(
            migration.shadow_table.to_string(),
            "post_migrations.test_table"
        );
        assert!(
            migration
                .shadow_table_migrate_sql
                .contains("CREATE TABLE post_migrations.test_table")
        );
    }

    #[test]
    fn test_drop_and_recreate_partitioned_table_with_concurrent_changes() {
        let migration_sql = "DROP TABLE test_table; \
            CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT) PARTITION BY HASH (id); \
            CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
            CREATE TABLE test_table_p1 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1);";
        run_concurrent_change_test(migration_sql);
    }

    #[test]
    fn test_full_migration_execute_swaps_tables() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let runner = postgres_ost::migration_runner::MigrationRunner::from_pool(pool.clone());
        let mut client = pool.get().unwrap();
        // Insert a row into the original table
        client
            .simple_query(
                "INSERT INTO test_table (assertable, target) VALUES ('before_swap', 't1')",
            )
            .unwrap();
        // Prepare migration SQL
        let migration_sql = "ALTER TABLE test_table ADD COLUMN swapped INTEGER DEFAULT 42;";
        // Use run_migrate to perform the migration and swap
        runner.run_migrate(migration_sql, true, false).unwrap();
        // After swap, the new table should be in public, and have the new column
        let row = client
            .query_one(
                "SELECT swapped FROM test_table WHERE assertable = 'before_swap'",
                &[],
            )
            .unwrap();
        let swapped: i32 = row.get("swapped");
        assert_eq!(swapped, 42);
        // The old table should exist in post_migrations_old
        let row = client.query_one(
            "SELECT assertable FROM post_migrations_old.test_table WHERE assertable = 'before_swap'",
            &[],
        ).unwrap();
        let assertable: String = row.get("assertable");
        assert_eq!(assertable, "before_swap");
    }

    #[test]
    fn test_logical_replay_with_concurrent_changes() {
        use postgres_ost::logical_replay::LogicalReplay;
        use postgres_ost::logical_replication::{Publication, Slot};
        use uuid::Uuid;
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();

        // --- Setup migration and shadow table ---
        let migration = postgres_ost::migration::Migration::new(
            "ALTER TABLE test_table ADD COLUMN bar TEXT",
            &mut client,
        );
        migration.create_shadow_table(&mut client).unwrap();
        migration.migrate_shadow_table(&mut client).unwrap();
        let column_map =
            postgres_ost::ColumnMap::new(&migration.table, &migration.shadow_table, &mut *client);

        // --- Initial data and backfill ---
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_backfilled', 'target_val')").unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_deleted', 'target_val')").unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_to_update', 'target_val')").unwrap();
        // Replace removed method with equivalent logic using BatchedBackfill
        let backfill = postgres_ost::backfill::BatchedBackfill { batch_size: 1000 };
        backfill
            .backfill(
                &migration.table,
                &migration.shadow_table,
                &column_map,
                &mut client,
            )
            .unwrap();

        // --- Logical replication setup ---
        let slot_name = format!("logical_replay_slot_{}", Uuid::new_v4().simple());
        let pub_name = format!("logical_replay_pub_{}", Uuid::new_v4().simple());
        let table = postgres_ost::Table::new("test_table");
        let slot = Slot::new(slot_name.clone());
        let publication = Publication::new(pub_name.clone(), table.clone(), slot.clone());
        let logical_replay = LogicalReplay {
            slot: slot.clone(),
            publication: publication.clone(),
            table: migration.table.clone(),
            shadow_table: migration.shadow_table.clone(),
            column_map: column_map.clone(),
            primary_key: migration.primary_key.clone(),
        };
        logical_replay.setup(&mut client).unwrap();

        // --- DML changes ---
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_inserted', 'target_val')").unwrap();
        client.simple_query("UPDATE test_table SET assertable = 'expect_row_updated' WHERE assertable = 'expect_row_to_update'").unwrap();
        client
            .simple_query("DELETE FROM test_table WHERE assertable = 'expect_row_deleted'")
            .unwrap();

        // --- Replay changes ---
        logical_replay.replay_log(&mut client).unwrap();

        // --- Assertions ---
        let rows = client.query(
            "SELECT assertable FROM post_migrations.test_table ORDER BY id",
            &[],
        );
        match rows {
            Ok(rows) => {
                let vals: Vec<String> = rows.iter().map(|row| row.get("assertable")).collect();
                assert!(
                    vals.contains(&"expect_backfilled".to_string()),
                    "Backfilled row should be present"
                );
                assert!(
                    vals.contains(&"expect_row_inserted".to_string()),
                    "Inserted row should have been replayed"
                );
                assert!(
                    vals.contains(&"expect_row_updated".to_string()),
                    "Updated row should have been replayed"
                );
                assert!(
                    !vals.contains(&"expect_row_to_update".to_string()),
                    "Row to update should not be present after update"
                );
                assert!(
                    !vals.contains(&"expect_row_deleted".to_string()),
                    "Deleted row should not be present after replay"
                );
            }
            Err(e) => {
                panic!("Unexpected error querying assertable: {}", e);
            }
        }

        // --- Teardown logical replication ---
        let mut transaction = client.transaction().unwrap();
        logical_replay.teardown(&mut transaction).unwrap();
        transaction.commit().unwrap();
    }
}
