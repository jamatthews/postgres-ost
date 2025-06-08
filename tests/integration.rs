// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use postgres_ost::migration::Migration;
    use r2d2::Pool;
    use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
    use uuid::Uuid;

    pub struct TestDb {
        pub pool: Pool<PostgresConnectionManager<R2d2NoTls>>,
        dbname: String,
    }

    fn setup_test_db() -> TestDb {
        let db_url = std::env::var("POSTGRES_OST_TEST_DB_URL")
            .unwrap_or_else(|_| "postgres://post_test:postgres@localhost/postgres".to_string());
        let dbname = format!("test_db_{}", Uuid::new_v4().simple());
        // Connect to the default database to create the test DB
        let mut admin_client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();
        let create_sql = format!("CREATE DATABASE {}", dbname);
        admin_client.simple_query(&create_sql).unwrap();
        // Now connect to the new test DB
        let test_db_url = format!("postgres://post_test:postgres@localhost/{}", dbname);
        let manager = PostgresConnectionManager::new(test_db_url.parse().unwrap(), R2d2NoTls);
        let pool = Pool::builder().max_size(3).build(manager).unwrap();
        let mut client = pool.get().unwrap();
        client
            .simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations")
            .unwrap();
        client
            .simple_query(
                "CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT)",
            )
            .unwrap();
        TestDb { pool, dbname }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let db_url = std::env::var("POSTGRES_OST_TEST_DB_URL")
                .unwrap_or_else(|_| "postgres://post_test:postgres@localhost/postgres".to_string());
            let mut admin_client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();
            // Terminate all connections to the test DB before dropping
            let terminate_sql = format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}' AND pid <> pg_backend_pid()",
                self.dbname
            );
            let _ = admin_client.simple_query(&terminate_sql);
            let drop_sql = format!("DROP DATABASE IF EXISTS {}", self.dbname);
            let _ = admin_client.simple_query(&drop_sql);
        }
    }

    #[test]
    fn test_replay_only_subcommand() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use std::thread;
        use std::time::Duration;
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client
            .simple_query("INSERT INTO test_table (assertable) VALUES ('before')")
            .unwrap();
        let stop_replay = Arc::new(AtomicBool::new(false));
        let pool2 = pool.clone();
        let stop_replay2 = stop_replay.clone();
        let replay_thread = thread::spawn(move || {
            postgres_ost::run_replay_only(
                &pool2,
                "ALTER TABLE test_table ADD COLUMN bar TEXT",
                stop_replay2,
            )
            .unwrap();
        });
        thread::sleep(Duration::from_secs(2));
        client
            .simple_query("INSERT INTO test_table (assertable) VALUES ('after')")
            .unwrap();
        thread::sleep(Duration::from_secs(2));
        stop_replay.store(true, Ordering::Relaxed);
        replay_thread.join().expect("Replay thread panicked");
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
        let mut client = pool.get().unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_backfilled', 'target_val')").unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_deleted', 'target_val')").unwrap();
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_to_update', 'target_val')").unwrap();

        let mut migration = Migration::new(alter_table_sql, &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        migration.migrate_shadow_table(&mut client).unwrap();
        migration.create_column_map(&mut client).unwrap();
        let replay = postgres_ost::replay::LogTableReplay {
            log_table_name: migration.log_table_name.clone(),
            shadow_table_name: migration.shadow_table_name.clone(),
            table_name: migration.table_name.clone(),
            column_map: migration.column_map.as_ref().unwrap().clone(),
            primary_key: migration.primary_key.clone(),
        };
        replay.setup(&mut client).unwrap();

        migration.backfill_shadow_table(&mut client).unwrap();

        // DML
        client.simple_query("INSERT INTO test_table (assertable, target) VALUES ('expect_row_inserted', 'target_val')").unwrap();
        client.simple_query("UPDATE test_table SET assertable = 'expect_row_updated' WHERE assertable = 'expect_row_to_update'").unwrap();
        client
            .simple_query("DELETE FROM test_table WHERE assertable = 'expect_row_deleted'")
            .unwrap();
        migration.replay_log(&mut client).unwrap();

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
    fn test_migration_new_with_simple_add_column() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let migration_sql = "ALTER TABLE test_table ADD COLUMN foo TEXT;";
        let mut migration = Migration::new(migration_sql, &mut client);
        assert_eq!(migration.table_name, "test_table");
        assert_eq!(migration.shadow_table_name, "post_migrations.test_table");
        // assert!(migration.shadow_table_migrate_sql.contains("ALTER TABLE post_migrations.test_table ADD COLUMN foo TEXT"));
        migration.setup_migration(pool).unwrap();
        // Now check if the shadow table has the new column
        let row = client.query_one(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'post_migrations' AND table_name = 'test_table' AND column_name = 'foo'",
            &[],
        ).unwrap();
        let col: String = row.get("column_name");
        assert_eq!(col, "foo");
    }

    #[test]
    fn test_migration_new_with_partitioned_table_sql() {
        // Use the test DB helper to ensure permissions and schema
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        // Ensure the table exists for PK detection
        client.batch_execute("DROP TABLE IF EXISTS test_table CASCADE; CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT);").unwrap();
        let migration_sql = "DROP TABLE test_table; \
            CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT) PARTITION BY HASH (id); \
            CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
            CREATE TABLE test_table_p1 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1);";
        let migration = postgres_ost::migration::Migration::new(migration_sql, &mut client);
        assert_eq!(migration.table_name, "test_table");
        assert_eq!(migration.shadow_table_name, "post_migrations.test_table");
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
}
