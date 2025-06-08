// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use r2d2::Pool;
    use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
    use postgres_ost::migration::Migration;
    use uuid::Uuid;

    pub struct TestDb {
        pub pool: Pool<PostgresConnectionManager<R2d2NoTls>>,
        dbname: String,
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            // Connect to the default 'postgres' database to drop the test DB
            let mut admin_client = postgres::Client::connect(
                "postgres://post_test@localhost/postgres",
                postgres::NoTls,
            ).unwrap();
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

    fn setup_test_db() -> TestDb {
        let dbname = format!("test_db_{}", Uuid::new_v4().simple());
        // Connect to the default 'postgres' database to create the test DB
        let mut admin_client = postgres::Client::connect(
            "postgres://post_test@localhost/postgres",
            postgres::NoTls,
        ).unwrap();
        let create_sql = format!("CREATE DATABASE {}", dbname);
        admin_client.simple_query(&create_sql).unwrap();
        // Now connect to the new test DB
        let db_url = format!("postgres://post_test@localhost/{}", dbname);
        let manager = PostgresConnectionManager::new(db_url.parse().unwrap(), R2d2NoTls);
        let pool = Pool::builder().max_size(3).build(manager).unwrap();
        let mut client = pool.get().unwrap();
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        client.simple_query("CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, foo TEXT)").unwrap();
        TestDb { pool, dbname }
    }



    #[test]
    fn test_replay_only_subcommand() {
        use std::process::{Command, Stdio};
        use std::thread;
        use std::time::Duration;
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client.simple_query("INSERT INTO test_table (foo) VALUES ('before')").unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_postgres-ost"))
            .arg("replay-only")
            .arg("--uri")
            .arg(format!("postgres://post_test@localhost/{}", test_db.dbname))
            .arg("--sql")
            .arg("ALTER TABLE test_table ADD COLUMN bar TEXT")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start replay-only subcommand");
        thread::sleep(Duration::from_secs(2));
        client.simple_query("INSERT INTO test_table (foo) VALUES ('after')").unwrap();
        thread::sleep(Duration::from_secs(2));
        child.kill().expect("Failed to kill replay-only process");
        let output = child.wait_with_output().expect("Failed to get output");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let row = client.query_one(
            "SELECT foo FROM post_migrations.test_table WHERE foo = 'after'",
            &[],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "after", "Row should be present in shadow table after replay-only");
        eprintln!("stdout: {}\nstderr: {}", stdout, stderr);
    }

    #[test]
    fn test_add_column_with_concurrent_changes() {
        // 1. Setup test DB and initial data
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client.simple_query("INSERT INTO test_table (foo) VALUES ('expect_backfilled')").unwrap();

        // 2. Start migration (create shadow table, log table, triggers)
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        migration.create_column_map(&mut client).unwrap();
        let replay = postgres_ost::replay::LogTableReplay {
            log_table_name: migration.log_table_name.clone(),
            shadow_table_name: migration.shadow_table_name.clone(),
            table_name: migration.table_name.clone(),
            column_map: migration.column_map.as_ref().unwrap().clone(),
            primary_key: migration.primary_key.clone(),
        };
        replay.setup(&mut client).unwrap();

        // 3. Perform insert, update, delete on main table
        // Insert
        client.simple_query("INSERT INTO test_table (foo) VALUES ('expect_row_inserted')").unwrap();
        // Update
        client.simple_query("UPDATE test_table SET foo = 'expect_row_updated' WHERE foo = 'expect_backfilled'").unwrap();
        // Delete
        client.simple_query("DELETE FROM test_table WHERE foo = 'expect_row_inserted'").unwrap();

        // 4. Backfill shadow table
        migration.backfill_shadow_table(&mut client).unwrap();
        // 5. Replay the log
        migration.replay_log(&mut client).unwrap();

        // 6. Assert shadow table state
        let rows = client.query(
            "SELECT foo FROM post_migrations.test_table ORDER BY id",
            &[],
        ).unwrap();
        let foos: Vec<String> = rows.iter().map(|row| row.get("foo")).collect();
        // Should contain the updated row, not the deleted one, and not the inserted-then-deleted one
        assert!(foos.contains(&"expect_row_updated".to_string()), "Updated row should be present");
        assert!(!foos.contains(&"expect_row_inserted".to_string()), "Deleted row should not be present");
        assert!(!foos.contains(&"expect_backfilled".to_string()), "Original value should have been updated");
    }

    #[test]
    fn test_drop_column_with_concurrent_changes() {
        // 1. Setup test DB and initial data
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client.simple_query("INSERT INTO test_table (foo) VALUES ('expect_backfilled')").unwrap();

        // 2. Start migration (drop column)
        let mut migration = Migration::new("ALTER TABLE test_table DROP COLUMN foo", &mut client);
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

        // 3. Perform insert, update, delete on main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('expect_row_inserted')").unwrap();
        client.simple_query("UPDATE test_table SET foo = 'expect_row_updated' WHERE foo = 'expect_backfilled'").unwrap();
        client.simple_query("DELETE FROM test_table WHERE foo = 'expect_row_inserted'").unwrap();

        // 4. Backfill shadow table
        migration.backfill_shadow_table(&mut client).unwrap();
        // 5. Replay the log
        migration.replay_log(&mut client).unwrap();

        // 6. Assert shadow table state: only id column should exist, foo is dropped
        let rows = client.query(
            "SELECT id FROM post_migrations.test_table ORDER BY id",
            &[],
        ).unwrap();
        let ids: Vec<i64> = rows.iter().map(|row| row.get("id")).collect();
        // Should contain only the updated row's id
        assert_eq!(ids.len(), 1, "Only one row should remain after update and delete");
        // Check that 'foo' column does not exist
        let err = client.query("SELECT foo FROM post_migrations.test_table", &[]);
        assert!(err.is_err(), "Column 'foo' should not exist in shadow table after drop");
    }

    #[test]
    fn test_rename_column_with_concurrent_changes() {
        // 1. Setup test DB and initial data
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client.simple_query("INSERT INTO test_table (foo) VALUES ('expect_backfilled')").unwrap();

        // 2. Start migration (rename column)
        let mut migration = Migration::new("ALTER TABLE test_table RENAME COLUMN foo TO bar", &mut client);
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

        // 3. Perform insert, update, delete on main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('expect_row_inserted')").unwrap();
        client.simple_query("UPDATE test_table SET foo = 'expect_row_updated' WHERE foo = 'expect_backfilled'").unwrap();
        client.simple_query("DELETE FROM test_table WHERE foo = 'expect_row_inserted'").unwrap();

        // 4. Backfill shadow table
        migration.backfill_shadow_table(&mut client).unwrap();
        // 5. Replay the log
        migration.replay_log(&mut client).unwrap();

        // 6. Assert shadow table state: only 'bar' column should exist, with correct value
        let rows = client.query(
            "SELECT bar FROM post_migrations.test_table ORDER BY id",
            &[],
        ).unwrap();
        let bars: Vec<String> = rows.iter().map(|row| row.get("bar")).collect();
        assert!(bars.contains(&"expect_row_updated".to_string()), "Renamed and updated row should be present");
        assert!(!bars.contains(&"expect_row_inserted".to_string()), "Deleted row should not be present");
        assert!(!bars.contains(&"expect_backfilled".to_string()), "Original value should have been updated");
        // Check that 'foo' column does not exist
        let err = client.query("SELECT foo FROM post_migrations.test_table", &[]);
        assert!(err.is_err(), "Column 'foo' should not exist in shadow table after rename");
    }
}
