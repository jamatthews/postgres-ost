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
    fn test_create_shadow_table() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        // Check that the shadow table exists
        let row = client.query_one(
            "SELECT to_regclass('post_migrations.test_table') IS NOT NULL AS exists",
            &[],
        ).unwrap();
        let exists: bool = row.get("exists");
        assert!(exists, "Shadow table was not created");
    }

    #[test]
    fn test_create_and_migrate_shadow_table() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        migration.migrate_shadow_table(&mut client).unwrap();
        // Check that the new column exists in the shadow table
        let row = client.query_one(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'post_migrations' AND table_name = 'test_table' AND column_name = 'bar'",
            &[],
        ).unwrap();
        let column_name: String = row.get("column_name");
        assert_eq!(column_name, "bar");
    }

    #[test]
    fn test_backfill_shadow_table() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        // Insert a row into the main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('hello')").unwrap();
        // Run the backfill
        migration.backfill_shadow_table(&mut client).unwrap();
        // Check that the row is copied to the shadow table
        let row = client.query_one(
            "SELECT id, foo FROM post_migrations.test_table",
            &[],
        ).unwrap();
        let id: i64 = row.get("id");
        let foo: String = row.get("foo");
        assert_eq!(id, 1);
        assert_eq!(foo, "hello");
    }

    #[test]
    fn test_batched_backfill_shadow_table() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        // Insert two rows into the main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('hello')").unwrap();
        client.simple_query("INSERT INTO test_table (foo) VALUES ('world')").unwrap();
        // Run the batched backfill
        migration.backfill_shadow_table(&mut client).unwrap();
        // Check that both rows are copied to the shadow table
        let rows = client.query(
            "SELECT id, foo FROM post_migrations.test_table ORDER BY id",
            &[],
        ).unwrap();
        let ids: Vec<i64> = rows.iter().map(|row| row.get("id")).collect();
        let foos: Vec<String> = rows.iter().map(|row| row.get("foo")).collect();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(foos, vec!["hello", "world"]);
    }

    #[test]
    fn test_orchestrate_add_column() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_column_map(&mut client).unwrap();
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        let manager = PostgresConnectionManager::new(format!("postgres://post_test@localhost/{}", test_db.dbname).parse().unwrap(), R2d2NoTls);
        let pool2 = Pool::new(manager).unwrap();
        migration.orchestrate(&pool2, false).unwrap();
        let row = client.query_one(
            "SELECT to_regclass('post_migrations.test_table') IS NULL AS dropped",
            &[],
        ).unwrap();
        let dropped: bool = row.get("dropped");
        assert!(dropped, "Shadow table should be dropped after orchestration");
        client.simple_query("INSERT INTO test_table (foo) VALUES ('hello')").unwrap();
        let row = client.query_one(
            "SELECT foo FROM test_table WHERE foo = 'hello'",
            &[],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "hello");
    }

    #[test]
    fn test_replay_log_insert() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        // Insert a row into the main table before setting up triggers
        client.simple_query("INSERT INTO test_table (foo) VALUES ('inserted')").unwrap();
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'inserted'", &[]).unwrap();
        let id: i64 = row.get("id");
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
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        migration.replay_log(&mut client).unwrap();
        let row = client.query_one(
            "SELECT id, foo FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "inserted");
    }

    #[test]
    fn test_replay_log_insert_with_removed_column() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table DROP COLUMN foo", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        migration.migrate_shadow_table(&mut client).unwrap();
        // Insert a row into the main table before setting up triggers
        client.simple_query("INSERT INTO test_table (foo) VALUES ('should_be_ignored')").unwrap();
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'should_be_ignored'", &[]).unwrap();
        let id: i64 = row.get("id");
        migration.create_column_map(&mut client).unwrap();
        let replay = postgres_ost::replay::LogTableReplay {
            log_table_name: migration.log_table_name.clone(),
            shadow_table_name: migration.shadow_table_name.clone(),
            table_name: migration.table_name.clone(),
            column_map: migration.column_map.as_ref().unwrap().clone(),
            primary_key: migration.primary_key.clone(),
        };
        replay.setup(&mut client).unwrap();

        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        migration.replay_log(&mut client).unwrap();
        let row = client.query_one(
            "SELECT id FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let id2: i64 = row.get("id");
        assert_eq!(id2, id);
        let err = client.query("SELECT foo FROM post_migrations.test_table WHERE id = $1", &[&id]);
        assert!(err.is_err(), "Column 'foo' should not exist in shadow table");
    }

    #[test]
    fn test_replay_log_insert_with_renamed_column() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table RENAME COLUMN foo TO bar", &mut client);
        // Insert a row into the main table before setting up triggers
        client.simple_query("INSERT INTO test_table (foo) VALUES ('should_be_renamed')").unwrap();
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'should_be_renamed'", &[]).unwrap();
        let id: i64 = row.get("id");
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
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        migration.replay_log(&mut client).unwrap();
        let row = client.query_one(
            "SELECT id, bar FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let bar: String = row.get("bar");
        assert_eq!(bar, "should_be_renamed");
        let err = client.query("SELECT foo FROM post_migrations.test_table WHERE id = $1", &[&id]);
        assert!(err.is_err(), "Column 'foo' should not exist in shadow table");
    }

    #[test]
    fn test_triggers_log_insert_update_delete() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
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

        // Insert
        client.simple_query("INSERT INTO test_table (foo) VALUES ('inserted')").unwrap();
        let row = client.query_one(
            &format!("SELECT operation, id FROM {} WHERE operation = 'INSERT'", migration.log_table_name),
            &[],
        ).unwrap();
        let op: String = row.get("operation");
        assert_eq!(op, "INSERT");

        // Update
        client.simple_query("UPDATE test_table SET foo = 'updated' WHERE foo = 'inserted'").unwrap();
        let row = client.query_one(
            &format!("SELECT operation, id FROM {} WHERE operation = 'UPDATE'", migration.log_table_name),
            &[],
        ).unwrap();
        let op: String = row.get("operation");
        assert_eq!(op, "UPDATE");

        // Delete
        client.simple_query("DELETE FROM test_table WHERE foo = 'updated'").unwrap();
        let row = client.query_one(
            &format!("SELECT operation, id FROM {} WHERE operation = 'DELETE'", migration.log_table_name),
            &[],
        ).unwrap();
        let op: String = row.get("operation");
        assert_eq!(op, "DELETE");
    }

    #[test]
    fn test_concurrent_backfill_and_log_replay() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        client.execute("INSERT INTO test_table (foo) VALUES ($1)", &[&"before"]).unwrap();
        let sql = "ALTER TABLE test_table ADD COLUMN extra TEXT";
        let mut migration = Migration::new(sql, &mut client);
        migration.setup_migration(pool).unwrap();
        migration.create_triggers(&mut client).unwrap();
        use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
        use std::thread;
        use std::time::Duration;
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = migration.start_log_replay_thread(pool, stop_replay.clone());
        let backfill_handle = migration.start_backfill_thread(pool);
        thread::sleep(Duration::from_millis(100));
        client.execute("INSERT INTO test_table (foo) VALUES ($1)", &[&"during1"]).unwrap();
        client.execute("INSERT INTO test_table (foo) VALUES ($1)", &[&"during2"]).unwrap();
        client.execute("UPDATE test_table SET foo = $1 WHERE foo = $2", &[&"updated", &"before"]).unwrap();
        client.execute("DELETE FROM test_table WHERE foo = $1", &[&"during1"]).unwrap();
        backfill_handle.join().expect("Backfill thread panicked").unwrap();
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        migration.replay_log(&mut client).unwrap();
        let mut shadow_client = pool.get().unwrap();
        let rows = shadow_client.query("SELECT foo FROM post_migrations.test_table ORDER BY id", &[]).unwrap();
        let values: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        assert!(values.contains(&"updated".to_string()));
        assert!(values.contains(&"during2".to_string()));
        assert!(!values.contains(&"during1".to_string()));
    }

    #[test]
    fn test_replay_log_update() {
        let test_db = setup_test_db();
        let pool = &test_db.pool;
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT", &mut client);
        migration.create_shadow_table(&mut client).unwrap();
        // Insert a row into the main table before setting up triggers
        client.simple_query("INSERT INTO test_table (foo) VALUES ('before_update')").unwrap();
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'before_update'", &[]).unwrap();
        let id: i64 = row.get("id");
        migration.create_column_map(&mut client).unwrap();
        let replay = postgres_ost::replay::LogTableReplay {
            log_table_name: migration.log_table_name.clone(),
            shadow_table_name: migration.shadow_table_name.clone(),
            table_name: migration.table_name.clone(),
            column_map: migration.column_map.as_ref().unwrap().clone(),
            primary_key: migration.primary_key.clone(),
        };
        replay.setup(&mut client).unwrap();
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        migration.replay_log(&mut client).unwrap();
        client.simple_query("UPDATE test_table SET foo = 'after_update' WHERE id = 1").unwrap();
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('UPDATE', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.replay_log(&mut client).unwrap();
        let row = client.query_one(
            "SELECT id, foo FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "after_update");
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
}
