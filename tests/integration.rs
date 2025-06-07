// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use r2d2::Pool;
    use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
    use postgres_ost::migration::Migration;
    use serial_test::serial;

    fn setup_test_db() -> Pool<PostgresConnectionManager<R2d2NoTls>> {
        let manager = PostgresConnectionManager::new(
            "postgres://post_test@localhost/post_test".parse().unwrap(),
            R2d2NoTls,
        );
        let pool = Pool::new(manager).unwrap();
        let mut client = pool.get().unwrap();
        // Drop just the test table, its sequence, and the post_migrations schema for simplicity
        client.simple_query("DROP TABLE IF EXISTS public.test_table CASCADE").unwrap();
        client.simple_query("DROP SEQUENCE IF EXISTS test_table_id_seq CASCADE").unwrap();
        client.simple_query("DROP SCHEMA IF EXISTS post_migrations CASCADE").unwrap();
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        client.simple_query("CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, foo TEXT)").unwrap();
        pool
    }

    #[test]
    #[serial]
    fn test_create_shadow_table() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
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
    #[serial]
    fn test_create_and_migrate_shadow_table() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
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
    #[serial]
    fn test_backfill_shadow_table() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
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
    #[serial]
    fn test_batched_backfill_shadow_table() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
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
    #[serial]
    fn test_orchestrate_add_column() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
        // Simulate the main.rs logic: create schema, then orchestrate
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        let manager = PostgresConnectionManager::new("postgres://post_test@localhost/post_test".parse().unwrap(), R2d2NoTls);
        let pool2 = Pool::new(manager).unwrap();
        migration.orchestrate(&pool2, false).unwrap();
        // Check that the shadow table does not exist (since execute=false, it should be dropped at the end)
        let row = client.query_one(
            "SELECT to_regclass('post_migrations.test_table') IS NULL AS dropped",
            &[],
        ).unwrap();
        let dropped: bool = row.get("dropped");
        assert!(dropped, "Shadow table should be dropped after orchestration");
        // Check that the original table is untouched and has the original data
        client.simple_query("INSERT INTO test_table (foo) VALUES ('hello')").unwrap();
        let row = client.query_one(
            "SELECT foo FROM test_table WHERE foo = 'hello'",
            &[],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "hello");
    }

    #[test]
    #[serial]
    fn test_replay_log_insert() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
        migration.create_shadow_table(&mut client).unwrap();
        migration.create_log_table(&mut client).unwrap();
        // Insert a row into the main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('inserted')").unwrap();
        // Log the insert operation manually
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'inserted'", &[]).unwrap();
        let id: i64 = row.get("id");
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        // Replay the log
        migration.replay_log(&mut client).unwrap();
        // Check that the row is present in the shadow table
        let row = client.query_one(
            "SELECT id, foo FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "inserted");
    }

    #[test]
    #[serial]
    fn test_replay_log_insert_with_removed_column() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        // Migration removes the 'foo' column
        let mut migration = Migration::new("ALTER TABLE test_table DROP COLUMN foo");
        migration.create_shadow_table(&mut client).unwrap();
        migration.migrate_shadow_table(&mut client).unwrap(); // Apply the migration to the shadow table
        migration.create_log_table(&mut client).unwrap();
        // Insert a row into the main table (with 'foo')
        client.simple_query("INSERT INTO test_table (foo) VALUES ('should_be_ignored')").unwrap();
        // Log the insert operation manually
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'should_be_ignored'", &[]).unwrap();
        let id: i64 = row.get("id");
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        // Replay the log (should insert into shadow table, which does not have 'foo')
        migration.replay_log(&mut client).unwrap();
        // Check that the row is present in the shadow table and 'foo' column does not exist
        let row = client.query_one(
            "SELECT id FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let id2: i64 = row.get("id");
        assert_eq!(id2, id);
        // Confirm that selecting 'foo' from shadow table fails (column does not exist)
        let err = client.query("SELECT foo FROM post_migrations.test_table WHERE id = $1", &[&id]);
        assert!(err.is_err(), "Column 'foo' should not exist in shadow table");
    }

    #[test]
    #[serial]
    fn test_replay_log_insert_with_renamed_column() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        // Migration renames 'foo' to 'bar'
        let mut migration = Migration::new("ALTER TABLE test_table RENAME COLUMN foo TO bar");
        migration.create_shadow_table(&mut client).unwrap();
        migration.migrate_shadow_table(&mut client).unwrap();
        migration.create_log_table(&mut client).unwrap();
        // Insert a row into the main table (with 'foo')
        client.simple_query("INSERT INTO test_table (foo) VALUES ('should_be_renamed')").unwrap();
        // Log the insert operation manually
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'should_be_renamed'", &[]).unwrap();
        let id: i64 = row.get("id");
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        // Replay the log (should insert into shadow table, which has 'bar' instead of 'foo')
        migration.replay_log(&mut client).unwrap();
        // Check that the row is present in the shadow table and the renamed column 'bar' has the correct value
        let row = client.query_one(
            "SELECT id, bar FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let bar: String = row.get("bar");
        assert_eq!(bar, "should_be_renamed");
        // Confirm that selecting 'foo' from shadow table fails (column does not exist)
        let err = client.query("SELECT foo FROM post_migrations.test_table WHERE id = $1", &[&id]);
        assert!(err.is_err(), "Column 'foo' should not exist in shadow table");
    }

    #[test]
    #[serial]
    fn test_triggers_log_insert_update_delete() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
        migration.create_shadow_table(&mut client).unwrap();
        migration.create_log_table(&mut client).unwrap();
        migration.create_triggers(&mut client).unwrap();

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
    #[serial]
    fn test_concurrent_backfill_and_log_replay() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        // Insert some initial data
        client.execute("INSERT INTO test_table (foo) VALUES ($1)", &[&"before"]).unwrap();

        // Prepare migration
        let sql = "ALTER TABLE test_table ADD COLUMN extra TEXT";
        let mut migration = Migration::new(sql);
        migration.setup_migration(&pool).unwrap();
        migration.create_triggers(&mut client).unwrap();

        // Start log replay thread
        use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
        use std::thread;
        use std::time::Duration;
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = migration.start_log_replay_thread(&pool, stop_replay.clone());

        // Start backfill thread
        let backfill_handle = migration.start_backfill_thread(&pool);

        // While backfill is running, insert/update/delete rows
        thread::sleep(Duration::from_millis(100)); // Let backfill start
        client.execute("INSERT INTO test_table (foo) VALUES ($1)", &[&"during1"]).unwrap();
        client.execute("INSERT INTO test_table (foo) VALUES ($1)", &[&"during2"]).unwrap();
        client.execute("UPDATE test_table SET foo = $1 WHERE foo = $2", &[&"updated", &"before"]).unwrap();
        client.execute("DELETE FROM test_table WHERE foo = $1", &[&"during1"]).unwrap();

        // Wait for backfill to finish
        backfill_handle.join().expect("Backfill thread panicked").unwrap();
        // Stop and join replay thread
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        // Ensure all log entries are replayed one last time
        migration.replay_log(&mut client).unwrap();

        // Check that shadow table has all expected rows and changes
        let mut shadow_client = pool.get().unwrap();
        let rows = shadow_client.query("SELECT foo FROM post_migrations.test_table ORDER BY id", &[]).unwrap();
        let values: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        // Should have "updated" and "during2" ("during1" was deleted)
        assert!(values.contains(&"updated".to_string()));
        assert!(values.contains(&"during2".to_string()));
        assert!(!values.contains(&"during1".to_string()));
    }

    #[test]
    #[serial]
    fn test_replay_log_update() {
        let pool = setup_test_db();
        let mut client = pool.get().unwrap();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
        migration.create_shadow_table(&mut client).unwrap();
        migration.create_log_table(&mut client).unwrap();
        // Insert a row into the main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('before_update')").unwrap();
        // Log the insert operation manually
        let row = client.query_one("SELECT id FROM test_table WHERE foo = 'before_update'", &[]).unwrap();
        let id: i64 = row.get("id");
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('INSERT', {})",
            migration.log_table_name, id
        )).unwrap();
        migration.create_column_map(&mut client).unwrap();
        // Replay the log (should insert into shadow table)
        migration.replay_log(&mut client).unwrap();
        // Now update the row in the main table
        client.simple_query("UPDATE test_table SET foo = 'after_update' WHERE id = 1").unwrap();
        // Log the update operation manually
        client.simple_query(&format!(
            "INSERT INTO {} (operation, id) VALUES ('UPDATE', {})",
            migration.log_table_name, id
        )).unwrap();
        // Replay the log (should update the shadow table)
        migration.replay_log(&mut client).unwrap();
        // Check that the row is updated in the shadow table
        let row = client.query_one(
            "SELECT id, foo FROM post_migrations.test_table WHERE id = $1",
            &[&id],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "after_update");
    }
}
