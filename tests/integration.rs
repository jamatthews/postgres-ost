// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use postgres::{Client, NoTls};
    use r2d2::Pool;
    use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
    use postgres_ost::migration::Migration;
    use serial_test::serial;

    fn setup_test_db() -> Client {
        let mut client = Client::connect("postgres://post_test@localhost/post_test", NoTls).unwrap();
        // Drop just the test table and the post_migrations schema for simplicity
        client.simple_query("DROP TABLE IF EXISTS public.test_table CASCADE").unwrap();
        client.simple_query("DROP SCHEMA IF EXISTS post_migrations CASCADE").unwrap();
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        client.simple_query("CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, foo TEXT)").unwrap();
        client
    }

    #[test]
    #[serial]
    fn test_create_shadow_table() {
        let mut client = setup_test_db();
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
        let mut client = setup_test_db();
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
        let mut client = setup_test_db();
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
        let mut client = setup_test_db();
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
        let mut client = setup_test_db();
        let mut migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
        // Simulate the main.rs logic: create schema, then orchestrate
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        let manager = PostgresConnectionManager::new("postgres://post_test@localhost/post_test".parse().unwrap(), R2d2NoTls);
        let pool = Pool::new(manager).unwrap();
        migration.orchestrate(&pool, false).unwrap();
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
        let mut client = setup_test_db();
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
        let mut client = setup_test_db();
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
        let mut client = setup_test_db();
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
}
