// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use postgres::{Client, NoTls};
    use r2d2::Pool;
    use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
    use postgres_ost::backfill::BatchedBackfill;
    use postgres_ost::migration::Migration;
    use serial_test::serial;

    fn setup_test_db() -> Client {
        let mut client = Client::connect("postgres://post_test@localhost/post_test", NoTls).unwrap();
        // Drop just the test table and the post_migrations schema for simplicity
        client.simple_query("DROP TABLE IF EXISTS public.test_table CASCADE").unwrap();
        client.simple_query("DROP SCHEMA IF EXISTS post_migrations CASCADE").unwrap();
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        client.simple_query("CREATE TABLE test_table (id SERIAL PRIMARY KEY, foo TEXT)").unwrap();
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
            "SELECT foo FROM post_migrations.test_table",
            &[],
        ).unwrap();
        let foo: String = row.get("foo");
        assert_eq!(foo, "hello");
    }

    #[test]
    #[serial]
    fn test_batched_backfill_shadow_table() {
        let client = setup_test_db();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
        migration.create_shadow_table(&client).unwrap();
        // Insert two rows into the main table
        client.simple_query("INSERT INTO test_table (foo) VALUES ('hello')").unwrap();
        client.simple_query("INSERT INTO test_table (foo) VALUES ('world')").unwrap();
        // Run the batched backfill
        migration.backfill_shadow_table(&client).unwrap();
        // Check that both rows are copied to the shadow table
        let rows = client.query(
            "SELECT foo FROM post_migrations.test_table ORDER BY id",
            &[],
        ).unwrap();
        let foos: Vec<String> = rows.iter().map(|row| row.get("foo")).collect();
        assert_eq!(foos, vec!["hello", "world"]);
    }

    #[test]
    #[serial]
    fn test_orchestrate_add_column() {
        let mut client = setup_test_db();
        let migration = Migration::new("ALTER TABLE test_table ADD COLUMN bar TEXT");
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
}
