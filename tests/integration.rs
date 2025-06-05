// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use postgres::{Client, NoTls};
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
}
