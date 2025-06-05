// Integration tests for postgres-ost

#[cfg(test)]
mod integration {
    use postgres::{Client, NoTls};
    use postgres_ost::migration::Migration;

    fn setup_test_db() -> Client {
        // Connect to the test db as the test user
        let mut client = Client::connect("postgres://post_test@localhost/post_test", NoTls).unwrap();
        // Drop all tables in the public and post_migrations schemas
        let drop_tables = r#"
            DO $$ DECLARE
                r RECORD;
            BEGIN
                FOR r IN (SELECT tablename, schemaname FROM pg_tables WHERE schemaname IN ('public', 'post_migrations')) LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        "#;
        client.simple_query(drop_tables).unwrap();
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations").unwrap();
        client.simple_query("CREATE TABLE test_table (id SERIAL PRIMARY KEY, foo TEXT)").unwrap();
        client
    }

    #[test]
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
}
