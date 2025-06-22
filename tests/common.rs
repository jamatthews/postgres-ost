use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
use uuid::Uuid;

pub struct TestDb {
    pub pool: Pool<PostgresConnectionManager<R2d2NoTls>>,
    pub dbname: String,
    #[allow(dead_code)] // (commented out to avoid dead code warning)
    pub test_db_url: String,
}

pub fn setup_test_db() -> TestDb {
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
        .simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations_old")
        .unwrap();
    client
        .simple_query(
            "CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT)",
        )
        .unwrap();
    TestDb {
        pool,
        dbname,
        test_db_url,
    }
}

#[cfg(test)]
impl TestDb {
    #[allow(dead_code)]
    pub fn get_client(
        &self,
    ) -> r2d2::PooledConnection<
        r2d2_postgres::PostgresConnectionManager<r2d2_postgres::postgres::NoTls>,
    > {
        self.pool.get().unwrap()
    }
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
