use anyhow::Result;
use itertools::Itertools;
use postgres::Client;
use postgres::types::Type;
use crate::{BatchedBackfill, LogTableReplay, Replay, ColumnMap, Parse, SqlParser};
use crate::backfill::Backfill;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Clone)]
pub struct PrimaryKeyInfo {
    pub name: String,
    pub ty: Type,
}

pub struct Migration {
    pub sql: String,
    pub ast: Vec<sqlparser::ast::Statement>,
    pub table_name: String,
    pub shadow_table_name: String,
    pub log_table_name: String,
    pub old_table_name: String,
    pub column_map: Option<ColumnMap>,
    pub primary_key: PrimaryKeyInfo,
}

impl Migration {
    pub fn new(sql: &str, client: &mut Client) -> Self {
        let parser = SqlParser;
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let tables = parser.extract_tables(sql);
        let unique_tables = tables.iter().unique().collect::<Vec<_>>();
        assert!(
            unique_tables.len() == 1,
            "Only one table can be altered per migration. Found: {:?}",
            unique_tables
        );
        let table_name = unique_tables[0];
        let shadow_table_name = format!("post_migrations.{}", table_name);
        let log_table_name = format!("post_migrations.{}_log", table_name);
        let old_table_name = format!("post_migrations.{}_old", table_name);
        let primary_key = Self::get_primary_key_info(client, table_name).expect("Failed to detect primary key");
        Migration {
            sql: sql.to_string(),
            ast: ast,
            table_name: table_name.to_string(),
            shadow_table_name: shadow_table_name.clone(),
            log_table_name: log_table_name.clone(),
            old_table_name: old_table_name.clone(),
            column_map: None,
            primary_key,
        }
    }

    pub fn drop_shadow_table_if_exists(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let drop_shadow_table_statement =
            format!("DROP TABLE IF EXISTS {}", self.shadow_table_name);
        client.simple_query(&drop_shadow_table_statement)?;
        Ok(())
    }

    pub fn create_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let create_table_statement = format!(
            "CREATE TABLE {} (LIKE {} INCLUDING ALL)",
            self.shadow_table_name, self.table_name
        );
        client.simple_query(&create_table_statement)?;
        Ok(())
    }

    pub fn migrate_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let parser = SqlParser;
        let altered_statement =
            parser.migrate_shadow_table_statement(&self.sql, &self.table_name, &self.shadow_table_name);
        client.batch_execute(&altered_statement)?;
        Ok(())
    }

    pub fn create_log_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let create_log_statement = format!(
            "CREATE TABLE IF NOT EXISTS {} (post_migration_log_id BIGSERIAL PRIMARY KEY, operation TEXT, timestamp TIMESTAMPTZ DEFAULT NOW(), LIKE {})",
            self.log_table_name, self.table_name
        );
        client.simple_query(&create_log_statement)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn backfill_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        BatchedBackfill { batch_size: 1000 }.backfill(&self.table_name, &self.shadow_table_name, client)
    }

    pub fn replay_log(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let column_map = self.column_map.as_ref().expect("column_map must be set before replay");
        let replay = LogTableReplay {
            log_table_name: self.log_table_name.clone(),
            shadow_table_name: self.shadow_table_name.clone(),
            table_name: self.table_name.clone(),
            column_map: column_map.clone(),
            primary_key: self.primary_key.clone(),
        };
        replay.replay_log(client)?;
        Ok(())
    }

    pub fn drop_old_table_if_exists(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let drop_old_table_statement = format!("DROP TABLE IF EXISTS {}", self.old_table_name);
        client.simple_query(&drop_old_table_statement)?;
        Ok(())
    }

    pub fn swap_tables(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let swap_statement = format!(
            "BEGIN; ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}; COMMIT;",
            self.table_name, self.old_table_name, self.shadow_table_name, self.table_name
        );
        client.simple_query(&swap_statement)?;
        Ok(())
    }

    pub fn create_column_map(&mut self, client: &mut Client) -> Result<(), anyhow::Error> {
        let main_cols = get_table_columns(client, &self.table_name);
        let shadow_cols = get_table_columns(client, &self.shadow_table_name);
        let map = ColumnMap::new(&main_cols, &shadow_cols);
        self.column_map = Some(map);
        Ok(())
    }

    pub fn get_primary_key_info(client: &mut Client, table: &str) -> anyhow::Result<PrimaryKeyInfo> {
        let (schema, table) = if let Some((schema, table)) = table.split_once('.') {
            (schema, table)
        } else {
            ("public", table)
        };
        let full_table = format!("{}.{}", schema, table);
        let row = client.query_one(
            "SELECT a.attname, a.atttypid::regtype::text
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             WHERE i.indrelid = ($1)::text::regclass AND i.indisprimary
             LIMIT 1",
            &[&full_table],
        )?;
        let name: String = row.get(0);
        let type_name: String = row.get(1);
        let ty = match type_name.as_str() {
            "integer" => Type::INT4,
            "bigint" => Type::INT8,
            _ => panic!("Unsupported PK type: {}", type_name),
        };
        Ok(PrimaryKeyInfo { name, ty })
    }

    pub fn create_triggers(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let pk_col = &self.primary_key.name;
        // Insert trigger
        let insert_trigger = format!(
            r#"
            CREATE OR REPLACE FUNCTION {log_table}_insert_trigger_fn() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {log_table} (operation, {pk_col}) VALUES ('INSERT', NEW.{pk_col});
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS {table}_insert_trigger ON {table};
            CREATE TRIGGER {table}_insert_trigger
                AFTER INSERT ON {table}
                FOR EACH ROW EXECUTE FUNCTION {log_table}_insert_trigger_fn();
            "#,
            log_table = self.log_table_name,
            table = self.table_name,
            pk_col = pk_col
        );
        client.batch_execute(&insert_trigger)?;

        // Delete trigger
        let delete_trigger = format!(
            r#"
            CREATE OR REPLACE FUNCTION {log_table}_delete_trigger_fn() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {log_table} (operation, {pk_col}) VALUES ('DELETE', OLD.{pk_col});
                RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS {table}_delete_trigger ON {table};
            CREATE TRIGGER {table}_delete_trigger
                AFTER DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION {log_table}_delete_trigger_fn();
            "#,
            log_table = self.log_table_name,
            table = self.table_name,
            pk_col = pk_col
        );
        client.batch_execute(&delete_trigger)?;

        // Update trigger
        let update_trigger = format!(
            r#"
            CREATE OR REPLACE FUNCTION {log_table}_update_trigger_fn() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {log_table} (operation, {pk_col}) VALUES ('UPDATE', NEW.{pk_col});
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS {table}_update_trigger ON {table};
            CREATE TRIGGER {table}_update_trigger
                AFTER UPDATE ON {table}
                FOR EACH ROW EXECUTE FUNCTION {log_table}_update_trigger_fn();
            "#,
            log_table = self.log_table_name,
            table = self.table_name,
            pk_col = pk_col
        );
        client.batch_execute(&update_trigger)?;

        Ok(())
    }

    fn create_post_migrations_schema(&self, client: &mut Client) -> anyhow::Result<()> {
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations;")?;
        Ok(())
    }

    pub fn setup_migration(&mut self, pool: &Pool<PostgresConnectionManager<R2d2NoTls>>) -> anyhow::Result<()> {
        let mut client = pool.get()?;
        self.create_post_migrations_schema(&mut client)?;
        self.drop_shadow_table_if_exists(&mut client)?;
        self.create_shadow_table(&mut client)?;
        self.migrate_shadow_table(&mut client)?;
        self.create_column_map(&mut client)?;
        self.create_log_table(&mut client)?;
        self.create_triggers(&mut client)?;
        Ok(())
    }

    pub fn start_log_replay_thread(
        &self,
        pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
        stop_replay: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;
        let mut replay_client = pool.get().expect("Failed to get replay client");
        let replay = LogTableReplay {
            log_table_name: self.log_table_name.clone(),
            shadow_table_name: self.shadow_table_name.clone(),
            table_name: self.table_name.clone(),
            column_map: self.column_map.as_ref().expect("column_map must be set before replay").clone(),
            primary_key: self.primary_key.clone(),
        };
        let stop_replay_clone = stop_replay.clone();
        thread::spawn(move || {
            while !stop_replay_clone.load(Ordering::Relaxed) {
                if let Err(e) = replay.replay_log(&mut replay_client) {
                    eprintln!("replay_log error: {e}");
                }
                thread::sleep(Duration::from_millis(200));
            }
        })
    }

    pub fn start_backfill_thread(
        &self,
        pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let table_name = self.table_name.clone();
        let shadow_table_name = self.shadow_table_name.clone();
        let mut backfill_client = pool.get().expect("Failed to get backfill client");
        let backfill = BatchedBackfill { batch_size: 1000 };
        std::thread::spawn(move || {
            backfill.backfill(&table_name, &shadow_table_name, &mut backfill_client)
        })
    }

    pub fn orchestrate(&mut self, pool: &Pool<PostgresConnectionManager<R2d2NoTls>>, execute: bool) -> anyhow::Result<()> {
        use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
        let client = pool.get()?;
        // primary_key is already set in struct
        drop(client);
        self.setup_migration(pool)?;
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = self.start_log_replay_thread(pool, stop_replay.clone());
        let backfill_handle = self.start_backfill_thread(pool);
        backfill_handle.join().expect("Backfill thread panicked")?;
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        // Ensure all log entries are replayed one last time
        let mut client = pool.get()?;
        self.replay_log(&mut client)?;
        if execute {
            // TODO: need to lock table against writes, finish replay, then swap tables
            self.swap_tables(&mut client)?;
            self.drop_old_table_if_exists(&mut client)?;
        } else {
            self.drop_shadow_table_if_exists(&mut client)?;
        }
        Ok(())
    }
}

// Remove the moved tests from migration.rs

// Helper to get the list of columns for a table (excluding dropped columns)
fn get_table_columns(client: &mut Client, table: &str) -> Vec<String> {
    let (schema, table) = if let Some((schema, table)) = table.split_once('.') {
        (schema, table)
    } else {
        ("public", table)
    };
    let rows = client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
        &[&schema, &table],
    ).unwrap();
    rows.iter().map(|row| row.get::<_, String>("column_name")).collect()
}
