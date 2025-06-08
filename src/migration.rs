use crate::backfill::Backfill;
use crate::{BatchedBackfill, ColumnMap, LogTableReplay, Parse, Replay};
use anyhow::Result;
use postgres::Client;
use postgres::types::Type;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
use std::fmt;
use std::str::FromStr;

#[derive(Clone)]
pub struct PrimaryKeyInfo {
    pub name: String,
    pub ty: Type,
}

pub struct Migration {
    pub sql: String,
    pub shadow_table_migrate_sql: String,
    pub table: Table,
    pub shadow_table: Table,
    pub log_table: Table,
    pub old_table: Table,
    pub primary_key: PrimaryKeyInfo,
}

impl Migration {
    pub fn new(sql: &str, client: &mut Client) -> Self {
        let parser = crate::pg_query_parser::PgQueryParser;
        let table_name = parser
            .extract_main_table(sql)
            .expect("Failed to extract main table");
        let table = Table::new(&table_name);
        let shadow_table = Table::new(&format!("post_migrations.{}", table_name));
        let log_table = Table::new(&format!("post_migrations.{}_log", table_name));
        let old_table = Table::new(&format!("post_migrations.{}_old", table_name));
        let primary_key = table
            .get_primary_key_info(client)
            .expect("Failed to detect primary key");
        let shadow_table_migrate_sql =
            parser.migrate_shadow_table_statement(sql, &table_name, &shadow_table.to_string());
        Migration {
            sql: sql.to_string(),
            shadow_table_migrate_sql,
            table,
            shadow_table,
            log_table,
            old_table,
            primary_key,
        }
    }

    pub fn drop_shadow_table_if_exists(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let drop_shadow_table_statement = format!("DROP TABLE IF EXISTS {}", self.shadow_table);
        client.simple_query(&drop_shadow_table_statement)?;
        Ok(())
    }

    pub fn create_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let create_table_statement = format!(
            "CREATE TABLE {} (LIKE {} INCLUDING ALL)",
            self.shadow_table, self.table
        );
        client.simple_query(&create_table_statement)?;
        Ok(())
    }

    pub fn migrate_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        client.batch_execute(&self.shadow_table_migrate_sql)?;
        Ok(())
    }

    pub fn create_log_table(&self, _client: &mut Client) -> Result<(), anyhow::Error> {
        // Deprecated: use LogTableReplay::setup instead
        Ok(())
    }

    pub fn create_column_map(&self, client: &mut Client) -> ColumnMap {
        let main_cols = self.table.get_columns(client);
        let shadow_cols = self.shadow_table.get_columns(client);
        ColumnMap::new(&main_cols, &shadow_cols)
    }

    #[allow(dead_code)]
    pub fn backfill_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let column_map = self.create_column_map(client);
        BatchedBackfill { batch_size: 1000 }.backfill(
            &self.table.to_string(),
            &self.shadow_table.to_string(),
            &column_map,
            client,
        )
    }

    pub fn replay_log(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let column_map = self.create_column_map(client);
        let replay = LogTableReplay {
            log_table_name: self.log_table.to_string(),
            shadow_table_name: self.shadow_table.to_string(),
            table_name: self.table.to_string(),
            column_map,
            primary_key: self.primary_key.clone(),
        };
        replay.replay_log(client)?;
        Ok(())
    }

    pub fn drop_old_table_if_exists(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let drop_old_table_statement = format!("DROP TABLE IF EXISTS {}", self.old_table);
        client.simple_query(&drop_old_table_statement)?;
        Ok(())
    }

    pub fn swap_tables(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let swap_statement = format!(
            "BEGIN; ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}; COMMIT;",
            self.table, self.old_table, self.shadow_table, self.table
        );
        client.simple_query(&swap_statement)?;
        Ok(())
    }

    pub fn setup_migration(
        &self,
        pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
    ) -> anyhow::Result<()> {
        let mut client = pool.get()?;
        self.create_post_migrations_schema(&mut client)?;
        self.drop_shadow_table_if_exists(&mut client)?;
        self.create_shadow_table(&mut client)?;
        self.migrate_shadow_table(&mut client)?;
        let column_map = self.create_column_map(&mut client);
        // Use LogTableReplay for log table and trigger setup
        let replay = LogTableReplay {
            log_table_name: self.log_table.to_string(),
            shadow_table_name: self.shadow_table.to_string(),
            table_name: self.table.to_string(),
            column_map,
            primary_key: self.primary_key.clone(),
        };
        replay.setup(&mut client)?;
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
        let column_map = self.create_column_map(&mut replay_client);
        let replay = LogTableReplay {
            log_table_name: self.log_table.to_string(),
            shadow_table_name: self.shadow_table.to_string(),
            table_name: self.table.to_string(),
            column_map,
            primary_key: self.primary_key.clone(),
        };
        let stop_replay_clone = stop_replay.clone();
        thread::spawn(move || {
            while !stop_replay_clone.load(Ordering::Relaxed) {
                let _ = replay.replay_log(&mut replay_client).is_err();
                thread::sleep(Duration::from_millis(200));
            }
        })
    }

    pub fn start_backfill_thread(
        &self,
        pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let table = self.table.clone();
        let shadow_table = self.shadow_table.clone();
        let mut backfill_client = pool.get().expect("Failed to get backfill client");
        let column_map = self.create_column_map(&mut backfill_client);
        let backfill = BatchedBackfill { batch_size: 1000 };
        std::thread::spawn(move || {
            backfill.backfill(
                &table.to_string(),
                &shadow_table.to_string(),
                &column_map,
                &mut backfill_client,
            )
        })
    }

    pub fn orchestrate(
        &self,
        pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
        execute: bool,
    ) -> anyhow::Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let client = pool.get()?;
        drop(client);
        self.setup_migration(pool)?;
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = self.start_log_replay_thread(pool, stop_replay.clone());
        let backfill_handle = self.start_backfill_thread(pool);
        backfill_handle.join().expect("Backfill thread panicked")?;
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        let mut client = pool.get()?;
        self.replay_log(&mut client)?;
        if execute {
            self.swap_tables(&mut client)?;
            self.drop_old_table_if_exists(&mut client)?;
        } else {
            self.drop_shadow_table_if_exists(&mut client)?;
        }
        Ok(())
    }

    fn create_post_migrations_schema(&self, client: &mut Client) -> anyhow::Result<()> {
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations;")?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Table {
    pub schema: Option<String>,
    pub name: String,
}

impl FromStr for Table {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((schema, name)) = s.split_once('.') {
            Ok(Table {
                schema: Some(schema.to_string()),
                name: name.to_string(),
            })
        } else {
            Ok(Table {
                schema: None,
                name: s.to_string(),
            })
        }
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{}.{}", schema, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

impl Table {
    pub fn new(full_name: &str) -> Self {
        full_name.parse().unwrap()
    }

    pub fn get_primary_key_info(&self, client: &mut Client) -> anyhow::Result<PrimaryKeyInfo> {
        let full_table = self.to_string();
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

    pub fn get_columns(&self, client: &mut Client) -> Vec<String> {
        let rows = client.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            &[&self.schema.as_deref().unwrap_or("public"), &self.name],
        ).unwrap();
        rows.iter()
            .map(|row| row.get::<_, String>("column_name"))
            .collect()
    }
}

// Remove the moved tests from migration.rs

// Helper to get the list of columns for a table (excluding dropped columns)
// (Moved to Table::get_columns)
