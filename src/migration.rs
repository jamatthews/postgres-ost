use crate::backfill::Backfill;
use crate::table::Table;
use crate::{BatchedBackfill, LogTableReplay, Parse, Replay};
use anyhow::Result;
use postgres::Client;
use postgres::types::Type;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};

use crate::ColumnMap;

#[derive(Clone)]
pub struct PrimaryKeyInfo {
    pub name: String,
    pub ty: Type,
}

#[derive(Clone)]
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

    #[allow(dead_code)]
    pub fn backfill_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let column_map = ColumnMap::new(&self.table, &self.shadow_table, client);
        BatchedBackfill { batch_size: 1000 }.backfill(
            &self.table,
            &self.shadow_table,
            &column_map,
            client,
        )
    }

    pub fn replay_log(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let column_map = ColumnMap::new(&self.table, &self.shadow_table, client);
        let replay = LogTableReplay {
            log_table: self.log_table.clone(),
            shadow_table: self.shadow_table.clone(),
            table: self.table.clone(),
            column_map,
            primary_key: self.primary_key.clone(),
        };
        replay.replay_log(client)?;
        Ok(())
    }

    pub fn setup_migration(
        &self,
        pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
    ) -> anyhow::Result<()> {
        let mut client = pool.get()?;
        self.create_post_migrations_schema(&mut client)?;
        self.shadow_table.drop_if_exists(&mut client)?;
        self.create_shadow_table(&mut client)?;
        self.migrate_shadow_table(&mut client)?;
        let column_map = ColumnMap::new(&self.table, &self.shadow_table, &mut client);
        let replay = LogTableReplay {
            log_table: self.log_table.clone(),
            shadow_table: self.shadow_table.clone(),
            table: self.table.clone(),
            column_map,
            primary_key: self.primary_key.clone(),
        };
        replay.setup(&mut client)?;
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

    fn create_post_migrations_schema(&self, client: &mut Client) -> anyhow::Result<()> {
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations;")?;
        Ok(())
    }
}

pub struct MigrationOrchestrator {
    pub migration: Migration,
    pub pool: Pool<PostgresConnectionManager<R2d2NoTls>>,
}

impl MigrationOrchestrator {
    pub fn new(migration: Migration, pool: Pool<PostgresConnectionManager<R2d2NoTls>>) -> Self {
        Self { migration, pool }
    }

    pub fn start_log_replay_thread(
        &self,
        stop_replay: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;
        let mut replay_client = self.pool.get().expect("Failed to get replay client");
        let column_map = ColumnMap::new(
            &self.migration.table,
            &self.migration.shadow_table,
            &mut replay_client,
        );
        let replay = LogTableReplay {
            log_table: self.migration.log_table.clone(),
            shadow_table: self.migration.shadow_table.clone(),
            table: self.migration.table.clone(),
            column_map,
            primary_key: self.migration.primary_key.clone(),
        };
        let stop_replay_clone = stop_replay.clone();
        thread::spawn(move || {
            while !stop_replay_clone.load(Ordering::Relaxed) {
                let _ = replay.replay_log(&mut replay_client).is_err();
                thread::sleep(Duration::from_millis(200));
            }
        })
    }

    pub fn start_backfill_thread(&self) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let table = self.migration.table.clone();
        let shadow_table = self.migration.shadow_table.clone();
        let mut backfill_client = self.pool.get().expect("Failed to get backfill client");
        let column_map = ColumnMap::new(&table, &shadow_table, &mut backfill_client);
        let backfill = BatchedBackfill { batch_size: 1000 };
        std::thread::spawn(move || {
            backfill.backfill(&table, &shadow_table, &column_map, &mut backfill_client)
        })
    }

    pub fn orchestrate(&self, execute: bool) -> anyhow::Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let client = self.pool.get()?;
        drop(client);
        self.migration.setup_migration(&self.pool)?;
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = self.start_log_replay_thread(stop_replay.clone());
        let backfill_handle = self.start_backfill_thread();
        backfill_handle.join().expect("Backfill thread panicked")?;
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        let mut client = self.pool.get()?;
        self.migration.replay_log(&mut client)?;
        if execute {
            self.migration.swap_tables(&mut client)?;
            self.migration.old_table.drop_if_exists(&mut client)?;
        } else {
            self.migration.shadow_table.drop_if_exists(&mut client)?;
        }
        Ok(())
    }
}

// Remove the moved tests from migration.rs

// Helper to get the list of columns for a table (excluding dropped columns)
// (Moved to Table::get_columns)
