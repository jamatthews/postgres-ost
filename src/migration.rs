use crate::backfill::Backfill;
use crate::table::Table;
use crate::{BatchedBackfill, LogTableReplay, Parse, Replay};
use anyhow::Result;
use postgres::Client;
use postgres::GenericClient;
use postgres::types::Type;

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

    pub fn setup_migration(&self, client: &mut Client) -> anyhow::Result<()> {
        self.create_post_migrations_schema(client)?;
        self.shadow_table.drop_if_exists(client)?;
        self.create_shadow_table(client)?;
        self.migrate_shadow_table(client)?;
        let column_map = ColumnMap::new(&self.table, &self.shadow_table, client);
        let replay = LogTableReplay {
            log_table: self.log_table.clone(),
            shadow_table: self.shadow_table.clone(),
            table: self.table.clone(),
            column_map,
            primary_key: self.primary_key.clone(),
        };
        replay.setup(client)?;
        Ok(())
    }

    pub fn swap_tables<C: GenericClient>(&self, client: &mut C) -> Result<(), anyhow::Error> {
        let move_old = format!(
            "ALTER TABLE public.{table} SET SCHEMA post_migrations_old;",
            table = self.table
        );
        client.batch_execute(&move_old)?;
        // Move the shadow table into public schema
        let move_shadow = format!(
            "ALTER TABLE post_migrations.{table} SET SCHEMA public;",
            table = self.table
        );
        client.batch_execute(&move_shadow)?;
        Ok(())
    }

    fn create_post_migrations_schema(&self, client: &mut Client) -> anyhow::Result<()> {
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations;")?;
        client.simple_query("CREATE SCHEMA IF NOT EXISTS post_migrations_old;")?;
        Ok(())
    }
}

// Remove the moved tests from migration.rs

// Helper to get the list of columns for a table (excluding dropped columns)
// (Moved to Table::get_columns)
