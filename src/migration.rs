use anyhow::Result;
use itertools::Itertools;
use postgres::Client;
use sqlparser::ast::{Ident, ObjectName, VisitMut, VisitorMut, visit_relations};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;
use crate::backfill::{Backfill, BatchedBackfill};
use crate::replay::{LogTableReplay, Replay};
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};

pub struct Migration {
    pub ast: Vec<sqlparser::ast::Statement>,
    pub table_name: String,
    pub shadow_table_name: String,
    pub log_table_name: String,
    pub old_table_name: String,
}

impl Migration {
    pub fn new(sql: &str) -> Self {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let tables = extract_tables(sql);
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
        Migration {
            ast: ast,
            table_name: table_name.to_string(),
            shadow_table_name: shadow_table_name.clone(),
            log_table_name: log_table_name.clone(),
            old_table_name: old_table_name.clone(),
        }
    }

    pub fn drop_shadow_table_if_exists(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let drop_shadow_table_statement =
            format!("DROP TABLE IF EXISTS {}", self.shadow_table_name);
        println!("Dropping shadow table:\n{:?}", drop_shadow_table_statement);
        client.simple_query(&drop_shadow_table_statement)?;
        Ok(())
    }

    pub fn create_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let create_table_statement = format!(
            "CREATE TABLE {} (LIKE {} INCLUDING ALL)",
            self.shadow_table_name, self.table_name
        );
        println!("Creating shadow table:\n{:?}", create_table_statement);
        client.simple_query(&create_table_statement)?;
        Ok(())
    }

    pub fn migrate_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let altered_statement =
            migrate_shadow_table_statement(&self.ast, &self.table_name, &self.shadow_table_name);
        println!("Migrating shadow table:\n{:?}", altered_statement);
        client.batch_execute(&altered_statement)?;
        Ok(())
    }

    pub fn create_log_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let create_log_statement = format!(
            "CREATE TABLE IF NOT EXISTS {} (post_migration_log_id BIGSERIAL PRIMARY KEY, operation TEXT, timestamp TIMESTAMPTZ DEFAULT NOW(), LIKE {})",
            self.log_table_name, self.table_name
        );
        println!("Creating log table:\n{:?}", create_log_statement);
        client.simple_query(&create_log_statement)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn backfill_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        BatchedBackfill { batch_size: 1000 }.backfill(&self.table_name, &self.shadow_table_name, client)
    }

    pub fn replay_log(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let replay = LogTableReplay {
            log_table_name: self.log_table_name.clone(),
            shadow_table_name: self.shadow_table_name.clone(),
            table_name: self.table_name.clone(),
        };
        replay.replay_log(client)?;
        Ok(())
    }

    pub fn drop_old_table_if_exists(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let drop_old_table_statement = format!("DROP TABLE IF EXISTS {}", self.old_table_name);
        println!("Dropping old table:\n{:?}", drop_old_table_statement);
        client.simple_query(&drop_old_table_statement)?;
        Ok(())
    }

    pub fn swap_tables(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        let swap_statement = format!(
            "BEGIN; ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}; COMMIT;",
            self.table_name, self.old_table_name, self.shadow_table_name, self.table_name
        );
        println!("Swapping tables:\n{:?}", swap_statement);
        client.simple_query(&swap_statement)?;
        Ok(())
    }

    pub fn orchestrate(&self, pool: &Pool<PostgresConnectionManager<R2d2NoTls>>, execute: bool) -> anyhow::Result<()> {
        let mut client = pool.get()?;
        // Leave the create schema in main
        self.drop_shadow_table_if_exists(&mut client)?;
        self.create_shadow_table(&mut client)?;
        self.migrate_shadow_table(&mut client)?;
        self.create_log_table(&mut client)?;
        // Run backfill in a background thread
        let table_name = self.table_name.clone();
        let shadow_table_name = self.shadow_table_name.clone();
        let mut replay_client = pool.get()?;
        let replay = LogTableReplay {
            log_table_name: self.log_table_name.clone(),
            shadow_table_name: shadow_table_name.clone(),
            table_name: table_name.clone(),
        };
        replay.replay_log(&mut replay_client)?;

        let mut backfill_client = pool.get()?;
        let backfill = BatchedBackfill { batch_size: 1000 };
        let backfill_handle = std::thread::spawn(move || {
            backfill.backfill(&table_name, &shadow_table_name, &mut backfill_client).expect("Backfill failed");
        });
        backfill_handle.join().expect("Backfill thread panicked");
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

#[derive(Debug)]
struct TableNameRewriter {
    rename_table_from: String,
    rename_table_to: String,
}

impl VisitorMut for TableNameRewriter {
    type Break = ();

    fn post_visit_relation(&mut self, object_name: &mut ObjectName) -> ControlFlow<Self::Break> {
        if object_name.to_string() == self.rename_table_from {
            *object_name = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
                Ident::new(&self.rename_table_to),
            )]);
        }
        ControlFlow::Continue(())
    }
}

fn extract_tables(sql: &str) -> Vec<String> {
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let mut tables = vec![];
    let _ = visit_relations(&ast, |relation| {
        tables.push(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    tables
}

fn migrate_shadow_table_statement(
    ast: &[sqlparser::ast::Statement],
    table_name: &str,
    shadow_table_name: &str,
) -> String {
    let mut rewriter = TableNameRewriter {
        rename_table_from: table_name.to_string(),
        rename_table_to: shadow_table_name.to_string(),
    };
    let mut altered_ast = ast.to_vec();
    let _ = altered_ast.visit(&mut rewriter);
    altered_ast[0].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tables() {
        let sql = "ALTER TABLE test_table ADD COLUMN bigint id";
        let tables = extract_tables(sql);
        assert_eq!(tables, vec!["test_table"]);
    }

    #[test]
    fn test_migrate_shadow_table_statement() {
        let sql = "ALTER TABLE test_table ADD COLUMN bigint id";
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let rewritten =
            migrate_shadow_table_statement(&ast, "test_table", "post_migrations.test_table");
        assert_eq!(
            rewritten,
            "ALTER TABLE post_migrations.test_table ADD COLUMN bigint id"
        );
    }
}
