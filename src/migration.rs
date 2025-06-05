use anyhow::Result;
use itertools::Itertools;
use postgres::Client;
use sqlparser::ast::{Ident, ObjectName, VisitMut, VisitorMut, visit_relations};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;
use crate::backfill::{BackfillStrategy, SimpleBackfill};

pub struct Migration {
    pub ast: Vec<sqlparser::ast::Statement>,
    pub table_name: String,
    pub shadow_table_name: String,
    pub log_table_name: String,
    pub old_table_name: String,
    pub backfill_strategy: Box<dyn BackfillStrategy>,
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
            backfill_strategy: Box::new(SimpleBackfill),
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

    pub fn backfill_shadow_table(&self, client: &mut Client) -> Result<(), anyhow::Error> {
        self.backfill_strategy.backfill(self, client)
    }

    pub fn replay_log(&self, _client: &mut Client) -> Result<(), anyhow::Error> {
        let mut replay_statements = vec![];
        let rows = _client.query(
            &format!(
                "SELECT * FROM {} ORDER BY post_migrations_log_id",
                self.log_table_name
            ),
            &[],
        )?;

        for row in rows {
            let (id, operation): (i64, String) = (row.get("id"), row.get("operation"));
            let values = row
                .columns()
                .iter()
                .skip(3)
                .map(|column| row.get(column.name()))
                .collect::<Vec<String>>();
            match operation.as_str() {
                "DELETE" => replay_statements.push(format!(
                    "DELETE FROM {} WHERE id = {}",
                    self.shadow_table_name, id
                )),
                "INSERT" => replay_statements.push(format!(
                    "INSERT INTO {} VALUES ({})",
                    self.shadow_table_name,
                    values.join(", ")
                )),
                "UPDATE" => {
                    let column_list = row
                        .columns()
                        .iter()
                        .skip(3)
                        .map(|column| column.name())
                        .collect::<Vec<&str>>()
                        .join(", ");
                    replay_statements.push(format!(
                        "UPDATE {} SET ({}) = ({}) WHERE id = {}",
                        self.shadow_table_name,
                        column_list,
                        values.join(", "),
                        id
                    ));
                }
                _ => return Err(anyhow::anyhow!("Unknown operation: {}", operation)),
            }
        }

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
