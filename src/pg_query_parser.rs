// src/pg_query_parser.rs

use crate::Parse;
use pg_query::{deparse, parse, NodeEnum};

pub struct PgQueryParser;

impl Parse for PgQueryParser {
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        match pg_query::parse(sql) {
            Ok(result) => {
                let mut tables: Vec<String> = result.tables.into_iter().map(|(name, _ctx)| name).collect();
                if tables.is_empty() {
                    // Fallback: try to extract table name from protobuf for RENAME COLUMN, etc.
                    use pg_query::NodeEnum;
                    for stmt in &result.protobuf.stmts {
                        if let Some(node) = stmt.stmt.as_ref().map(|s| &s.node) {
                            if let Some(NodeEnum::AlterTableStmt(alter_table)) = node.as_ref() {
                                if let Some(relation) = &alter_table.relation {
                                    tables.push(relation.relname.clone());
                                }
                            } else if let Some(NodeEnum::RenameStmt(rename_stmt)) = node.as_ref() {
                                if let Some(relation) = &rename_stmt.relation {
                                    tables.push(relation.relname.clone());
                                }
                            }
                        }
                    }
                }
                tables
            }
            Err(_) => vec![],
        }
    }

    fn migrate_shadow_table_statement(
        &self,
        sql: &str,
        table_name: &str,
        shadow_table_name: &str,
    ) -> String {
        // Split statements by semicolon and rewrite each one
        let stmts: Vec<&str> = sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        let mut rewritten_stmts = Vec::new();
        for stmt in stmts {
            let rewritten = match parse(stmt) {
                Ok(mut result) => {
                    let mut changed = false;
                    for stmt in &mut result.protobuf.stmts {
                        if let Some(node) = stmt.stmt.as_mut().map(|s| &mut s.node) {
                            match node {
                                Some(NodeEnum::AlterTableStmt(alter_table)) => {
                                    if let Some(relation) = &mut alter_table.relation {
                                        if relation.relname == table_name {
                                            relation.relname = shadow_table_name.to_string();
                                            changed = true;
                                        }
                                    }
                                }
                                Some(NodeEnum::DropStmt(drop_stmt)) => {
                                    for obj in &mut drop_stmt.objects {
                                        if let Some(NodeEnum::List(list)) = obj.node.as_mut() {
                                            for item in &mut list.items {
                                                if let Some(NodeEnum::String(s)) = item.node.as_mut() {
                                                    if s.sval == table_name {
                                                        s.sval = shadow_table_name.to_string();
                                                        changed = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(NodeEnum::RenameStmt(rename_stmt)) => {
                                    if let Some(relation) = &mut rename_stmt.relation {
                                        if relation.relname == table_name {
                                            relation.relname = shadow_table_name.to_string();
                                            changed = true;
                                        }
                                    }
                                }
                                Some(NodeEnum::CreateStmt(create_stmt)) => {
                                    if let Some(relation) = &mut create_stmt.relation {
                                        if relation.relname == table_name {
                                            relation.relname = shadow_table_name.to_string();
                                            changed = true;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    if changed {
                        deparse(&result.protobuf).unwrap_or_else(|_| stmt.to_string())
                    } else {
                        stmt.to_string()
                    }
                }
                Err(_) => stmt.to_string(),
            };
            rewritten_stmts.push(rewritten.trim().to_string());
        }
        rewritten_stmts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_query_extract_tables() {
        let sql = "ALTER TABLE test_table ADD COLUMN bigint id";
        let parser = PgQueryParser;
        let tables = parser.extract_tables(sql);
        assert_eq!(tables, vec!["test_table"]);
    }

    #[test]
    fn test_pg_query_extract_tables_drop() {
        let sql = "DROP TABLE test_table";
        let parser = PgQueryParser;
        let tables = parser.extract_tables(sql);
        assert_eq!(tables, vec!["test_table"]);
    }

    #[test]
    fn test_pg_query_extract_tables_rename_column() {
        let sql = "ALTER TABLE test_table RENAME COLUMN old_col TO new_col";
        let parser = PgQueryParser;
        let tables = parser.extract_tables(sql);
        assert_eq!(tables, vec!["test_table"]);
    }

    #[test]
    fn test_migrate_shadow_table_statement() {
        let sql = "ALTER TABLE test_table ADD COLUMN id bigint";
        let parser = PgQueryParser;
        let rewritten = parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        assert_eq!(
            rewritten,
            "ALTER TABLE \"post_migrations.test_table\" ADD COLUMN id bigint"
        );
    }

    #[test]
    fn test_migrate_shadow_table_statement_drop_and_create_partitioned() {
        let sql = "DROP TABLE test_table; CREATE TABLE test_table (id bigint) PARTITION BY RANGE (id)";
        let parser = PgQueryParser;
        let rewritten = parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        assert_eq!(rewritten, "DROP TABLE \"post_migrations.test_table\"; CREATE TABLE \"post_migrations.test_table\" (id bigint) PARTITION BY RANGE(id)");
    }
}
