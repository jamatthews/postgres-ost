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
        // Parse schema and table from shadow_table_name
        let (shadow_schema, shadow_table) = if let Some((schema, table)) = shadow_table_name.split_once('.') {
            (Some(schema.to_string()), table.to_string())
        } else {
            (None, shadow_table_name.to_string())
        };
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
                                            relation.relname = shadow_table.clone();
                                            if let Some(schema) = &shadow_schema {
                                                relation.schemaname = schema.clone();
                                            }
                                            changed = true;
                                        }
                                    }
                                }
                                Some(NodeEnum::DropStmt(drop_stmt)) => {
                                    for obj in &mut drop_stmt.objects {
                                        if let Some(NodeEnum::List(list)) = obj.node.as_mut() {
                                            let len = list.items.len();
                                            if len > 0 {
                                                // Find the last String node (should be the table name)
                                                if let Some(NodeEnum::String(s)) = list.items[len - 1].node.as_mut() {
                                                    if s.sval == table_name {
                                                        s.sval = shadow_table.clone();
                                                        changed = true;
                                                        if let Some(schema) = &shadow_schema {
                                                            // If schema is present, set or insert as the second-to-last String node
                                                            if len > 1 {
                                                                if let Some(NodeEnum::String(schema_node)) = list.items[len - 2].node.as_mut() {
                                                                    schema_node.sval = schema.clone();
                                                                }
                                                            } else {
                                                                // Insert schema node before table node
                                                                list.items.insert(0, pg_query::protobuf::Node {
                                                                    node: Some(NodeEnum::String(pg_query::protobuf::String {
                                                                        sval: schema.clone(),
                                                                    })),
                                                                });
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(NodeEnum::RenameStmt(rename_stmt)) => {
                                    if let Some(relation) = &mut rename_stmt.relation {
                                        if relation.relname == table_name {
                                            relation.relname = shadow_table.clone();
                                            if let Some(schema) = &shadow_schema {
                                                relation.schemaname = schema.clone();
                                            }
                                            changed = true;
                                        }
                                    }
                                }
                                Some(NodeEnum::CreateStmt(create_stmt)) => {
                                    if let Some(relation) = &mut create_stmt.relation {
                                        if relation.relname == table_name {
                                            relation.relname = shadow_table.clone();
                                            if let Some(schema) = &shadow_schema {
                                                relation.schemaname = schema.clone();
                                            }
                                            changed = true;
                                        }
                                    }
                                    // Also rewrite PARTITION OF references (inh_relations)
                                    for inh in &mut create_stmt.inh_relations {
                                        if let Some(NodeEnum::RangeVar(range_var)) = inh.node.as_mut() {
                                            if range_var.relname == table_name {
                                                range_var.relname = shadow_table.clone();
                                                if let Some(schema) = &shadow_schema {
                                                    range_var.schemaname = schema.clone();
                                                }
                                                changed = true;
                                            }
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

impl PgQueryParser {
    /// Extracts the main table name from a SQL statement (first table in DDL)
    pub fn extract_main_table(&self, sql: &str) -> Option<String> {
        for stmt_sql in sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            if let Ok(result) = pg_query::parse(stmt_sql) {
                // Try to get the first table from the parsed tables list
                if let Some((name, _)) = result.tables.into_iter().next() {
                    return Some(name);
                }
                // Fallback: look for first DDL table in protobuf
                if let Some(stmt) = result.protobuf.stmts.first() {
                    if let Some(node) = stmt.stmt.as_ref().map(|s| &s.node) {
                        use pg_query::NodeEnum;
                        match node {
                            Some(NodeEnum::AlterTableStmt(alter_table)) => {
                                if let Some(relation) = &alter_table.relation {
                                    return Some(relation.relname.clone());
                                }
                            }
                            Some(NodeEnum::DropStmt(drop_stmt)) => {
                                for obj in &drop_stmt.objects {
                                    if let Some(NodeEnum::List(list)) = obj.node.as_ref() {
                                        for item in &list.items {
                                            if let Some(NodeEnum::String(s)) = item.node.as_ref() {
                                                return Some(s.sval.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            Some(NodeEnum::RenameStmt(rename_stmt)) => {
                                if let Some(relation) = &rename_stmt.relation {
                                    return Some(relation.relname.clone());
                                }
                            }
                            Some(NodeEnum::CreateStmt(create_stmt)) => {
                                if let Some(relation) = &create_stmt.relation {
                                    return Some(relation.relname.clone());
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        None
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
            "ALTER TABLE post_migrations.test_table ADD COLUMN id bigint"
        );
    }

    #[test]
    fn test_migrate_shadow_table_statement_drop_and_create_partitioned() {
        let sql = "DROP TABLE test_table; CREATE TABLE test_table (id bigint) PARTITION BY RANGE (id)";
        let parser = PgQueryParser;
        let rewritten = parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        assert_eq!(rewritten, "DROP TABLE post_migrations.test_table; CREATE TABLE post_migrations.test_table (id bigint) PARTITION BY RANGE(id)");
    }

    #[test]
    fn test_extract_main_table_simple() {
        let sql = "ALTER TABLE test_table ADD COLUMN foo INT";
        let parser = PgQueryParser;
        let main = parser.extract_main_table(sql);
        assert_eq!(main, Some("test_table".to_string()));
    }

    #[test]
    fn test_extract_main_table_drop_and_create_partitioned() {
        let sql = "DROP TABLE test_table; CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT) PARTITION BY HASH (id); CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); CREATE TABLE test_table_p1 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1);";
        let parser = PgQueryParser;
        let main = parser.extract_main_table(sql);
        // Should return the first table being dropped/created, i.e., test_table
        assert_eq!(main, Some("test_table".to_string()));
    }

    #[test]
    fn test_migrate_shadow_table_statement_drop_and_create_partitioned_full() {
        let sql = "DROP TABLE test_table; \
            CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, assertable TEXT, target TEXT) PARTITION BY HASH (id); \
            CREATE TABLE test_table_p0 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
            CREATE TABLE test_table_p1 PARTITION OF test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1);";
        let parser = PgQueryParser;
        let rewritten = parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        let expected = "DROP TABLE post_migrations.test_table; \
            CREATE TABLE post_migrations.test_table (id bigserial PRIMARY KEY, assertable text, target text) PARTITION BY HASH(id); \
            CREATE TABLE test_table_p0 PARTITION OF post_migrations.test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
            CREATE TABLE test_table_p1 PARTITION OF post_migrations.test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1)";
        let norm_lines = |s: &str| s.lines().map(str::trim).filter(|l| !l.is_empty()).map(|l| l.to_string()).collect::<Vec<String>>();
        assert_eq!(norm_lines(&rewritten), norm_lines(expected));
    }

    #[test]
    fn test_migrate_shadow_table_statement_with_non_public_schema() {
        let sql = "ALTER TABLE my_schema.test_table ADD COLUMN foo TEXT; DROP TABLE my_schema.test_table;";
        let parser = PgQueryParser;
        let rewritten = parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        let expected = "ALTER TABLE post_migrations.test_table ADD COLUMN foo text; DROP TABLE post_migrations.test_table";
        let norm_lines = |s: &str| s.lines().map(str::trim).filter(|l| !l.is_empty()).map(|l| l.to_string()).collect::<Vec<String>>();
        assert_eq!(norm_lines(&rewritten), norm_lines(expected));
    }
}
