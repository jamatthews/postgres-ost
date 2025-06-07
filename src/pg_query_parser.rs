// src/pg_query_parser.rs

use crate::Parse;
use sqlparser::ast::Statement;

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
        _ast: &[Statement],
        _table_name: &str,
        _shadow_table_name: &str,
    ) -> String {
        unimplemented!("migrate_shadow_table_statement is not implemented for PgQueryParser");
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

    // PgQueryParser does not implement migrate_shadow_table_statement, so this test is not applicable.
    // #[test]
    // fn test_migrate_shadow_table_statement() {
    //     let sql = "ALTER TABLE test_table ADD COLUMN bigint id";
    //     let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    //     let parser = SqlParser;
    //     let rewritten =
    //         parser.migrate_shadow_table_statement(&ast, "test_table", "post_migrations.test_table");
    //     assert_eq!(
    //         rewritten,
    //         "ALTER TABLE post_migrations.test_table ADD COLUMN bigint id"
    //     );
    // }
}
