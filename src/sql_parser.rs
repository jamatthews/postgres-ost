// src/sql_parser.rs

use crate::Parse;
use sqlparser::ast::{ObjectName, VisitMut, VisitorMut, visit_relations};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::Ident;
use std::ops::ControlFlow;

#[derive(Debug)]
pub struct TableNameRewriter {
    pub rename_table_from: String,
    pub rename_table_to: String,
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

pub struct SqlParser;

impl Parse for SqlParser {
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
        let mut tables = vec![];
        let _ = visit_relations(&ast, |relation| {
            tables.push(relation.to_string());
            ControlFlow::<()>::Continue(())
        });
        tables
    }

    fn migrate_shadow_table_statement(
        &self,
        sql: &str,
        table_name: &str,
        shadow_table_name: &str,
    ) -> String {
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, &sql).unwrap();
        let mut rewriter = TableNameRewriter {
            rename_table_from: table_name.to_string(),
            rename_table_to: shadow_table_name.to_string(),
        };
        let mut altered_ast = ast.to_vec();
        let _ = altered_ast.visit(&mut rewriter);
        altered_ast[0].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tables() {
        let sql = "ALTER TABLE test_table ADD COLUMN bigint id";
        let parser = SqlParser;
        let tables = parser.extract_tables(sql);
        assert_eq!(tables, vec!["test_table"]);
    }

    #[test]
    fn test_migrate_shadow_table_statement() {
        let sql = "ALTER TABLE test_table ADD COLUMN bigint id";
        let parser = SqlParser;
        let rewritten =
            parser.migrate_shadow_table_statement(&sql, "test_table", "post_migrations.test_table");
        assert_eq!(
            rewritten,
            "ALTER TABLE post_migrations.test_table ADD COLUMN bigint id"
        );
    }
}
