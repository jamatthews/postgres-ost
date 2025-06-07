pub trait Parse {
    fn extract_tables(&self, sql: &str) -> Vec<String>;
    fn migrate_shadow_table_statement(
        &self,
        ast: &[sqlparser::ast::Statement],
        table_name: &str,
        shadow_table_name: &str,
    ) -> String;
}
