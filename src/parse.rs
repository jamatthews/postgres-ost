pub trait Parse {
    fn extract_tables(&self, sql: &str) -> Vec<String>;
    fn migrate_shadow_table_statement(
        &self,
        sql: &str,
        table_name: &str,
        shadow_table_name: &str,
    ) -> String;
}
