pub trait Parse {
    fn extract_tables(&self, sql: &str) -> Vec<String>;
    fn migrate_shadow_table_statement(
        &self,
        sql: &str,
        table_name: &str,
        shadow_table_name: &str,
    ) -> String;
    fn extract_main_table(&self, sql: &str) -> Option<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_query_parser::PgQueryParser;

    #[test]
    fn test_extract_tables() {
        let sql = "ALTER TABLE test_table ADD COLUMN id bigint";
        let parser = PgQueryParser;
        let tables = parser.extract_tables(sql);
        assert_eq!(tables, vec!["test_table"]);
    }

    #[test]
    fn test_migrate_shadow_table_statement() {
        let sql = "ALTER TABLE test_table ADD COLUMN id bigint";
        let parser = PgQueryParser;
        let rewritten =
            parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        assert_eq!(
            rewritten,
            "ALTER TABLE post_migrations.test_table ADD COLUMN id bigint"
        );
    }

    #[test]
    fn test_migrate_shadow_table_statement_drop_and_create_partitioned() {
        let sql =
            "DROP TABLE test_table; CREATE TABLE test_table (id bigint) PARTITION BY RANGE (id)";
        let parser = PgQueryParser;
        let rewritten =
            parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        assert_eq!(
            rewritten,
            "DROP TABLE post_migrations.test_table; CREATE TABLE post_migrations.test_table (id bigint) PARTITION BY RANGE(id)"
        );
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
        let rewritten =
            parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        let expected = "DROP TABLE post_migrations.test_table; \
            CREATE TABLE post_migrations.test_table (id bigserial PRIMARY KEY, assertable text, target text) PARTITION BY HASH(id); \
            CREATE TABLE test_table_p0 PARTITION OF post_migrations.test_table FOR VALUES WITH (MODULUS 2, REMAINDER 0); \
            CREATE TABLE test_table_p1 PARTITION OF post_migrations.test_table FOR VALUES WITH (MODULUS 2, REMAINDER 1)";
        let norm_lines = |s: &str| {
            s.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(|l| l.to_string())
                .collect::<Vec<String>>()
        };
        assert_eq!(norm_lines(&rewritten), norm_lines(expected));
    }

    #[test]
    fn test_migrate_shadow_table_statement_with_non_public_schema() {
        let sql = "ALTER TABLE my_schema.test_table ADD COLUMN foo TEXT; DROP TABLE my_schema.test_table;";
        let parser = PgQueryParser;
        let rewritten =
            parser.migrate_shadow_table_statement(sql, "test_table", "post_migrations.test_table");
        let expected = "ALTER TABLE post_migrations.test_table ADD COLUMN foo text; DROP TABLE post_migrations.test_table";
        let norm_lines = |s: &str| {
            s.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(|l| l.to_string())
                .collect::<Vec<String>>()
        };
        assert_eq!(norm_lines(&rewritten), norm_lines(expected));
    }
}
