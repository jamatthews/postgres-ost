// log_table_replay.rs
// Contains LogTableReplay and related logic.

use crate::{ColumnMap, PrimaryKeyInfo, Replay, Table};
use anyhow::Result;
use postgres::types::Type;

#[derive(Clone)]
pub struct LogTableReplay {
    pub log_table: Table,
    pub shadow_table: Table,
    pub table: Table,
    pub column_map: ColumnMap,
    pub primary_key: PrimaryKeyInfo,
}

impl LogTableReplay {
    /// Fetches and deletes a batch of N rows from the log table, ordered by post_migration_log_id, returning the deleted rows.
    pub fn fetch_batch(
        &self,
        client: &mut postgres::Transaction,
        batch_size: usize,
    ) -> Result<Vec<postgres::Row>> {
        let query = format!(
            "DELETE FROM {} WHERE post_migration_log_id IN (\
                SELECT post_migration_log_id FROM {} ORDER BY post_migration_log_id ASC LIMIT $1\
            ) RETURNING *",
            self.log_table, self.log_table
        );
        let rows = client.query(&query, &[&(batch_size as i64)])?;
        Ok(rows)
    }

    /// Converts a batch of log table rows to SQL statements to replay the changes.
    /// Handles DELETE and INSERT. For INSERT, uses a mapping of main to shadow columns, supporting dropped and renamed columns.
    pub fn batch2sql(&self, rows: &[postgres::Row], column_map: &ColumnMap) -> Vec<String> {
        let mut statements = Vec::new();
        let shadow_cols = column_map.shadow_cols();
        let main_cols = column_map.main_cols();
        let insert_cols_csv = shadow_cols.join(", ");
        let select_cols_csv = main_cols.join(", ");
        let pk_col = &self.primary_key.name;
        let pk_type = &self.primary_key.ty;
        for row in rows {
            let operation: String = row.get("operation");
            let pk_val = PrimaryKey::from_row(row, pk_col, pk_type);
            let pk_sql = pk_val.to_sql();
            if operation == "DELETE" {
                let stmt = format!(
                    "DELETE FROM {} WHERE {} = {}",
                    self.shadow_table, pk_col, pk_sql
                );
                statements.push(stmt);
            } else if operation == "INSERT" {
                let stmt = format!(
                    "INSERT INTO {shadow} ({cols}) SELECT {selectCols} FROM {main} WHERE {pk_col} = {pk_val}",
                    shadow = self.shadow_table,
                    main = self.table,
                    cols = insert_cols_csv,
                    selectCols = select_cols_csv,
                    pk_col = pk_col,
                    pk_val = pk_sql
                );
                statements.push(stmt);
            } else if operation == "UPDATE" {
                let set_clause = shadow_cols
                    .iter()
                    .zip(main_cols.iter())
                    .map(|(shadow_col, main_col)| {
                        format!(
                            "{} = (SELECT {} FROM {} WHERE {} = {})",
                            shadow_col, main_col, self.table, pk_col, pk_sql
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let stmt = format!(
                    "UPDATE {shadow} SET {set_clause} WHERE {pk_col} = {pk_val}",
                    shadow = self.shadow_table,
                    set_clause = set_clause,
                    pk_col = pk_col,
                    pk_val = pk_sql
                );
                statements.push(stmt);
            }
        }
        statements
    }
}

#[derive(Debug, Clone)]
pub enum PrimaryKey {
    I32(i32),
    I64(i64),
}

impl PrimaryKey {
    pub fn from_row(row: &postgres::Row, pk_col: &str, pk_type: &Type) -> Self {
        match *pk_type {
            Type::INT4 => PrimaryKey::I32(row.get::<_, i32>(pk_col)),
            Type::INT8 => PrimaryKey::I64(row.get::<_, i64>(pk_col)),
            _ => panic!("Unsupported primary key type: {:?}", pk_type),
        }
    }
    pub fn to_sql(&self) -> String {
        match self {
            PrimaryKey::I32(v) => v.to_string(),
            PrimaryKey::I64(v) => v.to_string(),
        }
    }
}

impl Replay for LogTableReplay {
    fn replay_log(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        let mut txn = client.transaction()?;
        let rows = self.fetch_batch(&mut txn, 100)?;
        let statements = self.batch2sql(&rows, &self.column_map);
        for stmt in statements {
            txn.batch_execute(&stmt)?;
        }
        txn.commit()?;
        Ok(())
    }
    fn setup(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        // Create log table
        let create_log_statement = format!(
            "CREATE TABLE IF NOT EXISTS {} (post_migration_log_id BIGSERIAL PRIMARY KEY, operation TEXT, timestamp TIMESTAMPTZ DEFAULT NOW(), LIKE {})",
            self.log_table, self.table
        );
        client.simple_query(&create_log_statement)?;

        let pk_col = &self.primary_key.name;
        // Insert trigger
        let insert_trigger = format!(
            r#"
            CREATE OR REPLACE FUNCTION {log_table}_insert_trigger_fn() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {log_table} (operation, {pk_col}) VALUES ('INSERT', NEW.{pk_col});
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS {table}_insert_trigger ON {table};
            CREATE TRIGGER {table}_insert_trigger
                AFTER INSERT ON {table}
                FOR EACH ROW EXECUTE FUNCTION {log_table}_insert_trigger_fn();
            "#,
            log_table = self.log_table,
            table = self.table,
            pk_col = pk_col
        );
        client.batch_execute(&insert_trigger)?;

        // Delete trigger
        let delete_trigger = format!(
            r#"
            CREATE OR REPLACE FUNCTION {log_table}_delete_trigger_fn() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {log_table} (operation, {pk_col}) VALUES ('DELETE', OLD.{pk_col});
                RETURN OLD;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS {table}_delete_trigger ON {table};
            CREATE TRIGGER {table}_delete_trigger
                AFTER DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION {log_table}_delete_trigger_fn();
            "#,
            log_table = self.log_table,
            table = self.table,
            pk_col = pk_col
        );
        client.batch_execute(&delete_trigger)?;

        // Update trigger
        let update_trigger = format!(
            r#"
            CREATE OR REPLACE FUNCTION {log_table}_update_trigger_fn() RETURNS trigger AS $$
            BEGIN
                INSERT INTO {log_table} (operation, {pk_col}) VALUES ('UPDATE', NEW.{pk_col});
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            DROP TRIGGER IF EXISTS {table}_update_trigger ON {table};
            CREATE TRIGGER {table}_update_trigger
                AFTER UPDATE ON {table}
                FOR EACH ROW EXECUTE FUNCTION {log_table}_update_trigger_fn();
            "#,
            log_table = self.log_table,
            table = self.table,
            pk_col = pk_col
        );
        client.batch_execute(&update_trigger)?;

        Ok(())
    }
    fn teardown(&self, transaction: &mut postgres::Transaction) -> anyhow::Result<()> {
        let drop_triggers_and_functions = format!(
            r#"
            DROP TRIGGER IF EXISTS {table}_insert_trigger ON {table};
            DROP TRIGGER IF EXISTS {table}_delete_trigger ON {table};
            DROP TRIGGER IF EXISTS {table}_update_trigger ON {table};
            DROP FUNCTION IF EXISTS {log_table}_insert_trigger_fn();
            DROP FUNCTION IF EXISTS {log_table}_delete_trigger_fn();
            DROP FUNCTION IF EXISTS {log_table}_update_trigger_fn();
            "#,
            table = self.table,
            log_table = self.log_table
        );
        transaction.batch_execute(&drop_triggers_and_functions)?;
        // Drop log table
        let drop_log_table = format!("DROP TABLE IF EXISTS {};", self.log_table);
        transaction.batch_execute(&drop_log_table)?;
        Ok(())
    }
    fn replay_log_until_complete(
        &self,
        transaction: &mut postgres::Transaction,
    ) -> anyhow::Result<()> {
        loop {
            let query = format!(
                "DELETE FROM {} WHERE post_migration_log_id IN (\
                    SELECT post_migration_log_id FROM {} ORDER BY post_migration_log_id ASC LIMIT $1\
                ) RETURNING *",
                self.log_table, self.log_table
            );
            let rows = transaction.query(&query, &[&100_i64])?;
            if rows.is_empty() {
                break;
            }
            let statements = self.batch2sql(&rows, &self.column_map);
            for stmt in statements {
                transaction.batch_execute(&stmt)?;
            }
        }
        Ok(())
    }
}
