// replay.rs
// Defines the Replay trait and LogTableReplay struct for log replay functionality.

use postgres::Client;
use anyhow::Result;
use crate::column_map::ColumnMap;
use postgres::types::Type;
use crate::migration::PrimaryKeyInfo;

pub trait Replay {
    fn replay_log(&self, client: &mut Client) -> Result<()>;
}

pub struct LogTableReplay {
    pub log_table_name: String,
    pub shadow_table_name: String,
    pub table_name: String,
    pub column_map: ColumnMap,
    pub primary_key: PrimaryKeyInfo,
}

impl Replay for LogTableReplay {
    fn replay_log(&self, client: &mut Client) -> Result<()> {
        let mut txn = client.transaction()?;
        let rows = self.fetch_batch(&mut txn, 100)?;
        let statements = self.batch2sql(&rows, &self.column_map);
        for stmt in statements {
            txn.batch_execute(&stmt)?;
        }
        txn.commit()?;
        Ok(())
    }
}

impl LogTableReplay {
    /// Fetches and deletes a batch of N rows from the log table, ordered by post_migration_log_id, returning the deleted rows.
    pub fn fetch_batch(&self, client: &mut postgres::Transaction, batch_size: usize) -> Result<Vec<postgres::Row>> {
        let query = format!(
            "DELETE FROM {} WHERE post_migration_log_id IN (
                SELECT post_migration_log_id FROM {} ORDER BY post_migration_log_id ASC LIMIT $1
            ) RETURNING *",
            self.log_table_name, self.log_table_name
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
                let stmt = format!("DELETE FROM {} WHERE {} = {}", self.shadow_table_name, pk_col, pk_sql);
                statements.push(stmt);
            } else if operation == "INSERT" {
                let stmt = format!(
                    "INSERT INTO {shadow} ({cols}) SELECT {selectCols} FROM {main} WHERE {pk_col} = {pk_val}",
                    shadow = self.shadow_table_name,
                    main = self.table_name,
                    cols = insert_cols_csv,
                    selectCols = select_cols_csv,
                    pk_col = pk_col,
                    pk_val = pk_sql
                );
                statements.push(stmt);
            } else if operation == "UPDATE" {
                let set_clause = shadow_cols.iter().zip(main_cols.iter())
                    .map(|(shadow_col, main_col)| format!("{} = (SELECT {} FROM {} WHERE {} = {})", shadow_col, main_col, self.table_name, pk_col, pk_sql))
                    .collect::<Vec<_>>().join(", ");
                let stmt = format!(
                    "UPDATE {shadow} SET {set_clause} WHERE {pk_col} = {pk_val}",
                    shadow = self.shadow_table_name,
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
