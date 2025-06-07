// replay.rs
// Defines the Replay trait and LogTableReplay struct for log replay functionality.

use postgres::Client;
use anyhow::Result;
use crate::column_map::ColumnMap;

pub trait Replay {
    fn replay_log(&self, client: &mut Client) -> Result<()>;
}

pub struct LogTableReplay {
    pub log_table_name: String,
    pub shadow_table_name: String,
    pub table_name: String,
    pub column_map: ColumnMap,
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
        for row in rows {
            let operation: String = row.get("operation");
            if operation == "DELETE" {
                let id: i64 = row.get("id");
                let stmt = format!("DELETE FROM {} WHERE id = {}", self.shadow_table_name, id);
                statements.push(stmt);
            } else if operation == "INSERT" {
                let id: i64 = row.get("id");
                let stmt = format!(
                    "INSERT INTO {shadow} ({cols}) SELECT {selectCols} FROM {main} WHERE id = {id}",
                    shadow = self.shadow_table_name,
                    main = self.table_name,
                    cols = insert_cols_csv,
                    selectCols = select_cols_csv,
                    id = id
                );
                statements.push(stmt);
            } else if operation == "UPDATE" {
                let id: i64 = row.get("id");
                // Generate: UPDATE shadow SET col1 = main.col1, col2 = main.col2, ... WHERE id = {id}
                let set_clause = shadow_cols.iter().zip(main_cols.iter())
                    .map(|(shadow_col, main_col)| format!("{} = (SELECT {} FROM {} WHERE id = {})", shadow_col, main_col, self.table_name, id))
                    .collect::<Vec<_>>().join(", ");
                let stmt = format!(
                    "UPDATE {shadow} SET {set_clause} WHERE id = {id}",
                    shadow = self.shadow_table_name,
                    set_clause = set_clause,
                    id = id
                );
                statements.push(stmt);
            }
        }
        statements
    }

}
