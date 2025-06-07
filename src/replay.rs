// replay.rs
// Defines the Replay trait and LogTableReplay struct for log replay functionality.

use postgres::Client;
use anyhow::Result;

pub trait Replay {
    fn replay_log(&self, client: &mut Client) -> Result<()>;
}

pub struct LogTableReplay {
    pub log_table_name: String,
    pub shadow_table_name: String,
    pub table_name: String,
    pub column_map: ColumnMap,
}

#[derive(Clone)]
pub struct ColumnMap(pub Vec<(String, Option<String>)>);

impl ColumnMap {
    pub fn from_main_and_shadow(main_cols: &[String], shadow_cols: &[String]) -> Self {
        let mut map = Vec::new();
        let unmatched_main: Vec<String> = main_cols.iter().filter(|c| !shadow_cols.contains(c)).cloned().collect();
        let unmatched_shadow: Vec<String> = shadow_cols.iter().filter(|c| !main_cols.contains(c)).cloned().collect();
        for main_col in main_cols {
            if let Some(shadow_col) = shadow_cols.iter().find(|c| *c == main_col) {
                map.push((main_col.clone(), Some(shadow_col.clone())));
            } else if unmatched_main.len() == 1 && unmatched_shadow.len() == 1 && unmatched_main[0] == *main_col {
                // Assume rename
                map.push((main_col.clone(), Some(unmatched_shadow[0].clone())));
            } else {
                map.push((main_col.clone(), None));
            }
        }
        ColumnMap(map)
    }

    pub fn shadow_cols(&self) -> Vec<String> {
        self.0.iter().filter_map(|(_main, shadow)| shadow.clone()).collect()
    }
    pub fn main_cols(&self) -> Vec<String> {
        self.0.iter().filter_map(|(main, shadow)| shadow.as_ref().map(|_| main.clone())).collect()
    }
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
            }
            // Future: handle UPDATE
        }
        statements
    }

}
