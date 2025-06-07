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
}

impl Replay for LogTableReplay {
    fn replay_log(&self, client: &mut Client) -> Result<()> {
        let mut txn = client.transaction()?;
        // Build column mapping: Vec<(main_col, Option<shadow_col>)>
        let main_cols = get_table_columns(&mut txn, &self.table_name);
        let shadow_cols = get_table_columns(&mut txn, &self.shadow_table_name);
        let column_map: Vec<(String, Option<String>)> = main_cols.iter().map(|main_col| {
            if let Some(shadow_col) = shadow_cols.iter().find(|c| *c == main_col) {
                (main_col.clone(), Some(shadow_col.clone()))
            } else {
                (main_col.clone(), None)
            }
        }).collect();
        let rows = self.fetch_batch(&mut txn, 100)?;
        let statements = self.batch2sql(&rows, &column_map);
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
    pub fn batch2sql(&self, rows: &[postgres::Row], column_map: &[(String, Option<String>)]) -> Vec<String> {
        let mut statements = Vec::new();
        // Build the list of shadow columns and the corresponding main columns for SELECT
        let shadow_cols: Vec<String> = column_map.iter().filter_map(|(_main, shadow)| shadow.clone()).collect();
        let main_cols: Vec<String> = column_map.iter().filter_map(|(main, shadow)| shadow.as_ref().map(|_| main.clone())).collect();
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
                // Insert only mapped columns (renamed/dropped handled by column_map)
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

/// Helper to get the list of columns for a table (excluding dropped columns)
fn get_table_columns<T: postgres::GenericClient>(client: &mut T, table: &str) -> Vec<String> {
    let (schema, table) = if let Some((schema, table)) = table.split_once('.') {
        (schema, table)
    } else {
        ("public", table)
    };
    let rows = client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
        &[&schema, &table],
    ).unwrap();
    rows.iter().map(|row| row.get::<_, String>("column_name")).collect()
}
