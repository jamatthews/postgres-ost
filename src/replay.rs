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
        // Fetch columns once per replay
        let main_cols = get_table_columns(&mut txn, &self.table_name);
        let shadow_cols = get_table_columns(&mut txn, &self.shadow_table_name);
        let rows = self.fetch_batch(&mut txn, 100)?;
        let statements = self.batch2sql(&rows, &main_cols, &shadow_cols);
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
    /// Handles DELETE and INSERT. For INSERT, only columns present in the shadow table are inserted.
    pub fn batch2sql(&self, rows: &[postgres::Row], main_cols: &[String], shadow_cols: &[String]) -> Vec<String> {
        let mut statements = Vec::new();
        let insert_cols: Vec<String> = shadow_cols.iter().filter(|c| main_cols.contains(c)).cloned().collect();
        let insert_cols_csv = insert_cols.join(", ");
        for row in rows {
            let operation: String = row.get("operation");
            if operation == "DELETE" {
                let id: i64 = row.get("id");
                let stmt = format!("DELETE FROM {} WHERE id = {}", self.shadow_table_name, id);
                statements.push(stmt);
            } else if operation == "INSERT" {
                let id: i64 = row.get("id");
                // Only insert columns that exist in the shadow table
                let stmt = format!(
                    "INSERT INTO {shadow} ({cols}) SELECT {cols} FROM {main} WHERE id = {id}",
                    shadow = self.shadow_table_name,
                    main = self.table_name,
                    cols = insert_cols_csv,
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
