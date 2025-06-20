// logical_replay.rs
// Contains LogicalReplay and related logic.

use crate::{ColumnMap, PrimaryKeyInfo, Replay};

#[derive(Clone)]
pub struct LogicalReplay {
    pub slot: crate::logical_replication::Slot,
    pub publication: crate::logical_replication::Publication,
    pub table: crate::table::Table,
    pub shadow_table: crate::table::Table,
    pub column_map: crate::ColumnMap,
    pub primary_key: crate::PrimaryKeyInfo,
}

impl Replay for LogicalReplay {
    fn replay_log(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        // Consume changes from the slot
        let rows = self.slot.get_changes(client, 100)?;
        let batch: Vec<serde_json::Value> = rows
            .iter()
            .filter_map(|row| {
                let data: String = row.get("data");
                serde_json::from_str(&data).ok()
            })
            .collect();
        let statements = wal2json2sql(
            &batch,
            &self.column_map,
            &self.table,
            &self.shadow_table,
            &self.primary_key,
        );
        for stmt in statements {
            client.batch_execute(&stmt)?;
        }
        Ok(())
    }
    fn setup(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        self.publication.create(client)?;
        self.slot.create_slot(client)?;
        Ok(())
    }
    fn teardown(&self, transaction: &mut postgres::Transaction) -> anyhow::Result<()> {
        self.slot.drop_slot(transaction)?;
        self.publication.drop(transaction)?;
        Ok(())
    }
    fn replay_log_until_complete(
        &self,
        transaction: &mut postgres::Transaction,
    ) -> anyhow::Result<()> {
        loop {
            let rows = self.slot.get_changes(transaction, 100)?;
            let batch: Vec<serde_json::Value> = rows
                .iter()
                .filter_map(|row| {
                    let data: String = row.get("data");
                    serde_json::from_str(&data).ok()
                })
                .collect();
            if batch.is_empty() {
                break;
            }
            let statements = wal2json2sql(
                &batch,
                &self.column_map,
                &self.table,
                &self.shadow_table,
                &self.primary_key,
            );
            for stmt in statements {
                transaction.batch_execute(&stmt)?;
            }
        }
        Ok(())
    }
}

/// Converts a batch of wal2json rows to SQL statements to replay the changes.
pub fn wal2json2sql(
    batch: &[serde_json::Value],
    column_map: &ColumnMap,
    main_table: &crate::table::Table,
    shadow_table: &crate::table::Table,
    primary_key: &PrimaryKeyInfo,
) -> Vec<String> {
    let mut statements = Vec::new();
    let shadow_cols = column_map.shadow_cols();
    let main_cols = column_map.main_cols();
    let insert_cols_csv = shadow_cols.join(", ");
    let select_cols_csv = main_cols.join(", ");
    let pk_col = &primary_key.name;
    let pk_type = &primary_key.ty;
    for json in batch {
        // Parse wal2json JSON and extract operation, pk, etc.
        if let Some(changes) = json.get("change").and_then(|c| c.as_array()) {
            for change in changes {
                let kind = change.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let pk_sql = if kind == "delete" {
                    // For DELETE, get PK from oldkeys.keyvalues
                    change
                        .get("oldkeys")
                        .and_then(|ok| ok.get("keyvalues"))
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .map(|pk_val| {
                            if pk_type == &postgres::types::Type::INT4 {
                                pk_val.as_i64().unwrap_or(0).to_string()
                            } else {
                                pk_val.to_string()
                            }
                        })
                        .unwrap_or("NULL".to_string())
                } else {
                    // For insert/update, get PK from columnvalues
                    change
                        .get("columnvalues")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .map(|pk_val| {
                            if pk_type == &postgres::types::Type::INT4 {
                                pk_val.as_i64().unwrap_or(0).to_string()
                            } else {
                                pk_val.to_string()
                            }
                        })
                        .unwrap_or("NULL".to_string())
                };
                match kind {
                    "delete" => {
                        let stmt =
                            format!("DELETE FROM {} WHERE {} = {}", shadow_table, pk_col, pk_sql);
                        statements.push(stmt);
                    }
                    "insert" => {
                        let stmt = format!(
                            "INSERT INTO {shadow} ({cols}) SELECT {selectCols} FROM {main} WHERE {pk_col} = {pk_val}",
                            shadow = shadow_table,
                            main = main_table,
                            cols = insert_cols_csv,
                            selectCols = select_cols_csv,
                            pk_col = pk_col,
                            pk_val = pk_sql
                        );
                        statements.push(stmt);
                    }
                    "update" => {
                        let set_clause = shadow_cols
                            .iter()
                            .zip(main_cols.iter())
                            .map(|(shadow_col, main_col)| {
                                format!(
                                    "{} = (SELECT {} FROM {} WHERE {} = {})",
                                    shadow_col, main_col, main_table, pk_col, pk_sql
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        let stmt = format!(
                            "UPDATE {shadow} SET {set_clause} WHERE {pk_col} = {pk_val}",
                            shadow = shadow_table,
                            set_clause = set_clause,
                            pk_col = pk_col,
                            pk_val = pk_sql
                        );
                        statements.push(stmt);
                    }
                    _ => {}
                }
            }
        }
    }
    statements
}

/// Emit a logical replication message to the stream using pg_logical_emit_message.
/// This is used to mark a point in the WAL for cutover replay completion.
pub fn emit_replay_complete_message(client: &mut postgres::Client) -> anyhow::Result<()> {
    // non-transactional, prefix as text, body as bytea, flush immediately
    client.batch_execute(
        "SELECT pg_logical_emit_message(false, 'postgres-ost', convert_to('replay complete', 'UTF8'), true);"
    )?;
    Ok(())
}
