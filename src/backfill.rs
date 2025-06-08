pub trait Backfill {
    fn backfill(
        &self,
        table_name: &str,
        shadow_table_name: &str,
        column_map: &crate::ColumnMap,
        client: &mut postgres::Client,
    ) -> anyhow::Result<()>;
}

pub struct SimpleBackfill;

impl Backfill for SimpleBackfill {
    fn backfill(
        &self,
        table_name: &str,
        shadow_table_name: &str,
        column_map: &crate::ColumnMap,
        client: &mut postgres::Client,
    ) -> anyhow::Result<()> {
        let shadow_cols = column_map.shadow_cols();
        let main_cols = column_map.main_cols();
        let insert_cols_csv = shadow_cols.join(", ");
        let select_cols_csv = main_cols.join(", ");
        let backfill_statement = format!(
            "INSERT INTO {} ({}) SELECT {} FROM {}",
            shadow_table_name, insert_cols_csv, select_cols_csv, table_name
        );
        println!("Backfilling shadow table:\n{:?}", backfill_statement);
        client.simple_query(&backfill_statement)?;
        Ok(())
    }
}

pub struct BatchedBackfill {
    pub batch_size: usize,
}

impl Backfill for BatchedBackfill {
    fn backfill(
        &self,
        table_name: &str,
        shadow_table_name: &str,
        column_map: &crate::ColumnMap,
        client: &mut postgres::Client,
    ) -> anyhow::Result<()> {
        let batch_size = self.batch_size;
        let shadow_cols = column_map.shadow_cols();
        let main_cols = column_map.main_cols();
        let insert_cols_csv = shadow_cols.join(", ");
        let select_cols_csv = main_cols.join(", ");
        let mut last_seen_id: Option<i64> = None;
        loop {
            let rows = if let Some(last_id) = last_seen_id {
                let backfill_statement = format!(
                    "INSERT INTO {} ({}) SELECT {} FROM {} WHERE id > $1 ORDER BY id ASC LIMIT {} RETURNING id",
                    shadow_table_name, insert_cols_csv, select_cols_csv, table_name, batch_size
                );
                println!(
                    "Batched backfilling shadow table:\n{:?}",
                    backfill_statement
                );
                client.query(&backfill_statement, &[&last_id])?
            } else {
                let backfill_statement = format!(
                    "INSERT INTO {} ({}) SELECT {} FROM {} ORDER BY id ASC LIMIT {} RETURNING id",
                    shadow_table_name, insert_cols_csv, select_cols_csv, table_name, batch_size
                );
                println!(
                    "Batched backfilling shadow table:\n{:?}",
                    backfill_statement
                );
                client.query(&backfill_statement, &[])?
            };
            if rows.is_empty() {
                break;
            }
            last_seen_id = rows.last().map(|row| row.get::<_, i64>(0));
        }
        Ok(())
    }
}
