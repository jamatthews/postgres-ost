use anyhow::Result;
use postgres::Client;
use crate::migration::Migration;

pub trait Backfill: Send + Sync {
    fn backfill(&self, migration: &Migration, client: &mut Client) -> Result<(), anyhow::Error>;
}

pub struct SimpleBackfill;

impl Backfill for SimpleBackfill {
    fn backfill(&self, migration: &Migration, client: &mut Client) -> Result<(), anyhow::Error> {
        let backfill_statement = format!(
            "INSERT INTO {} SELECT * FROM {}",
            migration.shadow_table_name, migration.table_name
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
    fn backfill(&self, migration: &Migration, client: &mut Client) -> Result<(), anyhow::Error> {
        let batch_size = self.batch_size;
        let mut last_seen_id: Option<i32> = None;
        loop {
            let rows = if let Some(last_id) = last_seen_id {
                let backfill_statement = format!(
                    "INSERT INTO {} SELECT * FROM {} WHERE id > $1 ORDER BY id ASC LIMIT {} RETURNING id",
                    migration.shadow_table_name, migration.table_name, batch_size
                );
                println!("Batched backfilling shadow table:\n{:?}", backfill_statement);
                client.query(&backfill_statement, &[&last_id])?
            } else {
                let backfill_statement = format!(
                    "INSERT INTO {} SELECT * FROM {} ORDER BY id ASC LIMIT {} RETURNING id",
                    migration.shadow_table_name, migration.table_name, batch_size
                );
                println!("Batched backfilling shadow table:\n{:?}", backfill_statement);
                client.query(&backfill_statement, &[])?
            };
            if rows.is_empty() {
                break;
            }
            last_seen_id = rows.last().map(|row| row.get::<_, i32>(0).clone());
        }
        Ok(())
    }
}
