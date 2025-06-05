use anyhow::Result;
use postgres::Client;
use crate::migration::Migration;

pub trait BackfillStrategy: Send + Sync {
    fn backfill(&self, migration: &Migration, client: &mut Client) -> Result<(), anyhow::Error>;
}

pub struct SimpleBackfill;

impl BackfillStrategy for SimpleBackfill {
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

impl BackfillStrategy for BatchedBackfill {
    fn backfill(&self, migration: &Migration, client: &mut Client) -> Result<(), anyhow::Error> {
        let mut offset = 0;
        let batch_size = self.batch_size;
        loop {
            let backfill_statement = format!(
                "INSERT INTO {} SELECT * FROM {} ORDER BY id OFFSET {} LIMIT {}",
                migration.shadow_table_name, migration.table_name, offset, batch_size
            );
            println!("Batched backfilling shadow table:\n{:?}", backfill_statement);
            let rows_affected = client.execute(&backfill_statement, &[])?;
            if rows_affected == 0 {
                break;
            }
            offset += rows_affected as usize;
        }
        Ok(())
    }
}
