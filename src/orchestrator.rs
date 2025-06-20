use crate::backfill::{Backfill, BatchedBackfill};
use crate::{ColumnMap, Migration, Replay};
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};

pub struct MigrationOrchestrator {
    pub migration: crate::Migration,
    pub pool: r2d2::Pool<r2d2_postgres::PostgresConnectionManager<r2d2_postgres::postgres::NoTls>>,
}

impl MigrationOrchestrator {
    pub fn new(migration: Migration, pool: Pool<PostgresConnectionManager<R2d2NoTls>>) -> Self {
        Self { migration, pool }
    }

    pub fn start_log_replay_thread<R: crate::replay::Replay + Send + Sync + 'static>(
        &self,
        replay: R,
        stop_replay: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;
        let mut replay_client = self.pool.get().expect("Failed to get replay client");
        let stop_replay_clone = stop_replay.clone();
        thread::spawn(move || {
            while !stop_replay_clone.load(Ordering::Relaxed) {
                let _ = replay.replay_log(&mut replay_client).is_err();
                thread::sleep(Duration::from_millis(200));
            }
        })
    }

    pub fn start_backfill_thread(
        &self,
        column_map: ColumnMap,
        table: crate::table::Table,
        shadow_table: crate::table::Table,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let mut backfill_client = self.pool.get().expect("Failed to get backfill client");
        let backfill = BatchedBackfill { batch_size: 1000 };
        std::thread::spawn(move || {
            backfill.backfill(&table, &shadow_table, &column_map, &mut backfill_client)
        })
    }

    /// Orchestrate the migration, assuming all setup is already done and a concrete Replay is provided.
    pub fn orchestrate<T: Replay + Clone + Send + Sync + 'static>(
        &self,
        execute: bool,
        column_map: ColumnMap,
        replay: T,
    ) -> anyhow::Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        // All setup (migration, column_map, replay construction) must be done by the caller
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = self.start_log_replay_thread(replay.clone(), stop_replay.clone());
        let backfill_handle = self.start_backfill_thread(
            column_map.clone(),
            self.migration.table.clone(),
            self.migration.shadow_table.clone(),
        );
        backfill_handle.join().expect("Backfill thread panicked")?;
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        let mut client = self.pool.get()?;
        if execute {
            let mut transaction = client.transaction()?;
            self.migration.table.lock_table(&mut transaction)?;
            replay.replay_log_until_complete(&mut transaction)?;
            replay.teardown(&mut transaction)?;
            self.migration.swap_tables(&mut transaction)?;
            transaction.commit()?;
        } else {
            let mut transaction = client.transaction()?;
            replay.teardown(&mut transaction)?;
            transaction.commit()?;
        }
        Ok(())
    }
}
