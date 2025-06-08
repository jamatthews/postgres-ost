use crate::ColumnMap;
use crate::backfill::{Backfill, BatchedBackfill};
use crate::replay::Replay;
use crate::{LogTableReplay, Migration};
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

    pub fn start_log_replay_thread(
        &self,
        stop_replay: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        use std::sync::atomic::Ordering;
        use std::thread;
        use std::time::Duration;
        let mut replay_client = self.pool.get().expect("Failed to get replay client");
        let column_map = ColumnMap::new(
            &self.migration.table,
            &self.migration.shadow_table,
            &mut replay_client,
        );
        let replay = LogTableReplay {
            log_table: self.migration.log_table.clone(),
            shadow_table: self.migration.shadow_table.clone(),
            table: self.migration.table.clone(),
            column_map,
            primary_key: self.migration.primary_key.clone(),
        };
        let stop_replay_clone = stop_replay.clone();
        thread::spawn(move || {
            while !stop_replay_clone.load(Ordering::Relaxed) {
                let _ = replay.replay_log(&mut replay_client).is_err();
                thread::sleep(Duration::from_millis(200));
            }
        })
    }

    pub fn start_backfill_thread(&self) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let table = self.migration.table.clone();
        let shadow_table = self.migration.shadow_table.clone();
        let mut backfill_client = self.pool.get().expect("Failed to get backfill client");
        let column_map = ColumnMap::new(&table, &shadow_table, &mut backfill_client);
        let backfill = BatchedBackfill { batch_size: 1000 };
        std::thread::spawn(move || {
            backfill.backfill(&table, &shadow_table, &column_map, &mut backfill_client)
        })
    }

    pub fn orchestrate(&self, execute: bool) -> anyhow::Result<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let client = self.pool.get()?;
        drop(client);
        self.migration.setup_migration(&self.pool)?;
        let stop_replay = Arc::new(AtomicBool::new(false));
        let replay_handle = self.start_log_replay_thread(stop_replay.clone());
        let backfill_handle = self.start_backfill_thread();
        backfill_handle.join().expect("Backfill thread panicked")?;
        stop_replay.store(true, Ordering::Relaxed);
        replay_handle.join().expect("Replay thread panicked");
        let mut client = self.pool.get()?;
        self.migration.replay_log(&mut client)?;
        if execute {
            self.migration.swap_tables(&mut client)?;
            self.migration.old_table.drop_if_exists(&mut client)?;
        } else {
            self.migration.shadow_table.drop_if_exists(&mut client)?;
        }
        Ok(())
    }
}
