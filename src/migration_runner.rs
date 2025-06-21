// Standard library imports
use std::sync::{Arc, atomic::AtomicBool};

// External crate imports
use anyhow::Result;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};

// Internal module imports
use crate::backfill::Backfill;
use crate::column_map::ColumnMap;
use crate::log_table_replay::LogTableReplay;
use crate::logical_replay::LogicalReplay;
use crate::logical_replication::{Publication, Slot};
use crate::migration::Migration;
use crate::orchestrator::MigrationOrchestrator;
use crate::replay::Replay;

pub struct MigrationRunner {
    pub pool: Pool<PostgresConnectionManager<R2d2NoTls>>,
}

pub enum ReplayKind {
    Log(LogTableReplay),
    Logical(LogicalReplay),
}

impl MigrationRunner {
    pub fn new(uri: &str) -> Result<Self> {
        let manager = PostgresConnectionManager::new(uri.parse()?, R2d2NoTls);
        let pool = Pool::new(manager)?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: Pool<PostgresConnectionManager<R2d2NoTls>>) -> Self {
        Self { pool }
    }

    pub fn run_schema_migration(&self, sql: &str) -> Result<(Migration, ColumnMap)> {
        let mut client = self.pool.get()?;
        let migration = Migration::new(sql, &mut client);
        migration.setup_migration(&mut client)?;
        let column_map = ColumnMap::new(&migration.table, &migration.shadow_table, &mut *client);
        Ok((migration, column_map))
    }

    pub fn run_migrate(&self, sql: &str, execute: bool, logical: bool) -> Result<()> {
        let (migration, column_map) = self.run_schema_migration(sql)?;
        self.run_replay_setup(&migration, &column_map)?;
        let orchestrator = MigrationOrchestrator::new(migration.clone(), self.pool.clone());
        match self.build_replay(&migration, &column_map, logical) {
            ReplayKind::Logical(replay) => {
                orchestrator.orchestrate(execute, column_map, replay)?;
            }
            ReplayKind::Log(replay) => {
                orchestrator.orchestrate(execute, column_map, replay)?;
            }
        }
        Ok(())
    }

    pub fn run_replay_only(
        &self,
        sql: &str,
        logical: bool,
        stop_replay: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let (migration, column_map) = self.run_schema_migration(sql).expect("Migration failed");
        if !logical {
            self.run_replay_setup(&migration, &column_map)
                .expect("Replay setup failed");
        }
        let pool = self.pool.clone();
        std::thread::spawn(move || {
            let mut client = pool.get().expect("Failed to get client");
            let replay_kind =
                Self { pool: pool.clone() }.build_replay(&migration, &column_map, logical);
            match replay_kind {
                ReplayKind::Logical(replay) => {
                    while !stop_replay.load(std::sync::atomic::Ordering::Relaxed) {
                        let _ = replay.replay_log(&mut client);
                        std::thread::sleep(std::time::Duration::from_millis(200));
                    }
                }
                ReplayKind::Log(replay) => {
                    while !stop_replay.load(std::sync::atomic::Ordering::Relaxed) {
                        let _ = replay.replay_log(&mut client);
                        std::thread::sleep(std::time::Duration::from_millis(200));
                    }
                }
            }
            Ok(())
        })
    }

    pub fn run_backfill(&self, migration: &Migration) -> Result<()> {
        let mut client = self.pool.get()?;
        let column_map = ColumnMap::new(&migration.table, &migration.shadow_table, &mut *client);
        let backfill = crate::backfill::BatchedBackfill { batch_size: 1000 };
        backfill.backfill(
            &migration.table,
            &migration.shadow_table,
            &column_map,
            &mut client,
        )?;
        Ok(())
    }

    pub fn run_replay(&self, migration: &Migration, column_map: &ColumnMap) -> Result<()> {
        let mut client = self.pool.get()?;
        match self.build_replay(migration, column_map, false) {
            ReplayKind::Log(replay) => {
                replay.setup(&mut client)?;
                replay.replay_log(&mut client)?;
            }
            ReplayKind::Logical(_) => {
                // Not supported in this helper
                anyhow::bail!("Logical replay not supported in run_replay");
            }
        }
        Ok(())
    }

    pub fn run_replay_setup(&self, migration: &Migration, column_map: &ColumnMap) -> Result<()> {
        let mut client = self.pool.get()?;
        let replay = crate::log_table_replay::LogTableReplay {
            log_table: migration.log_table.clone(),
            shadow_table: migration.shadow_table.clone(),
            table: migration.table.clone(),
            column_map: column_map.clone(),
            primary_key: migration.primary_key.clone(),
        };
        replay.setup(&mut client)?;
        Ok(())
    }

    pub fn build_replay(
        &self,
        migration: &Migration,
        column_map: &ColumnMap,
        logical: bool,
    ) -> ReplayKind {
        if logical {
            let slot_name = format!("ost_slot_{}", uuid::Uuid::new_v4().simple());
            let pub_name = format!("ost_pub_{}", uuid::Uuid::new_v4().simple());
            let slot = Slot::new(slot_name);
            let publication = Publication::new(pub_name, migration.table.clone(), slot.clone());
            ReplayKind::Logical(LogicalReplay {
                slot,
                publication,
                table: migration.table.clone(),
                shadow_table: migration.shadow_table.clone(),
                column_map: column_map.clone(),
                primary_key: migration.primary_key.clone(),
            })
        } else {
            ReplayKind::Log(LogTableReplay {
                log_table: migration.log_table.clone(),
                shadow_table: migration.shadow_table.clone(),
                table: migration.table.clone(),
                column_map: column_map.clone(),
                primary_key: migration.primary_key.clone(),
            })
        }
    }

    pub fn build_and_setup_replay(
        &self,
        migration: &Migration,
        column_map: &ColumnMap,
        logical: bool,
    ) -> Result<ReplayKind> {
        let mut client = self.pool.get()?;
        let replay_kind = if logical {
            let logical_replay = self.build_logical_replay(migration, column_map);
            logical_replay.setup(&mut client)?;
            ReplayKind::Logical(logical_replay)
        } else {
            let log_table_replay = self.build_log_table_replay(migration, column_map);
            log_table_replay.setup(&mut client)?;
            ReplayKind::Log(log_table_replay)
        };
        Ok(replay_kind)
    }

    fn build_logical_replay(&self, migration: &Migration, column_map: &ColumnMap) -> LogicalReplay {
        let slot_name = format!("ost_slot_{}", uuid::Uuid::new_v4().simple());
        let pub_name = format!("ost_pub_{}", uuid::Uuid::new_v4().simple());
        let slot = Slot::new(slot_name);
        let publication = Publication::new(pub_name, migration.table.clone(), slot.clone());
        LogicalReplay {
            slot,
            publication,
            table: migration.table.clone(),
            shadow_table: migration.shadow_table.clone(),
            column_map: column_map.clone(),
            primary_key: migration.primary_key.clone(),
        }
    }

    fn build_log_table_replay(
        &self,
        migration: &Migration,
        column_map: &ColumnMap,
    ) -> LogTableReplay {
        LogTableReplay {
            log_table: migration.log_table.clone(),
            shadow_table: migration.shadow_table.clone(),
            table: migration.table.clone(),
            column_map: column_map.clone(),
            primary_key: migration.primary_key.clone(),
        }
    }
}
