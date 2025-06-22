// Standard library imports
use std::sync::{Arc, atomic::AtomicBool};

// External crate imports
use anyhow::Result;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};

// Internal module imports
use crate::Replay;
use crate::backfill::Backfill;
use crate::column_map::ColumnMap;
use crate::logical_replication::{Publication, Slot};
use crate::migration::Migration;
use crate::orchestrator::MigrationOrchestrator;
use crate::replay::log_table_replay::LogTableReplay;
use crate::replay::logical_replay::LogicalReplay;
use crate::replay::streaming_logical_replay::StreamingLogicalReplay;

pub struct MigrationRunner {
    pub pool: Pool<PostgresConnectionManager<R2d2NoTls>>,
    pub conninfo: String,
}

pub enum ReplayMode {
    Log,
    Logical,
    StreamingLogical,
}

pub enum ReplayKind {
    Log(LogTableReplay),
    Logical(LogicalReplay),
    StreamingLogical(StreamingLogicalReplay),
}

impl MigrationRunner {
    pub fn new(uri: &str) -> Result<Self> {
        let manager = PostgresConnectionManager::new(uri.parse()?, R2d2NoTls);
        let pool = Pool::new(manager)?;
        // Detect and set Postgres version globally
        {
            let mut client = pool.get()?;
            crate::version::detect_and_set_pg_version(&mut client)?;
        }
        Ok(Self {
            pool,
            conninfo: uri.to_string(),
        })
    }

    pub fn from_pool(pool: Pool<PostgresConnectionManager<R2d2NoTls>>, conninfo: String) -> Self {
        Self { pool, conninfo }
    }

    pub fn run_schema_migration(&self, sql: &str) -> Result<(Migration, ColumnMap)> {
        let mut client = self.pool.get()?;
        let migration = Migration::new(sql, &mut client);
        migration.setup_migration(&mut client)?;
        let column_map = ColumnMap::new(&migration.table, &migration.shadow_table, &mut *client);
        Ok((migration, column_map))
    }

    pub fn run_migrate(&self, sql: &str, execute: bool, mode: ReplayMode) -> Result<()> {
        let (migration, column_map) = self.run_schema_migration(sql)?;
        self.run_replay_setup(&migration, &column_map)?;
        let orchestrator = MigrationOrchestrator::new(migration.clone(), self.pool.clone());
        match self.build_replay(&migration, &column_map, mode) {
            ReplayKind::Logical(replay) => {
                orchestrator.orchestrate(execute, column_map, replay)?;
            }
            ReplayKind::Log(replay) => {
                orchestrator.orchestrate(execute, column_map, replay)?;
            }
            ReplayKind::StreamingLogical(_) => {
                panic!(
                    "StreamingLogicalReplay is not supported in orchestrator context. Use single-threaded context only."
                );
            }
        }
        Ok(())
    }

    pub fn run_replay_only(
        &self,
        sql: &str,
        mode: ReplayMode,
        stop_replay: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        let (migration, column_map) = self.run_schema_migration(sql).expect("Migration failed");
        if let ReplayMode::Log = mode {
            self.run_replay_setup(&migration, &column_map)
                .expect("Replay setup failed");
        }
        let conninfo = self.conninfo.clone();
        let pool = self.pool.clone();
        std::thread::spawn(move || {
            let mut client = pool.get().expect("Failed to get client");
            let replay_kind = Self {
                pool: pool.clone(),
                conninfo: conninfo.clone(),
            }
            .build_replay(&migration, &column_map, mode);
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
                ReplayKind::StreamingLogical(_) => {
                    panic!(
                        "StreamingLogicalReplay is not supported in threaded replay context. Use single-threaded context only."
                    );
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
        match self.build_replay(migration, column_map, ReplayMode::Log) {
            ReplayKind::Log(replay) => {
                replay.setup(&mut client)?;
                replay.replay_log(&mut client)?;
            }
            ReplayKind::Logical(_) => {
                // Not supported in this helper
                anyhow::bail!("Logical replay not supported in run_replay");
            }
            ReplayKind::StreamingLogical(_) => {
                // Not supported in this helper
                anyhow::bail!("Streaming logical replay not supported in run_replay");
            }
        }
        Ok(())
    }

    pub fn run_replay_setup(&self, migration: &Migration, column_map: &ColumnMap) -> Result<()> {
        let mut client = self.pool.get()?;
        let replay = LogTableReplay {
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
        mode: ReplayMode,
    ) -> ReplayKind {
        match mode {
            ReplayMode::Logical => {
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
            }
            ReplayMode::StreamingLogical => {
                let slot_name = format!("ost_slot_{}", uuid::Uuid::new_v4().simple());
                let pub_name = format!("ost_pub_{}", uuid::Uuid::new_v4().simple());
                let slot = Slot::new(slot_name.clone());
                let publication = Publication::new(pub_name, migration.table.clone(), slot.clone());
                let start_lsn = crate::logical_replication::message::Lsn(0); // Start from 0 or use a real value
                let stream = crate::logical_replication::LogicalReplicationStream::new(
                    &self.conninfo,
                    &slot_name,
                    start_lsn,
                )
                .expect("Failed to create LogicalReplicationStream");
                ReplayKind::StreamingLogical(StreamingLogicalReplay {
                    stream: std::cell::RefCell::new(stream),
                    slot,
                    publication,
                    table: migration.table.clone(),
                    shadow_table: migration.shadow_table.clone(),
                    column_map: column_map.clone(),
                    primary_key: migration.primary_key.clone(),
                })
            }
            ReplayMode::Log => ReplayKind::Log(LogTableReplay {
                log_table: migration.log_table.clone(),
                shadow_table: migration.shadow_table.clone(),
                table: migration.table.clone(),
                column_map: column_map.clone(),
                primary_key: migration.primary_key.clone(),
            }),
        }
    }

    pub fn build_and_setup_replay(
        &self,
        migration: &Migration,
        column_map: &ColumnMap,
        mode: ReplayMode,
    ) -> Result<ReplayKind> {
        let mut client = self.pool.get()?;
        let replay_kind = match mode {
            ReplayMode::Logical => {
                let logical_replay = self.build_logical_replay(migration, column_map);
                logical_replay.setup(&mut client)?;
                ReplayKind::Logical(logical_replay)
            }
            ReplayMode::StreamingLogical => {
                let streaming_replay = self.build_streaming_logical_replay(migration, column_map);
                streaming_replay.setup(&mut client)?;
                ReplayKind::StreamingLogical(streaming_replay)
            }
            ReplayMode::Log => {
                let log_table_replay = self.build_log_table_replay(migration, column_map);
                log_table_replay.setup(&mut client)?;
                ReplayKind::Log(log_table_replay)
            }
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

    fn build_streaming_logical_replay(
        &self,
        migration: &Migration,
        column_map: &ColumnMap,
    ) -> StreamingLogicalReplay {
        let slot_name = format!("ost_slot_{}", uuid::Uuid::new_v4().simple());
        let pub_name = format!("ost_pub_{}", uuid::Uuid::new_v4().simple());
        let slot = Slot::new(slot_name.clone());
        let publication = Publication::new(pub_name, migration.table.clone(), slot.clone());
        let start_lsn = crate::logical_replication::message::Lsn(0); // Start from 0 or use a real value
        let stream = crate::logical_replication::LogicalReplicationStream::new(
            &self.conninfo,
            &slot_name,
            start_lsn,
        )
        .expect("Failed to create LogicalReplicationStream");
        StreamingLogicalReplay {
            stream: std::cell::RefCell::new(stream),
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
