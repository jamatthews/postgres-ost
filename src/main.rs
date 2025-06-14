//! Main binary entry point for postgres-ost.

use anyhow::Result;
use postgres_ost::Migration;
use postgres_ost::args::{Command, get_args};
use postgres_ost::Replay;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn main() -> Result<()> {
    let args = get_args()?;
    match args.command {
        Command::Migrate {
            uri,
            sql,
            execute,
            strategy: _,
            logical,
        } => {
            let manager = PostgresConnectionManager::new(uri.parse()?, R2d2NoTls);
            let pool = Pool::new(manager)?;
            let mut client = pool.get()?;
            let migration = Migration::new(&sql, &mut client);
            migration.setup_migration(&mut client)?;
            let column_map = postgres_ost::ColumnMap::new(
                &migration.table,
                &migration.shadow_table,
                &mut *client,
            );
            let orchestrator = postgres_ost::MigrationOrchestrator::new(migration.clone(), pool);
            if logical {
                // Construct slot and publication names
                let slot_name = format!("ost_slot_{}", uuid::Uuid::new_v4().simple());
                let pub_name = format!("ost_pub_{}", uuid::Uuid::new_v4().simple());
                let slot = postgres_ost::logical_replication::Slot::new(slot_name);
                let publication = postgres_ost::logical_replication::Publication::new(
                    pub_name,
                    migration.table.clone(),
                    slot.clone(),
                );
                let replay = postgres_ost::LogicalReplay {
                    slot,
                    publication,
                    table: migration.table.clone(),
                    shadow_table: migration.shadow_table.clone(),
                    column_map: column_map.clone(),
                    primary_key: migration.primary_key.clone(),
                };
                orchestrator.orchestrate(execute, column_map, replay)?;
            } else {
                let replay = postgres_ost::LogTableReplay {
                    log_table: migration.log_table.clone(),
                    shadow_table: migration.shadow_table.clone(),
                    table: migration.table.clone(),
                    column_map: column_map.clone(),
                    primary_key: migration.primary_key.clone(),
                };
                orchestrator.orchestrate(execute, column_map, replay)?;
            }
        }
        Command::ReplayOnly {
            uri,
            sql,
            strategy: _,
            logical,
        } => {
            let manager = PostgresConnectionManager::new(uri.parse()?, R2d2NoTls);
            let pool = Pool::new(manager)?;
            let stop_replay = Arc::new(AtomicBool::new(false));
            let stop_replay_clone = stop_replay.clone();
            ctrlc::set_handler(move || {
                stop_replay_clone.store(true, Ordering::Relaxed);
            })?;
            let mut client = pool.get()?;
            let migration = Migration::new(&sql, &mut client);
            migration.setup_migration(&mut client)?;
            let column_map = postgres_ost::ColumnMap::new(
                &migration.table,
                &migration.shadow_table,
                &mut *client,
            );
            if logical {
                let slot_name = format!("ost_slot_{}", uuid::Uuid::new_v4().simple());
                let pub_name = format!("ost_pub_{}", uuid::Uuid::new_v4().simple());
                let slot = postgres_ost::logical_replication::Slot::new(slot_name);
                let publication = postgres_ost::logical_replication::Publication::new(
                    pub_name,
                    migration.table.clone(),
                    slot.clone(),
                );
                let replay = postgres_ost::LogicalReplay {
                    slot,
                    publication,
                    table: migration.table.clone(),
                    shadow_table: migration.shadow_table.clone(),
                    column_map: column_map.clone(),
                    primary_key: migration.primary_key.clone(),
                };
                // Run replay in a loop until stop_replay is set
                let replay_handle = std::thread::spawn(move || {
                    while !stop_replay.load(Ordering::Relaxed) {
                        let _ = replay.replay_log(&mut client);
                        std::thread::sleep(std::time::Duration::from_millis(200));
                    }
                });
                replay_handle.join().expect("Replay thread panicked");
            } else {
                let replay = postgres_ost::LogTableReplay {
                    log_table: migration.log_table.clone(),
                    shadow_table: migration.shadow_table.clone(),
                    table: migration.table.clone(),
                    column_map: column_map.clone(),
                    primary_key: migration.primary_key.clone(),
                };
                let replay_handle = std::thread::spawn(move || {
                    while !stop_replay.load(Ordering::Relaxed) {
                        let _ = replay.replay_log(&mut client);
                        std::thread::sleep(std::time::Duration::from_millis(200));
                    }
                });
                replay_handle.join().expect("Replay thread panicked");
            }
        }
    }
    Ok(())
}
