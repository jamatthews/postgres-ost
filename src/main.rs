//! Main binary entry point for postgres-ost.

use anyhow::Result;
use postgres_ost::Migration;
use postgres_ost::args::{Command, get_args};
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
            let replay = postgres_ost::LogTableReplay {
                log_table: migration.log_table.clone(),
                shadow_table: migration.shadow_table.clone(),
                table: migration.table.clone(),
                column_map: column_map.clone(),
                primary_key: migration.primary_key.clone(),
            };
            let orchestrator = postgres_ost::MigrationOrchestrator::new(migration, pool);
            orchestrator.orchestrate(execute, column_map, replay)?;
        }
        Command::ReplayOnly {
            uri,
            sql,
            strategy: _,
        } => {
            let manager = PostgresConnectionManager::new(uri.parse()?, R2d2NoTls);
            let pool = Pool::new(manager)?;
            let stop_replay = Arc::new(AtomicBool::new(false));
            let stop_replay_clone = stop_replay.clone();
            ctrlc::set_handler(move || {
                stop_replay_clone.store(true, Ordering::Relaxed);
            })?;
            postgres_ost::run_replay_only(&pool, &sql, stop_replay)?;
        }
    }
    Ok(())
}
