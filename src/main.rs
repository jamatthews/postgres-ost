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
            let mut migration = Migration::new(&sql, &mut client);
            migration.orchestrate(&pool, execute)?;
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
