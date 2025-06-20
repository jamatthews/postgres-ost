//! Main binary entry point for postgres-ost.

use anyhow::Result;
use postgres_ost::args::{Command, get_args};
use postgres_ost::migration_runner::MigrationRunner;
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
            let runner = MigrationRunner::new(&uri)?;
            runner.run_migrate(&sql, execute, logical)?;
        }
        Command::ReplayOnly {
            uri,
            sql,
            strategy: _,
            logical,
        } => {
            let runner = MigrationRunner::new(&uri)?;
            let stop_replay = Arc::new(AtomicBool::new(false));
            let stop_replay_clone = stop_replay.clone();
            ctrlc::set_handler(move || {
                stop_replay_clone.store(true, Ordering::Relaxed);
            })?;
            let handle = runner.run_replay_only(&sql, logical, stop_replay);
            handle.join().expect("Replay thread panicked")?;
        }
    }
    Ok(())
}
