//! Main binary entry point for postgres-ost.

use anyhow::Result;
use postgres_ost::args::Strategy;
use postgres_ost::args::{Command, get_args};
use postgres_ost::migration_runner::{MigrationRunner, ReplayMode};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

fn strategy_to_replay_mode(strategy: Strategy) -> ReplayMode {
    match strategy {
        Strategy::Triggers => ReplayMode::Log,
        Strategy::Logical => ReplayMode::Logical, // You can add StreamingLogical if you add a CLI option
    }
}

fn main() -> Result<()> {
    let args = get_args()?;
    match args.command {
        Command::Migrate {
            uri,
            sql,
            execute,
            strategy,
            ..
        } => {
            let runner = MigrationRunner::new(&uri)?;
            let replay_mode = strategy_to_replay_mode(strategy);
            runner.run_migrate(&sql, execute, replay_mode)?;
        }
        Command::ReplayOnly {
            uri, sql, strategy, ..
        } => {
            let runner = MigrationRunner::new(&uri)?;
            let stop_replay = Arc::new(AtomicBool::new(false));
            let stop_replay_clone = stop_replay.clone();
            ctrlc::set_handler(move || {
                stop_replay_clone.store(true, Ordering::Relaxed);
            })?;
            let replay_mode = strategy_to_replay_mode(strategy);
            let handle = runner.run_replay_only(&sql, replay_mode, stop_replay);
            handle.join().expect("Replay thread panicked")?;
        }
    }
    Ok(())
}
