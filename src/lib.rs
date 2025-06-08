//! Main library entry point for postgres-ost.

pub mod args;
pub mod backfill;
pub mod column_map;
pub mod migration;
pub mod parse;
pub mod replay;
pub mod pg_query_parser;

// Re-export key types for ergonomic access

pub use backfill::*;
pub use column_map::*;
pub use migration::*;
pub use parse::*;
pub use replay::*;

use anyhow::Result;
use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
use std::sync::{Arc, atomic::AtomicBool};

/// Run replay-only mode: sets up migration and starts log replay until stop_replay is set.
pub fn run_replay_only(
    pool: &Pool<PostgresConnectionManager<R2d2NoTls>>,
    sql: &str,
    stop_replay: Arc<AtomicBool>,
) -> Result<()> {
    let mut client = pool.get()?;
    let mut migration = crate::migration::Migration::new(sql, &mut client);
    migration.setup_migration(pool)?;
    let replay_handle = migration.start_log_replay_thread(pool, stop_replay.clone());
    replay_handle.join().expect("Replay thread panicked");
    Ok(())
}
