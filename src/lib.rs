//! Main library entry point for postgres-ost.

pub mod column_map;
pub mod migration;
pub mod backfill;
pub mod replay;
pub mod args;

// Re-export key types for ergonomic access
pub use column_map::ColumnMap;
pub use migration::Migration;
pub use backfill::{Backfill, BatchedBackfill};
pub use replay::{LogTableReplay, Replay};
