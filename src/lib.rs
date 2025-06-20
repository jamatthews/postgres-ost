//! Main library entry point for postgres-ost.

pub mod args;
pub mod backfill;
pub mod column_map;
pub mod log_table_replay;
pub mod logical_replay;
pub mod logical_replication;
pub mod migration;
pub mod migration_runner;
mod orchestrator;
pub mod parse;
pub mod pg_query_parser;
pub mod replay;
pub mod table;

// Re-export key types for ergonomic access

pub use crate::column_map::ColumnMap;
pub use crate::log_table_replay::LogTableReplay;
pub use crate::log_table_replay::PrimaryKey;
pub use crate::logical_replay::LogicalReplay;
pub use crate::logical_replay::wal2json2sql;
pub use crate::migration::PrimaryKeyInfo;
pub use crate::replay::Replay;
pub use crate::table::Table;
pub use backfill::*;
pub use migration::*;
pub use orchestrator::MigrationOrchestrator;
pub use parse::*;

/// Result type for the library
pub type Result<T = ()> = anyhow::Result<T, anyhow::Error>;
