//! Main library entry point for postgres-ost.

pub mod args;
pub mod backfill;
pub mod column_map;
pub mod logical_replication;
pub mod migration;
pub mod migration_runner;
mod orchestrator;
pub mod parse;
pub mod pg_query_parser;
pub mod replay;
pub mod table;
pub mod version;

// Re-export key types for ergonomic access

pub use crate::column_map::ColumnMap;
pub use crate::replay::Replay;
pub use crate::replay::log_table_replay::LogTableReplay;
pub use crate::replay::log_table_replay::PrimaryKey;
pub use crate::replay::logical_replay::LogicalReplay;
pub use crate::replay::logical_replay::wal2json2sql;
pub use crate::replay::streaming_logical_replay::StreamingLogicalReplay;
pub use crate::table::Table;
pub use backfill::*;
pub use migration::*;
pub use orchestrator::MigrationOrchestrator;
pub use parse::*;

/// Result type for the library
pub type Result<T = ()> = anyhow::Result<T, anyhow::Error>;
