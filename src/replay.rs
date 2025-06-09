pub use crate::log_table_replay::{LogTableReplay, PrimaryKey};
pub use crate::logical_replay::{LogicalReplay, wal2json2sql};

#[derive(Clone)]
pub enum ReplayImpl {
    LogTable(LogTableReplay),
    Logical(LogicalReplay),
}

impl ReplayImpl {
    pub fn replay_log(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        match self {
            ReplayImpl::LogTable(r) => r.replay_log(client),
            ReplayImpl::Logical(r) => r.replay_log(client),
        }
    }
    pub fn setup(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        match self {
            ReplayImpl::LogTable(r) => r.setup(client),
            ReplayImpl::Logical(r) => r.setup(client),
        }
    }
    pub fn teardown<C: postgres::GenericClient>(&self, client: &mut C) -> anyhow::Result<()> {
        match self {
            ReplayImpl::LogTable(r) => r.teardown(client),
            ReplayImpl::Logical(r) => r.teardown(client),
        }
    }
    pub fn replay_log_until_complete<C: postgres::GenericClient>(
        &self,
        client: &mut C,
    ) -> anyhow::Result<()> {
        match self {
            ReplayImpl::LogTable(r) => r.replay_log_until_complete(client),
            ReplayImpl::Logical(r) => r.replay_log_until_complete(client),
        }
    }
}
