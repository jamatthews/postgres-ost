pub use crate::{LogTableReplay, LogicalReplay, PrimaryKey, wal2json2sql};

pub trait Replay {
    fn replay_log(&self, client: &mut postgres::Client) -> anyhow::Result<()>;
    fn setup(&self, client: &mut postgres::Client) -> anyhow::Result<()>;
    fn teardown(&self, transaction: &mut postgres::Transaction) -> anyhow::Result<()>;
    fn replay_log_until_complete(
        &self,
        transaction: &mut postgres::Transaction,
    ) -> anyhow::Result<()>;
}
