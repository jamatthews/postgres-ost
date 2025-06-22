// streaming_logical_replay.rs
// Implements StreamingLogicalReplay using LogicalReplicationStream.

use crate::logical_replication::LogicalReplicationStream;
use crate::{ColumnMap, PrimaryKeyInfo, Replay, Table};
use std::cell::RefCell;

pub struct StreamingLogicalReplay {
    pub stream: RefCell<LogicalReplicationStream>,
    pub slot: crate::logical_replication::Slot,
    pub publication: crate::logical_replication::Publication,
    pub table: Table,
    pub shadow_table: Table,
    pub column_map: ColumnMap,
    pub primary_key: PrimaryKeyInfo,
}

impl Replay for StreamingLogicalReplay {
    fn setup(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        // Create publication if needed
        self.publication.create(client)?;
        // Create slot if needed
        self.slot.create_slot(client)?;
        // Start the logical replication stream
        self.stream.borrow_mut().start()?;
        Ok(())
    }

    fn teardown(&self, _transaction: &mut postgres::Transaction) -> anyhow::Result<()> {
        // TODO: implement teardown logic
        Ok(())
    }

    fn replay_log(&self, _client: &mut postgres::Client) -> anyhow::Result<()> {
        let mut stream = self.stream.borrow_mut();
        // Fetch a batch of messages (e.g., up to 100, with a short timeout)
        let _messages = stream.next_batch(100, Some(std::time::Duration::from_millis(500)))?;
        // Advance the slot's confirmed_flush_lsn to the stream's last_lsn
        let lsn = stream.last_lsn();
        // Send feedback to Postgres to advance the slot
        stream.send_feedback(lsn)?;
        Ok(())
    }

    fn replay_log_until_complete(
        &self,
        _transaction: &mut postgres::Transaction,
    ) -> anyhow::Result<()> {
        // TODO: implement streaming replay until complete
        Ok(())
    }
}
