// streaming_logical_replay.rs
// Implements StreamingLogicalReplay using LogicalReplicationStream.

use crate::logical_replication::LogicalReplicationStream;
use crate::replay::logical_replay;
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

    fn replay_log(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        let mut stream = self.stream.borrow_mut();
        let messages = stream.next_batch(100, Some(std::time::Duration::from_millis(500)))?;

        // Collect wal2json JSON values from XLogData messages
        let mut batch = Vec::new();
        for msg in &messages {
            if let crate::logical_replication::message::ReplicationMessage::XLogData(xlog) = msg {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&xlog.data) {
                    batch.push(json);
                }
            }
        }

        // Generate SQL statements and execute them
        let statements = logical_replay::wal2json2sql(
            &batch,
            &self.column_map,
            &self.table,
            &self.shadow_table,
            &self.primary_key,
        );
        for stmt in statements {
            client.batch_execute(&stmt)?;
        }

        // Advance the slot's confirmed_flush_lsn to the stream's last_lsn
        let lsn = stream.last_lsn();
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
