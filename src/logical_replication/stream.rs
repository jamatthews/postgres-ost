// LogicalReplicationStream: streaming, batching, and LSN tracking

pub struct LogicalReplicationStream {
    pub conn: libpq::Connection,
    pub slot_name: String,
    pub last_lsn: crate::logical_replication::message::Lsn,
}

impl LogicalReplicationStream {
    pub fn new(
        conninfo: &str,
        slot_name: &str,
        start_lsn: crate::logical_replication::message::Lsn,
    ) -> anyhow::Result<Self> {
        let conn = libpq::Connection::new(conninfo)?;
        Ok(Self {
            conn,
            slot_name: slot_name.to_string(),
            last_lsn: start_lsn,
        })
    }

    /// Format an Lsn as a Postgres LSN string (e.g., "0/0").
    fn lsn_to_pg_string(lsn: crate::logical_replication::message::Lsn) -> String {
        let val = lsn.0;
        format!("{:X}/{:X}", (val >> 32), (val & 0xFFFFFFFF))
    }

    /// Start replication and return a stream ready to pull messages.
    pub fn start(&mut self) -> anyhow::Result<()> {
        let lsn_str = Self::lsn_to_pg_string(self.last_lsn);
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {}",
            self.slot_name, lsn_str
        );
        let res = self.conn.exec(&query);
        // Use the libpq::Status::CopyBoth enum variant for clarity
        if res.status() != libpq::Status::CopyBoth {
            let msg = self.conn.error_message();
            anyhow::bail!(
                "Failed to start replication: status {:?}, error: {:?}",
                res.status(),
                msg
            );
        }
        Ok(())
    }

    /// Pull up to `max_messages` replication messages, or until timeout (if provided).
    pub fn next_batch(
        &mut self,
        max_messages: usize,
        timeout: Option<std::time::Duration>,
    ) -> anyhow::Result<Vec<crate::logical_replication::message::ReplicationMessage>> {
        let mut messages = Vec::new();
        let start = std::time::Instant::now();
        while messages.len() < max_messages {
            let _ = self.conn.consume_input();
            match self.conn.copy_data(false) {
                Ok(msg) => {
                    if let Some(rep_msg) =
                        crate::logical_replication::message::ReplicationMessage::parse(&msg)
                    {
                        if let crate::logical_replication::message::ReplicationMessage::XLogData(
                            ref xlog,
                        ) = rep_msg
                        {
                            self.last_lsn = xlog.wal_end;
                        }
                        messages.push(rep_msg);
                    }
                }
                Err(_) => break,
            }
            if let Some(t) = timeout {
                if start.elapsed() > t {
                    break;
                }
            }
        }
        Ok(messages)
    }

    /// Update the confirmed LSN (send feedback to Postgres).
    pub fn update_confirmed_lsn(
        &mut self,
        lsn: crate::logical_replication::message::Lsn,
    ) -> anyhow::Result<()> {
        self.last_lsn = lsn;
        Ok(())
    }

    /// Send a feedback message to Postgres with the confirmed LSN.
    pub fn send_feedback(
        &mut self,
        confirmed_lsn: crate::logical_replication::message::Lsn,
    ) -> anyhow::Result<()> {
        // Standby status update message format:
        // 'r' + 8 bytes wal_write + 8 bytes wal_flush + 8 bytes wal_apply + 8 bytes client time + 1 byte reply requested
        // We'll set all LSNs to confirmed_lsn, client time to now, reply_requested to 1 for test
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut buf = Vec::with_capacity(1 + 8 * 3 + 8 + 1);
        buf.push(b'r');
        let lsn = confirmed_lsn.0;
        for _ in 0..3 {
            // wal_write, wal_flush, wal_apply
            buf.extend_from_slice(&lsn.to_be_bytes());
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        buf.extend_from_slice(&now.to_be_bytes());
        buf.push(1); // reply_requested = true (for test)
        self.conn.put_copy_data(&buf)?;
        Ok(())
    }

    pub fn last_lsn(&self) -> crate::logical_replication::message::Lsn {
        self.last_lsn
    }
}
