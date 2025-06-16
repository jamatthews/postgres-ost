// Replication protocol messages for logical replication

#[derive(Debug, PartialEq)]
pub enum ReplicationMessage {
    XLogData(XLogData),
    PrimaryKeepAlive(PrimaryKeepAlive),
    Unknown(u8, Vec<u8>),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Lsn(pub u64);

impl Lsn {
    pub fn from_u64(val: u64) -> Self {
        Lsn(val)
    }
    /// Parse a Postgres LSN string (e.g., "0/16B6C50") into an Lsn
    pub fn from_pg_string(s: &str) -> Option<Self> {
        let mut parts = s.split('/');
        let hi = u64::from_str_radix(parts.next()?, 16).ok()?;
        let lo = u64::from_str_radix(parts.next()?, 16).ok()?;
        Some(Lsn((hi << 32) | lo))
    }
    /// Convert this Lsn to a Postgres LSN string (e.g., "0/16B6C50")
    pub fn to_pg_string(&self) -> String {
        format!("{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFFFFFF)
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_pg_string())
    }
}

impl From<u64> for Lsn {
    fn from(val: u64) -> Self {
        Lsn(val)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}

impl PartialOrd for Lsn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Lsn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Debug, PartialEq)]
pub struct XLogData {
    pub wal_start: Lsn,
    pub wal_end: Lsn,
    pub timestamp: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub struct PrimaryKeepAlive {
    pub wal_end: Lsn,
    pub timestamp: u64,
    pub reply_requested: bool,
}

impl ReplicationMessage {
    /// Parse a replication message from a raw byte buffer.
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.is_empty() {
            return None;
        }
        match buf[0] {
            b'w' => XLogData::parse(&buf[1..]).map(ReplicationMessage::XLogData),
            b'k' => PrimaryKeepAlive::parse(&buf[1..]).map(ReplicationMessage::PrimaryKeepAlive),
            other => Some(ReplicationMessage::Unknown(other, buf[1..].to_vec())),
        }
    }
}

impl XLogData {
    /// Parse an XLogData message from the buffer (excluding the leading 'w' byte).
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < 24 {
            return None;
        }
        let wal_start = Lsn::from_u64(u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]));
        let wal_end = Lsn::from_u64(u64::from_be_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]));
        let timestamp = u64::from_be_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        let data = buf[24..].to_vec();
        Some(XLogData {
            wal_start,
            wal_end,
            timestamp,
            data,
        })
    }
}

impl PrimaryKeepAlive {
    /// Parse a PrimaryKeepAlive message from the buffer (excluding the leading 'k' byte).
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < 17 {
            return None;
        }
        let wal_end = Lsn::from_u64(u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]));
        let timestamp = u64::from_be_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let reply_requested = buf[16] != 0;
        Some(PrimaryKeepAlive {
            wal_end,
            timestamp,
            reply_requested,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_xlogdata_lsn() {
        // Example values:
        // wal_start: 0x000000010000000A (LSN 1/0xA)
        // wal_end:   0x000000010000000B (LSN 1/0xB)
        // timestamp: 0x0000018D4FDFB000 (arbitrary)
        // data:      [0xDE, 0xAD, 0xBE, 0xEF]
        let mut buf = vec![b'w'];
        buf.extend_from_slice(&0x000000010000000A_u64.to_be_bytes());
        buf.extend_from_slice(&0x000000010000000B_u64.to_be_bytes());
        buf.extend_from_slice(&0x0000018D4FDFB000_u64.to_be_bytes());
        buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let msg = ReplicationMessage::parse(&buf);
        match msg {
            Some(ReplicationMessage::XLogData(xlog)) => {
                assert_eq!(xlog.wal_start, Lsn(0x000000010000000A));
                assert_eq!(xlog.wal_end, Lsn(0x000000010000000B));
                assert_eq!(xlog.timestamp, 0x0000018D4FDFB000);
                assert_eq!(xlog.data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
            }
            _ => panic!("Failed to parse XLogData message"),
        }
    }
}
