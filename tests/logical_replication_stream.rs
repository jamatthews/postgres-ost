mod common;

use postgres_ost::logical_replay::emit_replay_complete_message;
use postgres_ost::logical_replication::LogicalReplicationStream;
use postgres_ost::logical_replication::ReplicationMessage;
use postgres_ost::logical_replication::Slot;

// Integration test for using libpq to access logical replication XLogData
// Requires: a running Postgres instance, logical replication enabled, and a logical slot created
// Adjust connection string and slot name as needed

fn setup_slot_and_stream() -> (common::TestDb, LogicalReplicationStream) {
    let test_db = common::setup_test_db();
    let dbname = &test_db.dbname;
    let mut pooled = test_db.pool.get().unwrap();
    let client: &mut postgres::Client = &mut pooled;
    let slot = Slot {
        name: dbname.to_string(),
        plugin: "test_decoding".to_string(),
    };
    let _ = slot.drop_slot(client);
    slot.create_slot(client).unwrap();

    let conninfo = format!(
        "host=localhost user=post_test dbname={} password=postgres replication=database",
        dbname
    );
    let stream = LogicalReplicationStream::new(
        &conninfo,
        &slot.name,
        postgres_ost::logical_replication::message::Lsn(0),
    )
    .expect("stream new");
    (test_db, stream)
}

/// Polls the replication stream for up to `max_attempts` batches, returning true if any XLogData contains all `needles`.
fn wait_for_xlog_data_containing(
    stream: &mut LogicalReplicationStream,
    batch_size: usize,
    max_attempts: usize,
    needles: &[&str],
) -> bool {
    for _ in 0..max_attempts {
        let messages =
            match stream.next_batch(batch_size, Some(std::time::Duration::from_millis(20))) {
                Ok(msgs) => msgs,
                Err(_) => continue,
            };
        for rep_msg in messages {
            if let ReplicationMessage::XLogData(xlog) = rep_msg {
                let body_str = String::from_utf8_lossy(&xlog.data);
                if needles.iter().all(|needle| body_str.contains(needle)) {
                    return true;
                }
            }
        }
    }
    false
}

#[test]
fn test_libpq_logical_replication_xlogdata() {
    let (test_db, mut stream) = setup_slot_and_stream();
    let mut client = test_db.get_client();

    if let Err(e) = stream.start() {
        eprintln!("Failed to start replication: {e:?}");
        panic!("stream start: {e:?}");
    }

    // Insert a row into test_table to generate XLogData
    client
        .simple_query("INSERT INTO test_table (assertable, target) VALUES ('foo', 'bar')")
        .unwrap();

    // Use helper to check for test_decoding output
    let found = wait_for_xlog_data_containing(&mut stream, 5, 50, &["foo", "bar"]);
    assert!(
        found,
        "Did not find expected test_decoding output in XLogData"
    );
}

#[test]
fn test_send_feedback() {
    let (test_db, mut stream) = setup_slot_and_stream();
    let mut client = test_db.get_client();
    stream.start().expect("stream start");

    client
        .simple_query("INSERT INTO test_table (assertable, target) VALUES ('foo', 'bar')")
        .unwrap();

    let messages = stream
        .next_batch(20, Some(std::time::Duration::from_secs(2)))
        .expect("next_batch");
    let mut feedback_lsn = None;
    for rep_msg in messages {
        if let ReplicationMessage::XLogData(xlog) = rep_msg {
            // Send feedback for the received LSN, requesting a reply
            stream.send_feedback(xlog.wal_end).expect("send_feedback");
            stream.conn.flush().expect("flush after feedback");
            feedback_lsn = Some(xlog.wal_end);
            break;
        }
    }
    assert!(
        feedback_lsn.is_some(),
        "Did not find XLogData to send feedback for"
    );

    // After sending feedback, expect a PrimaryKeepAlive with reply_requested = true and wal_end >= feedback_lsn
    use std::{thread, time::Duration, time::Instant};
    let start = Instant::now();
    let mut got_reply = false;
    if let Some(lsn) = feedback_lsn {
        for _ in 0..50 {
            stream.conn.consume_input().expect("consume_input");
            let responses = stream
                .next_batch(1, Some(Duration::from_millis(10)))
                .expect("next_batch after feedback");
            for rep_msg in &responses {
                if let ReplicationMessage::PrimaryKeepAlive(pk) = rep_msg {
                    if pk.reply_requested {
                        assert!(
                            pk.wal_end >= lsn,
                            "PrimaryKeepAlive.wal_end ({:?}) < feedback_lsn ({:?})",
                            pk.wal_end,
                            lsn
                        );
                        got_reply = true;
                        println!("Got reply after {:?}", start.elapsed());
                        break;
                    }
                }
            }
            if got_reply {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
    assert!(
        got_reply,
        "Did not receive PrimaryKeepAlive with reply_requested = true after feedback"
    );
}

#[test]
fn test_emit_replay_complete_message() {
    let (test_db, mut stream) = setup_slot_and_stream();
    let mut client = test_db.get_client();
    stream.start().expect("stream start");

    // Insert a row to generate WAL
    client
        .simple_query("INSERT INTO test_table (assertable, target) VALUES ('emit', 'msg')")
        .unwrap();

    // Emit the replay complete message
    emit_replay_complete_message(&mut client).expect("emit_replay_complete_message");

    // Use helper to look for the replay complete marker
    let found_marker = wait_for_xlog_data_containing(&mut stream, 10, 50, &["replay complete"]);
    assert!(
        found_marker,
        "Did not find replay complete marker in WAL stream"
    );
}
