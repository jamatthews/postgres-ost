mod common;

// Integration test for using libpq to access logical replication XLogData
// Requires: a running Postgres instance, logical replication enabled, and a logical slot created
// Adjust connection string and slot name as needed

#[test]
fn test_libpq_logical_replication_xlogdata() {
    use postgres_ost::logical_replication::LogicalReplicationStream;
    use postgres_ost::logical_replication::ReplicationMessage;

    let test_db = common::setup_test_db();
    let dbname = &test_db.dbname;
    let conninfo = format!(
        "host=localhost user=post_test dbname={} password=postgres replication=database",
        dbname
    );

    // Create a unique slot name and ensure it exists
    let slot_name = "libpq_test_slot";
    let mut pooled = test_db.pool.get().unwrap();
    let client: &mut postgres::Client = &mut pooled;
    // Drop in case it exists (ignore error)
    let _ = client.simple_query(&format!("SELECT pg_drop_replication_slot('{}')", slot_name));
    client
        .simple_query(&format!(
            "SELECT * FROM pg_create_logical_replication_slot('{}', 'test_decoding')",
            slot_name
        ))
        .unwrap();

    // Start logical replication stream
    let mut stream = LogicalReplicationStream::new(
        &conninfo,
        slot_name,
        postgres_ost::logical_replication::message::Lsn(0),
    )
    .expect("stream new");
    if let Err(e) = stream.start() {
        eprintln!("Failed to start replication: {e:?}");
        panic!("stream start: {e:?}");
    }

    // Insert a row into test_table to generate XLogData
    client
        .simple_query("INSERT INTO test_table (assertable, target) VALUES ('foo', 'bar')")
        .unwrap();

    // Pull a batch of messages and check for test_decoding output
    let mut found = false;
    for _ in 0..50 {
        let messages = stream
            .next_batch(5, Some(std::time::Duration::from_millis(20)))
            .expect("next_batch");
        for rep_msg in messages {
            match rep_msg {
                ReplicationMessage::XLogData(xlog) => {
                    let body_str = String::from_utf8_lossy(&xlog.data);
                    println!("XLogData body: {}", body_str);
                    if body_str.contains("foo") && body_str.contains("bar") {
                        found = true;
                        break;
                    }
                }
                ReplicationMessage::PrimaryKeepAlive(pk) => {
                    println!(
                        "PrimaryKeepAlive: wal_end={:?} ts={} reply_requested={}",
                        pk.wal_end, pk.timestamp, pk.reply_requested
                    );
                }
                ReplicationMessage::Unknown(typ, rest) => {
                    println!("Unknown replication message type: {} rest: {:?}", typ, rest);
                }
            }
        }
        if found {
            break;
        }
    }
    assert!(
        found,
        "Did not find expected test_decoding output in XLogData"
    );

    // Clean up slot
    let _ = client.simple_query(&format!("SELECT pg_drop_replication_slot('{}')", slot_name));
}

#[test]
fn test_send_feedback() {
    use postgres_ost::logical_replication::LogicalReplicationStream;
    use postgres_ost::logical_replication::ReplicationMessage;

    let test_db = common::setup_test_db();
    let dbname = &test_db.dbname;
    let conninfo = format!(
        "host=localhost user=post_test dbname={} password=postgres replication=database",
        dbname
    );

    let slot_name = "libpq_test_slot_feedback";
    let mut pooled = test_db.pool.get().unwrap();
    let client: &mut postgres::Client = &mut pooled;
    let _ = client.simple_query(&format!("SELECT pg_drop_replication_slot('{}')", slot_name));
    client
        .simple_query(&format!(
            "SELECT * FROM pg_create_logical_replication_slot('{}', 'test_decoding')",
            slot_name
        ))
        .unwrap();

    let mut stream = LogicalReplicationStream::new(
        &conninfo,
        slot_name,
        postgres_ost::logical_replication::message::Lsn(0),
    )
    .expect("stream new");
    stream.start().expect("stream start");

    client
        .simple_query("INSERT INTO test_table (assertable, target) VALUES ('foo', 'bar')")
        .unwrap();

    let messages = stream
        .next_batch(20, Some(std::time::Duration::from_secs(2)))
        .expect("next_batch");
    let mut sent_feedback = false;
    let mut feedback_lsn = None;
    for rep_msg in messages {
        if let ReplicationMessage::XLogData(xlog) = rep_msg {
            // Send feedback for the received LSN, requesting a reply
            stream.send_feedback(xlog.wal_end).expect("send_feedback");
            stream.conn.flush().expect("flush after feedback");
            feedback_lsn = Some(xlog.wal_end);
            sent_feedback = true;
            // Insert another row to generate more WAL and trigger a server message
            client
                .simple_query("INSERT INTO test_table (assertable, target) VALUES ('baz', 'qux')")
                .expect("insert second row");
            break;
        }
    }
    assert!(sent_feedback, "Did not find XLogData to send feedback for");

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

    // Clean up slot
    let _ = client.simple_query(&format!("SELECT pg_drop_replication_slot('{}')", slot_name));
}
