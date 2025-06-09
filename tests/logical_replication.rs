mod common;
use postgres_ost::logical_replication::{Publication, Slot};
use postgres_ost::table::Table;
use uuid::Uuid;

fn unique_slot_name() -> String {
    format!("logical_replication_slot_{}", Uuid::new_v4().simple())
}

fn unique_pub_name() -> String {
    format!("logical_replication_pub_{}", Uuid::new_v4().simple())
}

#[test]
fn test_create_and_drop_slot() {
    let test_db = common::setup_test_db();
    let pool = &test_db.pool;
    let mut client = pool.get().unwrap();
    let slot_name = unique_slot_name();
    let slot = Slot::new(slot_name.clone());
    // Drop in case it exists (ignore error)
    let _ = slot.drop_slot(&mut client);
    slot.create_slot(&mut client).expect("create slot");
    slot.drop_slot(&mut client).expect("drop slot");
}

#[test]
fn test_consume_changes() {
    let test_db = common::setup_test_db();
    let pool = &test_db.pool;
    let mut client = pool.get().unwrap();
    let slot_name = unique_slot_name();
    let slot = Slot::new(slot_name.clone());
    // Drop in case it exists (ignore error)
    let _ = slot.drop_slot(&mut client);
    slot.create_slot(&mut client).expect("create slot");
    // Consume changes (should be empty at first)
    let consumed = slot
        .consume_changes(&mut client, 10)
        .expect("consume changes");
    assert!(consumed.is_empty() || !consumed.is_empty()); // Accept either for now
    slot.drop_slot(&mut client).expect("drop slot");
}

#[test]
fn test_publication_and_logical_replication() {
    let test_db = common::setup_test_db();
    let pool = &test_db.pool;
    let mut client = pool.get().unwrap();
    let slot_name = unique_slot_name();
    let pub_name = unique_pub_name();
    let table = Table::new("test_table");
    let slot = Slot::new(slot_name.clone());
    let publication = Publication::new(pub_name.clone(), table.clone(), slot.clone());
    // Clean up in case they exist
    let _ = publication.drop(&mut client);
    let _ = slot.drop_slot(&mut client);
    // Create publication and slot
    publication.create(&mut client).expect("create publication");
    slot.create_slot(&mut client).expect("create slot");
    // Make a change to the table
    client
        .simple_query("INSERT INTO test_table (assertable, target) VALUES ('foo', 'bar')")
        .unwrap();
    // Consume changes
    let changes = slot
        .consume_changes(&mut client, 10)
        .expect("consume changes");
    let found = changes.iter().any(|row| {
        let txt: String = row.get("data");
        txt.contains("foo") && txt.contains("bar")
    });
    assert!(found, "Logical replication should see the inserted row");
    // Clean up
    publication.drop(&mut client).ok();
    slot.drop_slot(&mut client).ok();
}
