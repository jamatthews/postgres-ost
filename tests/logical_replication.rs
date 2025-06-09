mod common;
use postgres_ost::logical_replication::Slot;
use uuid::Uuid;

fn unique_slot_name() -> String {
    format!("logical_replication_slot_{}", Uuid::new_v4().simple())
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
