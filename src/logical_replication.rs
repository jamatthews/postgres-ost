pub struct Slot {
    name: String,
    plugin: String,
}

impl Slot {
    pub fn new(name: String) -> Self {
        Slot {
            name,
            plugin: "wal2json".to_string(),
        }
    }

    pub fn create_slot(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        let create_slot_statement = format!(
            "SELECT pg_create_logical_replication_slot('{}', '{}')",
            self.name, self.plugin
        );
        client.simple_query(&create_slot_statement)?;
        Ok(())
    }

    pub fn drop_slot(&self, client: &mut postgres::Client) -> anyhow::Result<()> {
        let drop_slot_statement = format!("SELECT pg_drop_replication_slot('{}')", self.name);
        client.simple_query(&drop_slot_statement)?;
        Ok(())
    }

    pub fn consume_changes(
        &self,
        client: &mut postgres::Client,
        upto_n_changes: i64,
    ) -> anyhow::Result<Vec<postgres::Row>> {
        let get_changes_statement = format!(
            "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, {})",
            self.name, upto_n_changes
        );
        let rows = client.query(&get_changes_statement, &[])?;
        Ok(rows)
    }
}
