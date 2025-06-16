// Slot management for logical replication

#[derive(Clone)]
pub struct Slot {
    pub name: String,
    pub plugin: String,
}

impl Slot {
    pub fn new(name: String) -> Self {
        Slot {
            name,
            plugin: "wal2json".to_string(),
        }
    }

    pub fn create_slot<C: postgres::GenericClient>(&self, client: &mut C) -> anyhow::Result<()> {
        let create_slot_statement = format!(
            "SELECT pg_create_logical_replication_slot('{}', '{}')",
            self.name, self.plugin
        );
        client.simple_query(&create_slot_statement)?;
        Ok(())
    }

    pub fn drop_slot<C: postgres::GenericClient>(&self, client: &mut C) -> anyhow::Result<()> {
        let drop_slot_statement = format!("SELECT pg_drop_replication_slot('{}')", self.name);
        client.simple_query(&drop_slot_statement)?;
        Ok(())
    }

    pub fn get_changes<C: postgres::GenericClient>(
        &self,
        client: &mut C,
        upto_n_changes: i64,
    ) -> anyhow::Result<Vec<postgres::Row>> {
        let get_changes_statement = format!(
            "SELECT * FROM pg_logical_slot_get_changes('{}', NULL, {})",
            self.name, upto_n_changes
        );
        let rows = client.query(&get_changes_statement, &[])?;
        Ok(rows)
    }

    /// Fetch the confirmed_flush_lsn for this slot from the database.
    pub fn confirmed_flush_lsn(
        &self,
        client: &mut postgres::Client,
    ) -> anyhow::Result<crate::logical_replication::message::Lsn> {
        let row = client.query_one(
            &format!(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
                self.name
            ),
            &[],
        )?;
        let pg_lsn: postgres::types::PgLsn = row.get(0);
        let lsn_str = pg_lsn.to_string();
        crate::logical_replication::message::Lsn::from_pg_string(&lsn_str)
            .ok_or_else(|| anyhow::anyhow!("Failed to parse confirmed_flush_lsn: {}", lsn_str))
    }
}
