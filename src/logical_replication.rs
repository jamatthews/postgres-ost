#[derive(Clone)]
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

    pub fn consume_changes<C: postgres::GenericClient>(
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
}

#[derive(Clone)]
pub struct Publication {
    pub name: String,
    pub table: crate::table::Table,
    pub slot: crate::logical_replication::Slot,
}

impl Publication {
    pub fn new(name: String, table: crate::table::Table, slot: Slot) -> Self {
        Publication { name, table, slot }
    }

    pub fn create<C: postgres::GenericClient>(&self, client: &mut C) -> anyhow::Result<()> {
        // Set REPLICA IDENTITY FULL for the table
        let identity_sql = format!("ALTER TABLE {} REPLICA IDENTITY FULL", self.table);
        client.simple_query(&identity_sql)?;
        // Create the publication
        let create_pub_sql = format!("CREATE PUBLICATION {} FOR TABLE {}", self.name, self.table);
        client.simple_query(&create_pub_sql)?;
        Ok(())
    }

    pub fn drop<C: postgres::GenericClient>(&self, client: &mut C) -> anyhow::Result<()> {
        let drop_pub_sql = format!("DROP PUBLICATION IF EXISTS {}", self.name);
        client.simple_query(&drop_pub_sql)?;
        Ok(())
    }
}
