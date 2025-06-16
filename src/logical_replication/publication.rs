// Publication management for logical replication

use crate::logical_replication::slot::Slot;

#[derive(Clone)]
pub struct Publication {
    pub name: String,
    pub table: crate::table::Table,
    pub slot: Slot,
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
