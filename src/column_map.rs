use crate::table::Table;
use postgres::Client;

/// Maps columns from the main table to the shadow table, handling renames and drops.
#[derive(Clone)]
pub struct ColumnMap(Vec<(String, Option<String>)>);

impl ColumnMap {
    /// Constructs a new `ColumnMap` from the main and shadow Table objects, fetching columns from the database.
    pub fn new(main: &Table, shadow: &Table, client: &mut Client) -> Self {
        let main_cols = main.get_columns(client);
        let shadow_cols = shadow.get_columns(client);
        let mut map = Vec::new();
        let unmatched_main: Vec<String> = main_cols
            .iter()
            .filter(|c| !shadow_cols.contains(c))
            .cloned()
            .collect();
        let unmatched_shadow: Vec<String> = shadow_cols
            .iter()
            .filter(|c| !main_cols.contains(c))
            .cloned()
            .collect();
        for main_col in &main_cols {
            if let Some(shadow_col) = shadow_cols.iter().find(|c| *c == main_col) {
                map.push((main_col.clone(), Some(shadow_col.clone())));
            } else if unmatched_main.len() == 1
                && unmatched_shadow.len() == 1
                && unmatched_main[0] == *main_col
            {
                // Assume rename
                map.push((main_col.clone(), Some(unmatched_shadow[0].clone())));
            } else {
                map.push((main_col.clone(), None));
            }
        }
        ColumnMap(map)
    }

    /// Returns the shadow table columns that correspond to main table columns.
    pub fn shadow_cols(&self) -> Vec<String> {
        self.0
            .iter()
            .filter_map(|(_main, shadow)| shadow.clone())
            .collect()
    }
    /// Returns the main table columns that have a corresponding shadow column.
    pub fn main_cols(&self) -> Vec<String> {
        self.0
            .iter()
            .filter_map(|(main, shadow)| shadow.as_ref().map(|_| main.clone()))
            .collect()
    }
}
