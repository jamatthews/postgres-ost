#[derive(Clone)]
pub struct ColumnMap(pub Vec<(String, Option<String>)>);

impl ColumnMap {
    pub fn from_main_and_shadow(main_cols: &[String], shadow_cols: &[String]) -> Self {
        let mut map = Vec::new();
        let unmatched_main: Vec<String> = main_cols.iter().filter(|c| !shadow_cols.contains(c)).cloned().collect();
        let unmatched_shadow: Vec<String> = shadow_cols.iter().filter(|c| !main_cols.contains(c)).cloned().collect();
        for main_col in main_cols {
            if let Some(shadow_col) = shadow_cols.iter().find(|c| *c == main_col) {
                map.push((main_col.clone(), Some(shadow_col.clone())));
            } else if unmatched_main.len() == 1 && unmatched_shadow.len() == 1 && unmatched_main[0] == *main_col {
                // Assume rename
                map.push((main_col.clone(), Some(unmatched_shadow[0].clone())));
            } else {
                map.push((main_col.clone(), None));
            }
        }
        ColumnMap(map)
    }

    pub fn shadow_cols(&self) -> Vec<String> {
        self.0.iter().filter_map(|(_main, shadow)| shadow.clone()).collect()
    }
    pub fn main_cols(&self) -> Vec<String> {
        self.0.iter().filter_map(|(main, shadow)| shadow.as_ref().map(|_| main.clone())).collect()
    }
}