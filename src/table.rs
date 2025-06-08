// src/table.rs
// Extracted Table struct and related impls from migration.rs

use anyhow::Result;
use postgres::Client;
use postgres::types::Type;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Table {
    pub schema: Option<String>,
    pub name: String,
}

impl FromStr for Table {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((schema, name)) = s.split_once('.') {
            Ok(Table {
                schema: Some(schema.to_string()),
                name: name.to_string(),
            })
        } else {
            Ok(Table {
                schema: None,
                name: s.to_string(),
            })
        }
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{}.{}", schema, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

impl Table {
    pub fn new(full_name: &str) -> Self {
        full_name.parse().unwrap()
    }

    pub fn get_primary_key_info(&self, client: &mut Client) -> Result<crate::PrimaryKeyInfo> {
        let full_table = self.to_string();
        let row = client.query_one(
            "SELECT a.attname, a.atttypid::regtype::text
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
             WHERE i.indrelid = ($1)::text::regclass AND i.indisprimary
             LIMIT 1",
            &[&full_table],
        )?;
        let name: String = row.get(0);
        let type_name: String = row.get(1);
        let ty = match type_name.as_str() {
            "integer" => Type::INT4,
            "bigint" => Type::INT8,
            _ => panic!("Unsupported PK type: {}", type_name),
        };
        Ok(crate::PrimaryKeyInfo { name, ty })
    }

    pub fn get_columns(&self, client: &mut Client) -> Vec<String> {
        let rows = client.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
            &[&self.schema.as_deref().unwrap_or("public"), &self.name],
        ).unwrap();
        rows.iter()
            .map(|row| row.get::<_, String>("column_name"))
            .collect()
    }
}
