use anyhow::Result;
use postgres::{Client, NoTls};

mod args;
use args::*;

mod migration;
use migration::Migration;
mod backfill;

fn main() -> Result<()> {
    let args = get_args()?;
    let mut client = Client::connect(&args.uri, NoTls)?;
    let migration = Migration::new(&args.sql);

    let create_schema_statemement = format!("CREATE SCHEMA IF NOT EXISTS post_migrations");
    client.simple_query(&create_schema_statemement)?;

    migration.orchestrate(&mut client, args.execute)?;

    Ok(())
}
