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

    migration.drop_shadow_table_if_exists(&mut client)?;
    migration.create_shadow_table(&mut client)?;
    migration.migrate_shadow_table(&mut client)?;
    migration.create_log_table(&mut client)?;
    // TODO need to add triggers to log changes to the table into the log table
    migration.backfill_shadow_table(&mut client)?;
    migration.replay_log(&mut client)?;
    if args.execute {
        // TODO need to lock table against writes, finish replay, then swap tables
        migration.swap_tables(&mut client)?;
        migration.drop_old_table_if_exists(&mut client)?;
    } else {
        migration.drop_shadow_table_if_exists(&mut client)?;
    }

    Ok(())
}
