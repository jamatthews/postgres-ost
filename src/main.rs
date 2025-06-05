use anyhow::Result;
use postgres::{Client, NoTls};
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};

mod args;
use args::*;

mod migration;
use migration::Migration;
mod backfill;

fn main() -> Result<()> {
    let args = get_args()?;
    let manager = PostgresConnectionManager::new(args.uri.parse()?, R2d2NoTls);
    let pool = Pool::new(manager)?;
    let mut client = pool.get()?;
    let migration = Migration::new(&args.sql);
    migration.orchestrate(&mut client, args.execute)?;
    Ok(())
}
