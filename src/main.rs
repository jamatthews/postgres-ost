//! Main binary entry point for postgres-ost.

use anyhow::Result;
use r2d2::{Pool};
use r2d2_postgres::{PostgresConnectionManager, postgres::NoTls as R2d2NoTls};
use postgres_ost::Migration;
use postgres_ost::args::get_args;

fn main() -> Result<()> {
    let args = get_args()?;
    let manager = PostgresConnectionManager::new(args.uri.parse()?, R2d2NoTls);
    let pool = Pool::new(manager)?;
    let mut migration = Migration::new(&args.sql);
    migration.orchestrate(&pool, args.execute)?;
    Ok(())
}
