use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the full migration (default)
    Migrate {
        /// PostgreSQL connection URI
        #[arg(short, long)]
        uri: String,

        /// ALTER TABLE statement
        #[arg(short, long)]
        sql: String,

        /// Execute the migration (swap tables and drop old table)
        #[arg(short, long, default_value = "false")]
        execute: bool,
    },
    /// Run only migration setup and log replay (no backfill)
    ReplayOnly {
        /// PostgreSQL connection URI
        #[arg(short, long)]
        uri: String,

        /// ALTER TABLE statement
        #[arg(short, long)]
        sql: String,
    },
}

pub fn get_args() -> Result<Args, clap::Error> {
    Args::try_parse()
}
