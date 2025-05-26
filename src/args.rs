use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// PostgreSQL connection URI
    #[arg(short, long)]
    pub uri: String,

    /// ALTER TABLE statement
    #[arg(short, long)]
    pub sql: String,

    /// Execute the migration (swap tables and drop old table)
    #[arg(short, long, default_value = "false")]
    pub execute: bool,
}

pub fn get_args() -> Result<Args, clap::Error> {
    Args::try_parse()
}
