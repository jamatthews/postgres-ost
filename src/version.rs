use once_cell::sync::OnceCell;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgVersion {
    pub major: u32,
    pub minor: u32,
}

static PG_VERSION: OnceCell<Mutex<Option<PgVersion>>> = OnceCell::new();

pub fn set_pg_version(major: u32, minor: u32) {
    let cell = PG_VERSION.get_or_init(|| Mutex::new(None));
    *cell.lock().unwrap() = Some(PgVersion { major, minor });
}

pub fn get_pg_version() -> Option<PgVersion> {
    PG_VERSION.get().and_then(|cell| *cell.lock().unwrap())
}

pub fn detect_and_set_pg_version(client: &mut postgres::Client) -> anyhow::Result<PgVersion> {
    let row = client.query_one("SHOW server_version_num", &[])?;
    let version_num: i32 = row.get::<_, String>(0).parse()?;
    let major = (version_num / 10000) as u32;
    let minor = ((version_num / 100) % 100) as u32;
    set_pg_version(major, minor);
    Ok(PgVersion { major, minor })
}
