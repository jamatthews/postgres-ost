use once_cell::sync::OnceCell;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgVersion {
    pub version_num: i32,
}

static PG_VERSION: OnceCell<Mutex<Option<PgVersion>>> = OnceCell::new();

pub fn set_pg_version(version_num: i32) {
    let cell = PG_VERSION.get_or_init(|| Mutex::new(None));
    *cell.lock().unwrap() = Some(PgVersion { version_num });
}

pub fn get_pg_version() -> Option<PgVersion> {
    PG_VERSION.get().and_then(|cell| *cell.lock().unwrap())
}

pub fn detect_and_set_pg_version(client: &mut postgres::Client) -> anyhow::Result<PgVersion> {
    let row = client.query_one("SHOW server_version_num", &[])?;
    let version_num: i32 = row.get::<_, String>(0).parse()?;
    set_pg_version(version_num);
    Ok(PgVersion { version_num })
}
