//! Journal-mode transition for the catalogue source only. Call after the
//! existing source identity validation and publication migration succeed.
use rusqlite::{Connection, Error, ErrorCode};
use std::time::{Duration, Instant};

const RETRY_PAUSE: Duration = Duration::from_millis(10);

pub(super) fn configure(conn: &Connection) -> rusqlite::Result<()> {
    let busy_ms: i32 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
    let budget = Duration::from_millis(u64::try_from(busy_ms).map_err(|_| Error::InvalidQuery)?);
    // SQLite can bypass its busy handler on a journal-mode transition. Own
    // exactly one explicit startup waiting budget instead of nesting two.
    conn.busy_timeout(Duration::ZERO)?;
    let deadline = Instant::now() + budget;
    let result = enable_wal(conn, deadline, std::thread::sleep).and_then(|()| {
        conn.busy_timeout(deadline.saturating_duration_since(Instant::now()))?;
        conn.pragma_update(None, "synchronous", "FULL")
    });
    let restored = conn.busy_timeout(budget);
    result.and(restored)
}

fn enable_wal(
    conn: &Connection,
    deadline: Instant,
    mut wait: impl FnMut(Duration),
) -> rusqlite::Result<()> {
    loop {
        match conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get::<_, String>(0)) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            // Refuse a file or SQLite backend which did not actually select WAL.
            Ok(_) => return Err(Error::InvalidQuery),
            Err(err) if err.sqlite_error_code() == Some(ErrorCode::DatabaseBusy) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(err);
                }
                wait(remaining.min(RETRY_PAUSE));
                if Instant::now() >= deadline {
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::domain::Id;
    use crate::replication::infrastructure::{SqliteCatalogueSource, SqliteOpenError};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct File(PathBuf);
    impl File {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "catalogue-journal-mode-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&dir).unwrap();
            Self(dir)
        }
        fn open(&self) -> Connection {
            let db = Connection::open(self.0.join("catalogue.db")).unwrap();
            db.busy_timeout(Duration::ZERO).unwrap();
            db
        }
    }
    impl Drop for File {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn journal(db: &Connection) -> String {
        db.pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn one_busy_transition_can_finish_after_the_blocker_releases() {
        let file = File::new();
        let writer = file.open();
        writer
            .execute_batch(
                "CREATE TABLE sample(key INTEGER); BEGIN IMMEDIATE; INSERT INTO sample VALUES(1)",
            )
            .unwrap();
        let contender = file.open();
        assert_eq!(journal(&contender), "delete");
        let mut waited = 0;
        enable_wal(
            &contender,
            Instant::now() + Duration::from_secs(1),
            |pause| {
                assert_eq!(waited, 0);
                assert!(pause <= RETRY_PAUSE);
                waited += 1;
                writer.execute_batch("COMMIT").unwrap();
            },
        )
        .unwrap();
        assert_eq!(waited, 1);
        assert_eq!(journal(&contender), "wal");
    }

    #[test]
    fn exhausted_startup_budget_does_not_install_wal_or_change_the_deadline() {
        let file = File::new();
        let writer = file.open();
        writer
            .execute_batch(
                "CREATE TABLE sample(key INTEGER); BEGIN IMMEDIATE; INSERT INTO sample VALUES(1)",
            )
            .unwrap();
        let contender = file.open();
        let result = configure(&contender);
        assert_eq!(
            result.unwrap_err().sqlite_error_code(),
            Some(ErrorCode::DatabaseBusy)
        );
        assert_eq!(journal(&contender), "delete");
        let timeout: i64 = contender
            .pragma_query_value(None, "busy_timeout", |r| r.get(0))
            .unwrap();
        assert_eq!(timeout, 0);
        writer.execute_batch("COMMIT").unwrap();
        contender.busy_timeout(Duration::from_millis(25)).unwrap();
        configure(&contender).unwrap();
        let timeout: i64 = contender
            .pragma_query_value(None, "busy_timeout", |r| r.get(0))
            .unwrap();
        assert_eq!(timeout, 25);
        let synchronous: u8 = contender
            .pragma_query_value(None, "synchronous", |r| r.get(0))
            .unwrap();
        assert_eq!(synchronous, 2);
        assert_eq!(journal(&contender), "wal");
    }
    #[test]
    fn wrong_source_identity_does_not_touch_the_journal_mode() {
        let file = File::new();
        let path = file.0.join("catalogue.db");
        let id = |v| Id::new(v).unwrap();
        let open = |origin| {
            SqliteCatalogueSource::open(
                &path,
                id(origin),
                id("catalogue"),
                id("incarnation"),
                id("schema"),
            )
        };
        drop(open("correct").unwrap());
        let conn = file.open();
        let rollback = conn
            .pragma_update_and_check(None, "journal_mode", "DELETE", |r| r.get::<_, String>(0))
            .unwrap();
        assert_eq!(rollback, "delete");
        assert!(matches!(
            open("other"),
            Err(SqliteOpenError::SourceIdentityMismatch)
        ));
        assert_eq!(journal(&conn), "delete");
    }
}
