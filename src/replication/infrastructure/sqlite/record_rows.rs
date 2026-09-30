//! SQLite record text acquisition shared by forward, history and replica reads.
//! The representation/bounds table in the core walkthrough owns the storage and
//! conversion accounting; `id_preflight_tests` exercises its encoding/order rows.

use rusqlite::{Error, Row};

use crate::replication::domain::{Id, ValidationError, MAX_ID_BYTES};

// A storage envelope derived from the semantic owner, not ID admissibility.
// UTF-16 ASCII can occupy two stored bytes per accepted UTF-8 byte.
pub(super) const MAX_STORED_ID_BYTES: usize = 2 * MAX_ID_BYTES;

// Call after storage and payload metadata admission in the same read snapshot.
// Borrow the returned UTF-8 before retaining an ID; copy payload only after the
// domain owner accepts it. SQLite/type errors and domain refusals stay distinct
// until the source/store port maps them to its existing availability failure.
pub(super) fn read_record_row(
    row: &Row<'_>,
) -> rusqlite::Result<Result<(Id, Vec<u8>), ValidationError>> {
    #[cfg(test)]
    super::id_preflight_tests::materialized();
    let value = row.get_ref(0)?;
    let text = value
        .as_str()
        .map_err(|error| Error::FromSqlConversionFailure(0, value.data_type(), Box::new(error)))?;
    let id = match Id::new(text) {
        Ok(id) => id,
        Err(error) => return Ok(Err(error)),
    };
    #[cfg(test)]
    super::id_preflight_tests::copied_payload();
    Ok(Ok((id, row.get(1)?)))
}
