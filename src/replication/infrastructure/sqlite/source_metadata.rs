//! Shared value decoding only; each source table owns its own durable truth.
use crate::replication::domain::{CapabilityError, CapabilitySupport, Scope, SourceHead};
#[derive(Clone, Copy)]
pub(super) struct Publication {
    pub version: u16,
    pub position: u64,
    pub reader: u64,
    pub writer: u64,
}
impl Publication {
    pub fn head(self, scope: &Scope) -> SourceHead {
        SourceHead::advertised(
            scope.clone(),
            self.version,
            self.position,
            self.reader,
            self.writer,
        )
    }
    pub fn reader(self) -> Result<(), CapabilityError> {
        CapabilitySupport::current().validate_reader(self.version, self.reader)
    }
    pub fn writer(self) -> Result<(), CapabilityError> {
        CapabilitySupport::current().validate_writer(self.version, self.reader, self.writer)
    }
}

pub(super) fn decode(r: &rusqlite::Row<'_>) -> rusqlite::Result<Publication> {
    let version: i64 = r.get(0)?;
    let position: i64 = r.get(1)?;
    let reader: Vec<u8> = r.get(2)?;
    let writer: Vec<u8> = r.get(3)?;
    let invalid = || rusqlite::Error::InvalidQuery;
    Ok(Publication {
        version: version.try_into().map_err(|_| invalid())?,
        position: position.try_into().map_err(|_| invalid())?,
        reader: u64::from_be_bytes(reader.try_into().map_err(|_| invalid())?),
        writer: u64::from_be_bytes(writer.try_into().map_err(|_| invalid())?),
    })
}
