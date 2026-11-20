//! Machine-readable owner bounds for generated host wire contracts.
//! No adapter, optional feature, or serialization dependency is required.
use nessa_sync::replication::{
    application::MAX_RECORD_BYTES,
    catalogue::{MAX_CATALOGUE_ENTRIES, MAX_CATALOGUE_PAYLOAD_BYTES},
    domain::{
        KNOWN_READER_CAPABILITIES, KNOWN_WRITER_CAPABILITIES, MAX_ID_BYTES, SOURCE_HEAD_VERSION,
    },
};

fn render() -> String {
    format!("{{\"record_max_payload_bytes\":{MAX_RECORD_BYTES},\"id_max_utf8_bytes\":{MAX_ID_BYTES},\"catalogue_max_entries\":{MAX_CATALOGUE_ENTRIES},\"catalogue_max_payload_bytes\":{MAX_CATALOGUE_PAYLOAD_BYTES},\"source_head_version\":{SOURCE_HEAD_VERSION},\"known_reader_capabilities\":{KNOWN_READER_CAPABILITIES},\"known_writer_capabilities\":{KNOWN_WRITER_CAPABILITIES}}}")
}

fn main() {
    println!("{}", render());
}

#[cfg(test)]
#[path = "../tests/replication/wire_contract.rs"]
mod tests;
