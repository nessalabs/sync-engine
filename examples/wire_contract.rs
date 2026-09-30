//! Machine-readable owner bounds for generated host wire contracts.
//! No adapter, optional feature, or serialization dependency is required.
use nessa_sync::replication::{
    catalogue::{MAX_CATALOGUE_ENTRIES, MAX_CATALOGUE_PAYLOAD_BYTES},
    domain::MAX_ID_BYTES,
};

fn render() -> String {
    format!("{{\"id_max_utf8_bytes\":{MAX_ID_BYTES},\"catalogue_max_entries\":{MAX_CATALOGUE_ENTRIES},\"catalogue_max_payload_bytes\":{MAX_CATALOGUE_PAYLOAD_BYTES}}}")
}

fn main() {
    println!("{}", render());
}

#[cfg(test)]
#[path = "../tests/replication/wire_contract.rs"]
mod tests;
