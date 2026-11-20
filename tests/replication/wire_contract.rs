//! The machine-readable consumer fields remain equal to their published owners.
use super::{
    render, KNOWN_READER_CAPABILITIES, KNOWN_WRITER_CAPABILITIES, MAX_CATALOGUE_ENTRIES,
    MAX_CATALOGUE_PAYLOAD_BYTES, MAX_ID_BYTES, MAX_RECORD_BYTES, SOURCE_HEAD_VERSION,
};
use std::collections::HashMap;

#[test]
fn exported_values_follow_owner_changes_without_retyped_bounds() {
    let wire = render();
    // The Python gate parses the actual stdout as JSON. This test extracts this
    // numeric-only producer's fields to compare each value with its Rust owner.
    let fields: HashMap<&str, u64> = wire
        .strip_prefix('{')
        .unwrap()
        .strip_suffix('}')
        .unwrap()
        .split(',')
        .map(|field| {
            let (name, value) = field.split_once(':').unwrap();
            (name.trim_matches('"'), value.parse().unwrap())
        })
        .collect();
    assert_eq!(fields.len(), 7);
    assert_eq!(
        fields.get("source_head_version"),
        Some(&(SOURCE_HEAD_VERSION as u64))
    );
    assert_eq!(
        fields.get("known_reader_capabilities"),
        Some(&(KNOWN_READER_CAPABILITIES))
    );
    assert_eq!(
        fields.get("known_writer_capabilities"),
        Some(&(KNOWN_WRITER_CAPABILITIES))
    );
    assert_eq!(
        fields.get("record_max_payload_bytes"),
        Some(&(MAX_RECORD_BYTES as u64))
    );
    assert_eq!(
        fields.get("id_max_utf8_bytes"),
        Some(&(MAX_ID_BYTES as u64))
    );
    assert_eq!(
        fields.get("catalogue_max_entries"),
        Some(&(MAX_CATALOGUE_ENTRIES as u64))
    );
    assert_eq!(
        fields.get("catalogue_max_payload_bytes"),
        Some(&(MAX_CATALOGUE_PAYLOAD_BYTES as u64))
    );
}
