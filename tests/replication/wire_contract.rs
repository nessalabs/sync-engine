//! The machine-readable consumer fields remain equal to their published owners.
use super::{render, MAX_CATALOGUE_ENTRIES, MAX_CATALOGUE_PAYLOAD_BYTES, MAX_ID_BYTES};
use std::collections::HashMap;

#[test]
fn exported_values_follow_owner_changes_without_retyped_bounds() {
    let wire = render();
    // The Python gate parses the actual stdout as JSON. This test extracts this
    // numeric-only producer's fields to compare each value with its Rust owner.
    let fields: HashMap<&str, usize> = wire
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
    assert_eq!(fields.len(), 3);
    assert_eq!(fields.get("id_max_utf8_bytes"), Some(&MAX_ID_BYTES));
    assert_eq!(
        fields.get("catalogue_max_entries"),
        Some(&MAX_CATALOGUE_ENTRIES)
    );
    assert_eq!(
        fields.get("catalogue_max_payload_bytes"),
        Some(&MAX_CATALOGUE_PAYLOAD_BYTES)
    );
}
