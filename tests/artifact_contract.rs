use nessa_sync::replication::artifacts::{
    availability, validate_manifest, ArtifactKey, ArtifactManifest, ArtifactState, Availability,
    ContentIdentity, ManifestError, ManifestReply, ManifestRequest,
};
use nessa_sync::replication::domain::{Id, Scope};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn key(epoch: &str) -> ArtifactKey {
    ArtifactKey {
        scope: Scope::new(
            id("phone"),
            id("gateway"),
            id("artifacts"),
            id("first"),
            id("bytes-v1"),
            id(epoch),
        ),
        id: id("artifact-1"),
    }
}
fn live(key: ArtifactKey, revision: u64, bytes: &[u8]) -> ArtifactManifest {
    ArtifactManifest {
        key,
        revision,
        state: ArtifactState::Live(ContentIdentity::of(bytes)),
    }
}
fn reply(manifest: ArtifactManifest) -> ManifestReply {
    ManifestReply {
        request: ManifestRequest {
            key: manifest.key.clone(),
        },
        manifest,
    }
}

#[test]
fn exact_scope_revision_and_fence_order() {
    let first = live(key("epoch-1"), 1, b"first");
    let request = ManifestRequest {
        key: first.key.clone(),
    };
    assert_eq!(
        validate_manifest(&request, &reply(first.clone()), None),
        Ok(())
    );
    assert_eq!(
        validate_manifest(&request, &reply(first.clone()), Some(&first)),
        Ok(())
    );
    let later = live(first.key.clone(), 2, b"second");
    assert_eq!(
        validate_manifest(&request, &reply(later.clone()), Some(&first)),
        Ok(())
    );
    assert_eq!(
        validate_manifest(&request, &reply(first.clone()), Some(&later)),
        Err(ManifestError::Stale)
    );
    assert_eq!(
        validate_manifest(
            &request,
            &reply(live(first.key.clone(), 1, b"other")),
            Some(&first)
        ),
        Err(ManifestError::Conflict)
    );
    assert_eq!(
        validate_manifest(
            &request,
            &reply(live(key("epoch-2"), 2, b"other")),
            Some(&first)
        ),
        Err(ManifestError::Foreign)
    );
    let deletion = ArtifactManifest {
        key: first.key.clone(),
        revision: 3,
        state: ArtifactState::Deleted,
    };
    assert_eq!(
        validate_manifest(&request, &reply(deletion.clone()), Some(&later)),
        Ok(())
    );
    assert_eq!(
        validate_manifest(
            &request,
            &reply(live(first.key, 4, b"resurrected")),
            Some(&deletion)
        ),
        Err(ManifestError::Fenced)
    );
}

#[test]
fn availability_never_confuses_missing_bytes_deletion_and_corruption() {
    let current = live(key("epoch-1"), 1, b"hello");
    assert_eq!(
        availability(&current, None, true),
        Availability::MetadataOnly
    );
    assert_eq!(
        availability(&current, None, false),
        Availability::SourceUnavailable
    );
    assert_eq!(
        availability(&current, Some(b"hello"), false),
        Availability::CachedVerified
    );
    assert_eq!(
        availability(&current, Some(b"HELLO"), true),
        Availability::HashMismatch
    );
    let deleted = ArtifactManifest {
        key: current.key,
        revision: 2,
        state: ArtifactState::Deleted,
    };
    assert_eq!(
        availability(&deleted, Some(b"hello"), false),
        Availability::Deleted
    );
    let empty = live(key("epoch-1"), 1, b"");
    assert_eq!(
        availability(&empty, Some(b""), false),
        Availability::CachedVerified
    );
}
