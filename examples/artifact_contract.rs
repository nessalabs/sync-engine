//! Small host-owned artifact status example; no network or file transfer.

use nessa_sync::replication::artifacts::{
    availability, validate_manifest, ArtifactCacheError, ArtifactCacheIndex, ArtifactKey,
    ArtifactManifest, ArtifactState, Availability, CachedArtifact, ContentIdentity, ManifestReply,
    ManifestRequest, ManifestSource, ManifestSourceError,
};
use nessa_sync::replication::domain::{Id, Scope};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

struct Source(ArtifactManifest);
impl ManifestSource for Source {
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestReply, ManifestSourceError> {
        Ok(ManifestReply {
            request: request.clone(),
            manifest: self.0.clone(),
        })
    }
}
struct Cache(Option<CachedArtifact>);
impl ArtifactCacheIndex for Cache {
    fn lookup(&mut self, _key: &ArtifactKey) -> Result<Option<CachedArtifact>, ArtifactCacheError> {
        Ok(self.0.clone())
    }
}
fn main() {
    let key = ArtifactKey {
        scope: Scope::new(
            id("phone"),
            id("gateway"),
            id("artifacts"),
            id("first"),
            id("bytes-v1"),
            id("epoch-1"),
        ),
        id: id("output-1"),
    };
    let live = ArtifactManifest {
        key: key.clone(),
        revision: 1,
        state: ArtifactState::Live(ContentIdentity::of(b"agent output")),
    };
    let request = ManifestRequest { key: key.clone() };
    let reply = Source(live.clone()).manifest(&request).unwrap();
    validate_manifest(&request, &reply, None).unwrap();
    let mut cache = Cache(Some(CachedArtifact {
        manifest: live.clone(),
        has_candidate_bytes: false,
    }));
    assert_eq!(cache.lookup(&key).unwrap().unwrap().manifest, live);
    assert_eq!(availability(&live, None, true), Availability::MetadataOnly);
    assert_eq!(
        availability(&live, None, false),
        Availability::SourceUnavailable
    );
    assert_eq!(
        availability(&live, Some(b"agent output"), false),
        Availability::CachedVerified
    );
    assert_eq!(
        availability(&live, Some(b"wrong bytes"), true),
        Availability::HashMismatch
    );
    let deleted = ArtifactManifest {
        key,
        revision: 2,
        state: ArtifactState::Deleted,
    };
    assert_eq!(
        availability(&deleted, Some(b"agent output"), false),
        Availability::Deleted
    );
    println!("{{\"metadata_only\":true,\"cached_verified\":true,\"source_unavailable\":true,\"deleted\":true,\"hash_mismatch\":true}}");
}
