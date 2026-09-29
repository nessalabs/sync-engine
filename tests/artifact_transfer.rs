use nessa_sync::replication::artifacts::{
    validate_chunk, ArtifactKey, ArtifactManifest, ArtifactState, ChunkReply, ChunkRequest,
    ChunkValidationError, ContentIdentity, MAX_CHUNK_BYTES,
};
use nessa_sync::replication::domain::{Id, Scope};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn manifest(bytes: &[u8]) -> ArtifactManifest {
    ArtifactManifest {
        key: ArtifactKey {
            scope: Scope::new(
                id("phone"),
                id("gateway"),
                id("files"),
                id("first"),
                id("opaque"),
                id("epoch"),
            ),
            id: id("artifact"),
        },
        revision: 1,
        state: ArtifactState::Live(ContentIdentity::of(bytes)),
    }
}

#[test]
fn chunk_reply_must_cover_exact_version_offset_and_length() {
    let bytes = vec![b'x'; MAX_CHUNK_BYTES + 9];
    let request = ChunkRequest {
        manifest: manifest(&bytes),
        offset: 0,
        max_bytes: MAX_CHUNK_BYTES,
    };
    let good = ChunkReply {
        request: request.clone(),
        bytes: bytes[..MAX_CHUNK_BYTES].to_vec(),
    };
    assert_eq!(validate_chunk(&request, &good), Ok(()));
    let mut short = good.clone();
    short.bytes.pop();
    assert_eq!(
        validate_chunk(&request, &short),
        Err(ChunkValidationError::WrongLength)
    );
    let mut stale = good.clone();
    stale.request.manifest.revision += 1;
    assert_eq!(
        validate_chunk(&request, &stale),
        Err(ChunkValidationError::WrongRequest)
    );
    let mut foreign = good.clone();
    foreign.request.manifest.key.id = id("other");
    assert_eq!(
        validate_chunk(&request, &foreign),
        Err(ChunkValidationError::WrongRequest)
    );
    let last = ChunkRequest {
        manifest: request.manifest.clone(),
        offset: MAX_CHUNK_BYTES as u64,
        max_bytes: MAX_CHUNK_BYTES,
    };
    assert_eq!(
        validate_chunk(
            &last,
            &ChunkReply {
                request: last.clone(),
                bytes: bytes[MAX_CHUNK_BYTES..].to_vec()
            }
        ),
        Ok(())
    );
    let past = ChunkRequest {
        offset: bytes.len() as u64,
        ..last
    };
    assert_eq!(
        validate_chunk(
            &past,
            &ChunkReply {
                request: past.clone(),
                bytes: vec![]
            }
        ),
        Err(ChunkValidationError::InvalidRequest)
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_stage_recovers_restart_rejects_changes_and_publishes_only_verified_bytes() {
    use nessa_sync::replication::artifacts::{
        ChunkSource, ChunkSourceError, ManifestRequest, ManifestSource, TransferStore,
        TransferStoreError,
    };
    use nessa_sync::replication::infrastructure::{SqliteArtifactCache, SqliteArtifactSource};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory: PathBuf = std::env::temp_dir().join(format!(
        "nessa-artifact-transfer-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    let source_path = directory.join("source.db");
    let cache_path = directory.join("cache.db");
    let mut source = SqliteArtifactSource::open(
        &source_path,
        id("gateway"),
        id("files"),
        id("first"),
        id("opaque"),
    )
    .unwrap();
    let original = vec![b'a'; MAX_CHUNK_BYTES * 3 + 7];
    source.upsert(&id("artifact"), &original).unwrap();
    let key = manifest(&original).key;
    let current = source
        .manifest(&ManifestRequest { key: key.clone() })
        .unwrap()
        .manifest;
    let mut cache = SqliteArtifactCache::open(&cache_path).unwrap();
    cache.accept_manifest(&current).unwrap();
    let first = ChunkRequest {
        manifest: current.clone(),
        offset: 0,
        max_bytes: MAX_CHUNK_BYTES,
    };
    let response = source.chunk(&first).unwrap();
    assert_eq!(
        cache.append(&response).unwrap().next_offset,
        MAX_CHUNK_BYTES as u64
    );
    drop(cache);
    let mut cache = SqliteArtifactCache::open(&cache_path).unwrap();
    assert_eq!(
        cache.append(&response).unwrap().next_offset,
        MAX_CHUNK_BYTES as u64
    );
    assert_eq!(
        cache.progress(&current).unwrap().unwrap().next_offset,
        MAX_CHUNK_BYTES as u64
    );
    let second = ChunkRequest {
        manifest: current.clone(),
        offset: MAX_CHUNK_BYTES as u64,
        max_bytes: MAX_CHUNK_BYTES,
    };
    let mut wrong = source.chunk(&second).unwrap();
    wrong.bytes.fill(b'z');
    cache.append(&wrong).unwrap();
    let mut offset = (MAX_CHUNK_BYTES * 2) as u64;
    while offset < original.len() as u64 {
        let request = ChunkRequest {
            manifest: current.clone(),
            offset,
            max_bytes: MAX_CHUNK_BYTES,
        };
        offset = cache
            .append(&source.chunk(&request).unwrap())
            .unwrap()
            .next_offset;
    }
    assert_eq!(
        cache.publish(&current),
        Err(TransferStoreError::HashMismatch)
    );
    assert_eq!(cache.progress(&current).unwrap().unwrap().next_offset, 0);
    assert_eq!(cache.read_verified(&current).unwrap(), None);
    let mut offset = 0;
    while offset < original.len() as u64 {
        let request = ChunkRequest {
            manifest: current.clone(),
            offset,
            max_bytes: MAX_CHUNK_BYTES,
        };
        offset = cache
            .append(&source.chunk(&request).unwrap())
            .unwrap()
            .next_offset;
    }
    assert!(cache.publish(&current).unwrap().verified);
    assert_eq!(cache.read_verified(&current).unwrap(), Some(original));
    let changed = vec![b'b'; MAX_CHUNK_BYTES * 2];
    source.upsert(&id("artifact"), &changed).unwrap();
    assert_eq!(source.chunk(&first), Err(ChunkSourceError::VersionChanged));
    let newer = source
        .manifest(&ManifestRequest { key: key.clone() })
        .unwrap()
        .manifest;
    cache.accept_manifest(&newer).unwrap();
    assert_eq!(cache.read_verified(&newer).unwrap(), None);
    assert_eq!(cache.append(&response), Err(TransferStoreError::Stale));
    source.delete(&id("artifact")).unwrap();
    assert_eq!(
        source.chunk(&ChunkRequest {
            manifest: newer.clone(),
            offset: 0,
            max_bytes: MAX_CHUNK_BYTES
        }),
        Err(ChunkSourceError::Deleted)
    );
    let deletion = source.manifest(&ManifestRequest { key }).unwrap().manifest;
    cache.accept_manifest(&deletion).unwrap();
    assert_eq!(cache.read_verified(&deletion).unwrap(), None);
    assert_eq!(
        cache.accept_manifest(&newer),
        Err(TransferStoreError::Fenced)
    );
    let mut new_epoch = newer.clone();
    new_epoch.key.scope = Scope::new(
        id("phone"),
        id("gateway"),
        id("files"),
        id("first"),
        id("opaque"),
        id("epoch-2"),
    );
    assert_eq!(
        cache.accept_manifest(&new_epoch),
        Err(TransferStoreError::Fenced)
    );
    cache.revoke(&deletion.key).unwrap();
    assert_eq!(
        cache.progress(&deletion),
        Err(TransferStoreError::ScopeChanged)
    );
    drop(cache);
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(feature = "sqlite")]
#[test]
fn failed_chunk_or_publish_transaction_never_exposes_partial_artifact() {
    use nessa_sync::replication::artifacts::{TransferStore, TransferStoreError};
    use nessa_sync::replication::infrastructure::SqliteArtifactCache;
    use rusqlite::Connection;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "nessa-artifact-atomic-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let current = manifest(b"atomic content");
    let mut first = SqliteArtifactCache::open(&path).unwrap();
    first.accept_manifest(&current).unwrap();
    let request = ChunkRequest {
        manifest: current.clone(),
        offset: 0,
        max_bytes: MAX_CHUNK_BYTES,
    };
    let reply = ChunkReply {
        request,
        bytes: b"atomic content".to_vec(),
    };
    let fault = Connection::open(&path).unwrap();
    fault
        .execute_batch(
            "CREATE TRIGGER fail_offset BEFORE UPDATE OF next_offset ON artifact_cache
        BEGIN SELECT RAISE(ABORT, 'offset fail'); END;",
        )
        .unwrap();
    assert_eq!(first.append(&reply), Err(TransferStoreError::Failed));
    drop(first);
    let mut second = SqliteArtifactCache::open(&path).unwrap();
    assert_eq!(second.progress(&current).unwrap().unwrap().next_offset, 0);
    fault.execute_batch("DROP TRIGGER fail_offset").unwrap();
    assert_eq!(second.append(&reply).unwrap().next_offset, 14);
    let mut competing = SqliteArtifactCache::open(&path).unwrap();
    assert_eq!(competing.append(&reply).unwrap().next_offset, 14);
    let mut wrong = reply.clone();
    wrong.bytes.fill(b'x');
    assert_eq!(competing.append(&wrong), Err(TransferStoreError::Conflict));
    fault
        .execute_batch(
            "CREATE TRIGGER fail_verify BEFORE UPDATE OF verified ON artifact_cache
        BEGIN SELECT RAISE(ABORT, 'verify fail'); END;",
        )
        .unwrap();
    assert_eq!(second.publish(&current), Err(TransferStoreError::Failed));
    assert!(!competing.progress(&current).unwrap().unwrap().verified);
    assert_eq!(competing.read_verified(&current).unwrap(), None);
    fault.execute_batch("DROP TRIGGER fail_verify").unwrap();
    assert!(competing.publish(&current).unwrap().verified);
    assert_eq!(
        second.read_verified(&current).unwrap(),
        Some(b"atomic content".to_vec())
    );
    drop(fault);
    drop(second);
    drop(competing);
    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "sqlite")]
#[test]
fn final_source_recheck_installs_new_version_before_publication() {
    use nessa_sync::replication::artifacts::{
        publish_if_current, transfer_one, ManifestRequest, ManifestSource, TransferError,
        TransferStore,
    };
    use nessa_sync::replication::infrastructure::{SqliteArtifactCache, SqliteArtifactSource};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "nessa-artifact-recheck-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    let mut source = SqliteArtifactSource::open(
        directory.join("source.db"),
        id("gateway"),
        id("files"),
        id("first"),
        id("opaque"),
    )
    .unwrap();
    source.upsert(&id("artifact"), b"before").unwrap();
    let mut cache = SqliteArtifactCache::open(directory.join("cache.db")).unwrap();
    let first = source
        .manifest(&ManifestRequest {
            key: manifest(b"before").key,
        })
        .unwrap()
        .manifest;
    cache.accept_manifest(&first).unwrap();
    assert_eq!(
        transfer_one(&first, MAX_CHUNK_BYTES, &mut source, &mut cache)
            .unwrap()
            .next_offset,
        6
    );
    source.upsert(&id("artifact"), b"after").unwrap();
    assert_eq!(
        publish_if_current(&first, &mut source, &mut cache),
        Err(TransferError::VersionChanged)
    );
    let newer = source
        .manifest(&ManifestRequest {
            key: first.key.clone(),
        })
        .unwrap()
        .manifest;
    assert_eq!(cache.progress(&newer).unwrap().unwrap().next_offset, 0);
    assert_eq!(cache.read_verified(&newer).unwrap(), None);
    assert_eq!(
        transfer_one(&newer, MAX_CHUNK_BYTES, &mut source, &mut cache)
            .unwrap()
            .next_offset,
        5
    );
    source.delete(&id("artifact")).unwrap();
    assert_eq!(
        publish_if_current(&newer, &mut source, &mut cache),
        Err(TransferError::Deleted)
    );
    let deleted = source
        .manifest(&ManifestRequest {
            key: newer.key.clone(),
        })
        .unwrap()
        .manifest;
    assert_eq!(cache.progress(&deleted).unwrap().unwrap().next_offset, 0);
    drop(cache);
    drop(source);
    std::fs::remove_dir_all(directory).unwrap();
}
