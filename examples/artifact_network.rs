//! Separate-process artifact transfer lab; run scripts/verify-artifact-transfer.py.

use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;

use nessa_sync::replication::application::RecordSource;
use nessa_sync::replication::artifacts::{
    publish_if_current, transfer_one, ArtifactCacheIndex, ArtifactKey, ArtifactState, ChunkReply,
    ChunkRequest, ChunkSource, ManifestRequest, ManifestSource, ManifestSourceError, TransferError,
    TransferStore, TransferStoreError, MAX_CHUNK_BYTES,
};
use nessa_sync::replication::domain::{Id, Scope};
use nessa_sync::replication::infrastructure::{
    LoopbackClient, LoopbackConfig, LoopbackServer, SqliteArtifactCache, SqliteArtifactSource,
};

fn id(value: &str) -> Id {
    Id::new(value).expect("valid lab identity")
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("gateway"),
        id("files"),
        id("first"),
        id("opaque"),
        id("epoch-1"),
    )
}
fn key(receiver: &str, artifact: &str) -> ArtifactKey {
    ArtifactKey {
        scope: scope(receiver),
        id: id(artifact),
    }
}
fn address(port: &str) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port.parse().expect("valid port"))
}
fn source(path: &str) -> SqliteArtifactSource {
    SqliteArtifactSource::open(path, id("gateway"), id("files"), id("first"), id("opaque")).unwrap()
}
fn client(port: &str, token: &str) -> LoopbackClient {
    LoopbackClient::new(address(port), id(token)).unwrap()
}
fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") if args.len() == 6 || args.len() == 7 => {
            let server = LoopbackServer::bind(args[3].parse().unwrap(),LoopbackConfig {
                source_path: PathBuf::from(&args[2]).with_extension("records.db"),
                catalogue_source_path: None,
                artifact_source_path: Some(PathBuf::from(&args[2])),
                catalogue_fault: None,
                origin: id("gateway"),stream: id("files"),incarnation: id("first"),
                schema: id("opaque"),access_epoch: id(args.get(6).map_or("epoch-1",String::as_str)),
                read_token: id(&args[4]),write_token: id(&args[5]),
                allowed_receivers: vec![id("phone"),id("laptop")],
            }).unwrap();
            eprintln!("listening on {}",server.local_addr().unwrap());
            server.serve().unwrap();
        }
        Some("upsert") if args.len() == 6 => {
            let size: usize = args[4].parse().unwrap();
            assert!(size <= 8*1024*1024);
            let value = args[5].as_bytes();
            assert_eq!(value.len(),1);
            let revision = source(&args[2]).upsert(&id(&args[3]),&vec![value[0];size]).unwrap();
            println!("{{\"revision\":{revision},\"size\":{size}}}");
        }
        Some("delete") if args.len() == 4 => {
            let revision = source(&args[2]).delete(&id(&args[3])).unwrap();
            println!("{{\"revision\":{revision}}}");
        }
        Some("step") if args.len() == 7 => {
            let selected = key(&args[4],&args[5]);
            let mut remote = client(&args[2],&args[6]);
            let mut cache = SqliteArtifactCache::open(&args[3]).unwrap();
            let request = ManifestRequest { key: selected };
            let manifest = match remote.manifest(&request) {
                Ok(reply) => reply.manifest,
                Err(ManifestSourceError::Denied | ManifestSourceError::ScopeChanged) => {
                    cache.revoke(&request.key).unwrap();
                    println!("{{\"revoked\":true}}");
                    return;
                }
                Err(error) => panic!("manifest unavailable: {error:?}"),
            };
            let progress = cache.accept_manifest(&manifest).unwrap();
            if matches!(manifest.state,ArtifactState::Deleted) {
                println!("{{\"deleted\":true,\"offset\":0,\"verified\":false}}");
                return;
            }
            if progress.verified {
                println!("{{\"deleted\":false,\"offset\":{},\"verified\":true,\"payload_bytes\":0,\"protocol_bytes\":{}}}",
                    progress.next_offset,remote.counters().protocol_bytes);
                return;
            }
            let ArtifactState::Live(content) = manifest.state else { unreachable!() };
            let mut latest = transfer_one(&manifest, MAX_CHUNK_BYTES, &mut remote, &mut cache).unwrap();
            if latest.next_offset == content.length {
                match publish_if_current(&manifest, &mut remote, &mut cache) {
                    Ok(published) => latest = published,
                    Err(TransferError::VersionChanged | TransferError::Deleted) => {
                        println!("{{\"changed_before_publish\":true}}");
                        return;
                    }
                    Err(error) => panic!("publish refused: {error:?}"),
                }
            }
            let wire = remote.counters();
            println!("{{\"deleted\":false,\"revision\":{},\"offset\":{},\"verified\":{},\"payload_bytes\":{},\"duplicate_bytes\":{},\"protocol_bytes\":{}}}",
                manifest.revision,latest.next_offset,latest.verified,wire.artifact_payload_bytes,
                wire.artifact_duplicate_bytes,wire.protocol_bytes);
        }
        Some("show") if args.len() == 5 => {
            let selected = key(&args[3],&args[4]);
            let mut cache = SqliteArtifactCache::open(&args[2]).unwrap();
            match cache.lookup(&selected) {
                Err(nessa_sync::replication::artifacts::ArtifactCacheError::ScopeChanged) => {
                    println!("{{\"revoked\":true}}");
                }
                Err(error) => panic!("cache unavailable: {error:?}"),
                Ok(Some(saved)) => {
                    let bytes = cache.read_verified(&saved.manifest).unwrap();
                    let offset = cache.progress(&saved.manifest).unwrap().unwrap().next_offset;
                    println!("{{\"revision\":{},\"deleted\":{},\"verified\":{},\"size\":{},\"offset\":{}}}",
                        saved.manifest.revision,matches!(saved.manifest.state,ArtifactState::Deleted),
                        bytes.is_some(),bytes.as_ref().map_or(0,Vec::len),offset);
                }
                Ok(None) => println!("{{\"missing\":true}}"),
            }
        }
        Some("record-head") if args.len() == 5 => {
            let mut remote = client(&args[2],&args[4]);
            let head = RecordSource::head(&mut remote,&scope(&args[3])).unwrap();
            println!("{{\"head\":{head}}}");
        }
        Some("repeat") if args.len() == 6 => {
            let selected = key(&args[3], &args[4]);
            let mut remote = client(&args[2], &args[5]);
            let manifest = remote.manifest(&ManifestRequest { key: selected }).unwrap().manifest;
            let request = ChunkRequest { manifest, offset: 0, max_bytes: MAX_CHUNK_BYTES };
            remote.chunk(&request).unwrap();
            remote.chunk(&request).unwrap();
            let wire = remote.counters();
            println!("{{\"payload_bytes\":{},\"duplicate_bytes\":{},\"protocol_bytes\":{}}}",
                wire.artifact_payload_bytes, wire.artifact_duplicate_bytes, wire.protocol_bytes);
        }
        Some("inject-bad") if args.len() == 7 => {
            let selected = key(&args[4],&args[5]);
            let mut remote = client(&args[2],&args[6]);
            let mut cache = SqliteArtifactCache::open(&args[3]).unwrap();
            let manifest = remote.manifest(&ManifestRequest { key: selected }).unwrap().manifest;
            let ArtifactState::Live(content) = manifest.state else { panic!("deleted") };
            assert!(content.length > 0 && content.length <= MAX_CHUNK_BYTES as u64);
            cache.accept_manifest(&manifest).unwrap();
            let request = ChunkRequest { manifest: manifest.clone(),offset: 0,max_bytes: MAX_CHUNK_BYTES };
            cache.append(&ChunkReply { request,bytes: vec![b'!';content.length as usize] }).unwrap();
            assert_eq!(cache.publish(&manifest),Err(TransferStoreError::HashMismatch));
            let state = cache.progress(&manifest).unwrap().unwrap();
            println!("{{\"hash_mismatch\":true,\"offset\":{},\"verified\":{}}}",state.next_offset,state.verified);
        }
        _ => panic!("usage: artifact_network serve SOURCE_DB PORT READ WRITE | upsert SOURCE_DB ID SIZE BYTE | delete SOURCE_DB ID | step PORT CACHE_DB RECEIVER ID READ | show CACHE_DB RECEIVER ID | record-head PORT RECEIVER READ | inject-bad PORT CACHE_DB RECEIVER ID READ"),
    }
}
