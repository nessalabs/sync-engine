//! Separate-process loopback catalogue lab. Use scripts/verify-slice-7.py.

use std::env;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;

use nessa_sync::replication::catalogue::{apply_next_page, begin_or_resume, CatalogueStore};
use nessa_sync::replication::domain::{Id, Scope};
use nessa_sync::replication::infrastructure::{
    CatalogueFault, LoopbackClient, LoopbackConfig, LoopbackServer, MemoryAuthorizer,
    SqliteCatalogueStore,
};

fn id(value: &str) -> Id {
    Id::new(value).expect("valid lab identity")
}
fn scope(receiver: &str) -> Scope {
    Scope::new(
        id(receiver),
        id("gateway"),
        id("transcript-index"),
        id("first"),
        id("opaque-v1"),
        id("epoch-1"),
    )
}
fn address(port: &str) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port.parse().expect("valid port"))
}
fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") if args.len() == 6 || args.len() == 7 => {
            let fault = match args.get(6).map(String::as_str) {
                Some("drop") => Some(CatalogueFault::DropFirstReply),
                Some("truncate") => Some(CatalogueFault::TruncateFirstReply),
                None => None,
                _ => panic!("fault must be drop or truncate"),
            };
            let server = LoopbackServer::bind(args[3].parse().unwrap(), LoopbackConfig {
                source_path: PathBuf::from(&args[2]).with_extension("records.db"),
                catalogue_source_path: Some(PathBuf::from(&args[2])),
                artifact_source_path: None,
                catalogue_fault: fault,
                origin: id("gateway"), stream: id("transcript-index"),
                incarnation: id("first"), schema: id("opaque-v1"), access_epoch: id("epoch-1"),
                read_token: id(&args[4]), write_token: id(&args[5]),
                allowed_receivers: vec![id("phone"), id("laptop")],
            }).unwrap();
            eprintln!("listening on {}", server.local_addr().unwrap());
            server.serve().unwrap();
        }
        Some("pass") if args.len() == 7 => {
            let selected = scope(&args[4]);
            let mut client = LoopbackClient::new(address(&args[2]), id(&args[5])).unwrap();
            let mut store = SqliteCatalogueStore::open(&args[3]).unwrap();
            let mut authorizer = MemoryAuthorizer::allowed(selected.clone());
            let limit: usize = args[6].parse().unwrap();
            let mut pages = 0usize;
            let mut pass = begin_or_resume(&selected, &mut authorizer, &mut client, &mut store).unwrap();
            while let Some(active) = pass {
                if pages >= limit { break; }
                pass = apply_next_page(&active, 40, 512 * 1024, &mut authorizer, &mut client, &mut store).unwrap().active;
                pages += 1;
            }
            let saved = store.progress(&selected).unwrap().unwrap();
            let wire = client.counters();
            println!("{{\"pages\":{pages},\"completed\":{},\"active\":{},\"count\":{},\"manifest_bytes\":{},\"payload_bytes\":{},\"duplicate_bytes\":{},\"protocol_bytes\":{}}}",
                saved.completed, saved.active.is_some(), store.count(&selected).unwrap(),
                wire.catalogue_manifest_bytes, wire.catalogue_payload_bytes,
                wire.catalogue_duplicate_bytes, wire.protocol_bytes);
        }
        Some("show") if args.len() == 5 => {
            let store = SqliteCatalogueStore::open(&args[2]).unwrap();
            let entry = store.cached_entry(&scope(&args[3]), &id(&args[4])).unwrap();
            match entry {
                Some(entry) => println!("{{\"revision\":{},\"deleted\":{},\"payload\":{:?}}}",
                    entry.manifest.revision, entry.manifest.deleted,
                    String::from_utf8_lossy(&entry.payload)),
                None => println!("{{\"missing\":true}}"),
            }
        }
        Some("counters") if args.len() == 4 => {
            let mut client = LoopbackClient::new(address(&args[2]), id(&args[3])).unwrap();
            let (heads, manifests, resolves) = client.catalogue_counters().unwrap();
            println!("{{\"head_reads\":{heads},\"manifest_reads\":{manifests},\"resolve_reads\":{resolves}}}");
        }
        _ => panic!("usage: catalogue_network serve SOURCE_DB PORT READ_TOKEN WRITE_TOKEN | pass PORT RECEIVER_DB RECEIVER READ_TOKEN MAX_PAGES | show RECEIVER_DB RECEIVER ID | counters PORT WRITE_TOKEN"),
    }
}
