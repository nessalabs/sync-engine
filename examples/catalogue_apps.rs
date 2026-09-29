//! Two small host catalogue examples over one reusable core. Run with
//! `python3 scripts/verify-slice-6.py` for the complete process lab.

use std::env;
use std::path::PathBuf;

use nessa_sync::replication::catalogue::{apply_next_page, begin_or_resume, CatalogueStore};
use nessa_sync::replication::domain::{Id, Scope};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore,
};

fn id(text: &str) -> Id {
    Id::new(text).expect("valid example ID")
}
fn scope(app: &str) -> Scope {
    let stream = match app {
        "transcript" => "transcript-index",
        "tasks" => "task-index",
        _ => panic!("app must be transcript or tasks"),
    };
    Scope::new(
        id("phone"),
        id("gateway"),
        id(stream),
        id("first"),
        id("opaque-v1"),
        id("epoch-1"),
    )
}
fn source(path: PathBuf, scope: &Scope) -> SqliteCatalogueSource {
    SqliteCatalogueSource::open(
        path,
        scope.origin().clone(),
        scope.stream().clone(),
        scope.incarnation().clone(),
        scope.schema().clone(),
    )
    .unwrap()
}
fn main() {
    let args: Vec<String> = env::args().collect();
    assert!(
        args.len() >= 5,
        "usage: catalogue_apps APP SOURCE_DB RECEIVER_DB COMMAND [args]"
    );
    let selected = scope(&args[1]);
    let source_path = PathBuf::from(&args[2]);
    let receiver_path = PathBuf::from(&args[3]);
    match args[4].as_str() {
        "seed" => {
            let count: usize = args[5].parse().unwrap();
            let mut source = source(source_path, &selected);
            for number in 0..count {
                let key = id(&format!("entry-{number:04}"));
                let value = if args[1] == "tasks" {
                    format!("task-{number:04}:open")
                } else {
                    format!("session-{number:04}:hello")
                };
                source.upsert(&key, value.as_bytes()).unwrap();
            }
            println!("{{\"seeded\":{count}}}");
        }
        "upsert" => {
            let mut source = source(source_path, &selected);
            let revision = source.upsert(&id(&args[5]), args[6].as_bytes()).unwrap();
            println!("{{\"revision\":{revision}}}");
        }
        "delete" => {
            let mut source = source(source_path, &selected);
            let revision = source.delete(&id(&args[5])).unwrap();
            println!("{{\"revision\":{revision}}}");
        }
        "pass" => {
            let mut source = source(source_path, &selected);
            let mut store = SqliteCatalogueStore::open(receiver_path).unwrap();
            let mut auth = MemoryAuthorizer::allowed(selected.clone());
            let max_pages: usize = args
                .get(5)
                .map_or(usize::MAX, |value| value.parse().unwrap());
            let mut pages = 0;
            let mut pass = begin_or_resume(&selected, &mut auth, &mut source, &mut store).unwrap();
            while let Some(active) = pass {
                if pages >= max_pages {
                    break;
                }
                let progress =
                    apply_next_page(&active, 40, 1024 * 1024, &mut auth, &mut source, &mut store)
                        .unwrap();
                pages += 1;
                pass = progress.active;
            }
            let saved = store.progress(&selected).unwrap();
            let completed = saved.as_ref().map_or(0, |progress| progress.completed);
            let boundary = saved
                .as_ref()
                .and_then(|progress| progress.active.as_ref())
                .map_or(0, |pass| pass.boundary);
            let count = store.count(&selected).unwrap();
            println!("{{\"app\":\"{}\",\"pages\":{pages},\"completed\":{completed},\"active_boundary\":{boundary},\"count\":{count},\"head_reads\":{},\"manifest_reads\":{},\"resolve_reads\":{},\"manifest_bytes\":{},\"payload_bytes\":{}}}",
                args[1], source.head_reads(), source.manifest_reads(), source.resolve_reads(), source.manifest_bytes(), source.payload_bytes());
        }
        "show" => {
            let store = SqliteCatalogueStore::open(receiver_path).unwrap();
            let entry = store.cached_entry(&selected, &id(&args[5])).unwrap();
            match entry {
                Some(entry) => println!(
                    "{{\"known\":true,\"revision\":{},\"deleted\":{},\"payload\":{:?}}}",
                    entry.manifest.revision,
                    entry.manifest.deleted,
                    String::from_utf8_lossy(&entry.payload)
                ),
                None => println!("{{\"known\":false}}"),
            }
        }
        _ => panic!("unknown command"),
    }
}
