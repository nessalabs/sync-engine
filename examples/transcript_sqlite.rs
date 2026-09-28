//! Small, separate-process transcript lab. Run with `--features sqlite`.
//! The source is a reference adapter, and the policy grant is a local fixture.

use std::path::PathBuf;

use nessa_sync::replication::application::{begin_pass, finish_pass};
use nessa_sync::replication::domain::{Id, Limits, Scope};
use nessa_sync::replication::infrastructure::{
    MemoryAuthorizer, SqliteReferenceSource, SqliteReplicaStore,
};

fn id(value: &str) -> Id {
    Id::new(value).expect("static fixture identity")
}

fn scope(receiver: &str) -> Result<Scope, String> {
    Ok(Scope::new(
        Id::new(receiver).map_err(|error| format!("invalid receiver: {error:?}"))?,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("utf8-v1"),
        id("local-grant-1"),
    ))
}

fn source(path: PathBuf) -> Result<SqliteReferenceSource, String> {
    SqliteReferenceSource::open(
        path,
        id("example-origin"),
        id("transcript"),
        id("first"),
        id("utf8-v1"),
    )
    .map_err(|error| error.to_string())
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command, source_path, fact_id, text] if command == "append" => {
            if text.contains(['\t', '\n', '\r']) {
                return Err("example text must be one line without tabs".into());
            }
            let mut source = source(PathBuf::from(source_path))?;
            let fact_id = Id::new(fact_id.clone()).map_err(|error| format!("ID: {error:?}"))?;
            let position = source
                .append(&fact_id, text.as_bytes())
                .map_err(|error| format!("append: {error:?}"))?;
            println!("{{\"position\":{position}}}");
        }
        [command, source_path, replica_path, receiver] if command == "sync" => {
            let scope = scope(receiver)?;
            let mut source = source(PathBuf::from(source_path))?;
            let mut replica =
                SqliteReplicaStore::open(replica_path).map_err(|error| error.to_string())?;
            let before = replica
                .checkpoint(&scope)
                .map_err(|error| format!("checkpoint: {error:?}"))?
                .map_or(0, |checkpoint| checkpoint.position());
            let mut policy = MemoryAuthorizer::allowed(scope.clone());
            let mut pass = begin_pass(&scope, &mut policy, &mut source, &mut replica)
                .map_err(|error| format!("begin: {error:?}"))?;
            finish_pass(
                &mut pass,
                Limits::new(2, 4096, 4096).expect("static limits"),
                &mut policy,
                &mut source,
                &mut replica,
            )
            .map_err(|error| format!("apply: {error:?}"))?;
            let applied = pass.position() - before;
            println!(
                "{{\"before\":{before},\"after\":{},\"applied_records\":{applied},\"head_reads\":{},\"page_reads\":{},\"payload_bytes\":{}}}",
                pass.position(),
                source.head_reads(),
                source.page_reads(),
                source.payload_bytes()
            );
        }
        [command, replica_path, receiver] if command == "show" => {
            let scope = scope(receiver)?;
            let mut replica =
                SqliteReplicaStore::open(replica_path).map_err(|error| error.to_string())?;
            let checkpoint = replica
                .checkpoint(&scope)
                .map_err(|error| format!("checkpoint: {error:?}"))?
                .map_or(0, |saved| saved.position());
            println!("checkpoint\t{checkpoint}");
            let mut after = 0;
            loop {
                let records = replica
                    .read_after(&scope, after, 32, 4096)
                    .map_err(|error| format!("cached read: {error:?}"))?;
                if records.is_empty() {
                    break;
                }
                for record in records {
                    let text = String::from_utf8(record.payload)
                        .map_err(|_| "cached payload is not UTF-8".to_string())?;
                    println!("{}\t{}\t{text}", record.position, record.id.as_str());
                    after = record.position;
                }
            }
        }
        _ => {
            return Err("usage: transcript_sqlite append SOURCE ID TEXT | sync SOURCE REPLICA RECEIVER | show REPLICA RECEIVER".into());
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
