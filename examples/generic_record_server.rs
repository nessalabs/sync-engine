//! Process-level demonstration of a non-SQLite read-only source.

use std::net::{Ipv4Addr, SocketAddrV4};

use nessa_sync::replication::application::RecordSource;
use nessa_sync::replication::domain::{Id, PageRequest, Scope};
use nessa_sync::replication::infrastructure::{
    LoopbackClient, LoopbackReadConfig, LoopbackRecordServer, MemorySource, SourceFact,
    MAX_PAGE_PAYLOAD, MAX_PAGE_RECORDS,
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("first"),
        id("opaque"),
        id("epoch"),
    )
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = args.get(2).ok_or("port required")?.parse()?;
    match args.get(1).map(String::as_str) {
        Some("server") => {
            let mut source =
                MemorySource::new(id("origin"), id("stream"), id("first"), id("opaque"));
            source
                .append(SourceFact {
                    id: id("fact"),
                    payload: b"hello".to_vec(),
                })
                .map_err(|_| "append failed")?;
            let config = LoopbackReadConfig {
                origin: id("origin"),
                stream: id("stream"),
                incarnation: id("first"),
                schema: id("opaque"),
                access_epoch: id("epoch"),
                read_token: id("read"),
                allowed_receivers: vec![id("receiver")],
            };
            LoopbackRecordServer::bind(port, config, move || Ok(source.clone()))?.serve()?;
        }
        Some("client") => {
            let mut client =
                LoopbackClient::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port), id("read"))?;
            let head = client.head(&scope()).map_err(|_| "head failed")?;
            let page = client
                .page(&PageRequest {
                    scope: scope(),
                    after: 0,
                    target: head,
                    max_records: MAX_PAGE_RECORDS,
                    max_payload_bytes: MAX_PAGE_PAYLOAD,
                    max_record_bytes: MAX_PAGE_PAYLOAD,
                })
                .map_err(|_| "page failed")?;
            println!(
                "{{\"head\":{head},\"records\":{},\"payload\":\"{}\"}}",
                page.records.len(),
                String::from_utf8(page.records[0].payload.clone())?
            );
        }
        _ => return Err("use server or client".into()),
    }
    Ok(())
}
