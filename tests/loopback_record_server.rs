#![cfg(feature = "transport")]

use std::io::Write;
use std::net::{SocketAddrV4, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nessa_sync::replication::application::{RecordSource, ScopeAuthorizer, SourceError};
use nessa_sync::replication::domain::{Id, Page, PageRequest, Record, Scope};
use nessa_sync::replication::infrastructure::{
    read_frame, LoopbackClient, LoopbackReadConfig, LoopbackRecordServer, MemorySource, SourceFact,
    MAX_FRAME_BYTES, MAX_PAGE_PAYLOAD, MAX_PAGE_RECORDS,
};

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}
fn config() -> LoopbackReadConfig {
    LoopbackReadConfig {
        origin: id("origin"),
        stream: id("stream"),
        incarnation: id("incarnation"),
        schema: id("schema"),
        access_epoch: id("epoch"),
        read_token: id("read"),
        allowed_receivers: vec![id("receiver")],
    }
}
fn request() -> PageRequest {
    PageRequest {
        scope: scope(),
        after: 0,
        target: 1,
        max_records: MAX_PAGE_RECORDS,
        max_payload_bytes: MAX_PAGE_PAYLOAD,
        max_record_bytes: MAX_PAGE_PAYLOAD,
    }
}
fn run<S, F>(factory: F) -> (SocketAddrV4, Arc<AtomicUsize>)
where
    S: RecordSource + Send + 'static,
    F: Fn() -> Result<S, SourceError> + Send + Sync + 'static,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let server = LoopbackRecordServer::bind(0, config(), move || {
        counted.fetch_add(1, Ordering::SeqCst);
        factory()
    })
    .unwrap();
    let addr = match server.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    thread::spawn(move || server.serve().unwrap());
    (addr, calls)
}

#[test]
fn non_sqlite_source_round_trips_and_denials_never_open_it() {
    let mut source = MemorySource::new(id("origin"), id("stream"), id("incarnation"), id("schema"));
    source
        .append(SourceFact {
            id: id("fact"),
            payload: b"opaque".to_vec(),
        })
        .unwrap();
    let (addr, calls) = run(move || Ok(source.clone()));
    let mut bad = LoopbackClient::new(addr, id("wrong")).unwrap();
    assert!(matches!(
        bad.authorize(&scope()),
        nessa_sync::replication::application::Access::Denied
    ));
    assert_eq!(bad.head(&scope()), Err(SourceError::Unavailable));
    let mut client = LoopbackClient::new(addr, id("read")).unwrap();
    let wrong_receiver = Scope::new(
        id("stranger"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    );
    let wrong_epoch = Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("old"),
    );
    assert_eq!(client.head(&wrong_receiver), Err(SourceError::Unavailable));
    assert_eq!(client.head(&wrong_epoch), Err(SourceError::Unavailable));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(client.head(&scope()), Ok(1));
    let page = client.page(&request()).unwrap();
    assert_eq!(page.records[0].payload, b"opaque");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

struct Scripted {
    error: Option<SourceError>,
    oversized: bool,
    dropped: Arc<AtomicUsize>,
}
impl Drop for Scripted {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl RecordSource for Scripted {
    fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
        self.error.clone().map_or(Ok(1), Err)
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        Ok(Page {
            request: request.clone(),
            records: vec![Record {
                position: 1,
                id: id("fact"),
                scope: request.scope.clone(),
                payload: vec![
                    7;
                    if self.oversized {
                        MAX_PAGE_PAYLOAD + 1
                    } else {
                        MAX_PAGE_PAYLOAD
                    }
                ],
            }],
        })
    }
}

#[test]
fn typed_source_errors_and_oversized_reply_round_trip() {
    for error in [
        SourceError::Unavailable,
        SourceError::Pruned,
        SourceError::InvalidRequest,
        SourceError::IdentityChanged,
        SourceError::OversizedRecord,
    ] {
        let expected = error.clone();
        let (addr, _) = run(move || {
            Ok(Scripted {
                error: Some(error.clone()),
                oversized: false,
                dropped: Arc::new(AtomicUsize::new(0)),
            })
        });
        let mut client = LoopbackClient::new(addr, id("read")).unwrap();
        assert_eq!(client.head(&scope()), Err(expected.clone()));
        assert_eq!(client.page(&request()), Err(expected));
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let track = Arc::clone(&dropped);
    let (addr, _) = run(move || {
        Ok(Scripted {
            error: None,
            oversized: true,
            dropped: Arc::clone(&track),
        })
    });
    let mut client = LoopbackClient::new(addr, id("read")).unwrap();
    assert_eq!(client.page(&request()), Err(SourceError::OversizedRecord));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn source_is_dropped_before_socket_response_and_malformed_frames_do_not_open_it() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let track = Arc::clone(&dropped);
    let (addr, calls) = run(move || {
        Ok(Scripted {
            error: None,
            oversized: false,
            dropped: Arc::clone(&track),
        })
    });
    let mut client = LoopbackClient::new(addr, id("read")).unwrap();
    let page = client.page(&request()).unwrap();
    assert_eq!(page.records[0].payload.len(), MAX_PAGE_PAYLOAD);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let mut socket = TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket
        .write_all(&((MAX_FRAME_BYTES + 1) as u32).to_be_bytes())
        .unwrap();
    assert!(read_frame(&mut socket).is_err());
    let mut socket = TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket.write_all(&4u32.to_be_bytes()).unwrap();
    socket.write_all(&[2, 4]).unwrap();
    socket.shutdown(std::net::Shutdown::Write).unwrap();
    assert!(read_frame(&mut socket).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
enum BadReply {
    Count,
    Echo,
    Scope,
    Gap,
}
struct BadSource(BadReply);
impl RecordSource for BadSource {
    fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
        Ok(1)
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        let mut echoed = request.clone();
        if matches!(self.0, BadReply::Echo) {
            echoed.target += 1;
        }
        let mut record = Record {
            position: 1,
            id: id("fact"),
            scope: request.scope.clone(),
            payload: vec![1],
        };
        if matches!(self.0, BadReply::Scope) {
            record.scope = Scope::new(
                id("other"),
                id("origin"),
                id("stream"),
                id("incarnation"),
                id("schema"),
                id("epoch"),
            );
        }
        if matches!(self.0, BadReply::Gap) {
            record.position = 2;
        }
        let records = if matches!(self.0, BadReply::Count) {
            vec![record; MAX_PAGE_RECORDS + 1]
        } else {
            vec![record]
        };
        Ok(Page {
            request: echoed,
            records,
        })
    }
}

#[test]
fn malicious_source_replies_are_refused_before_serialization() {
    for (bad, expected) in [
        (BadReply::Count, SourceError::InvalidRequest),
        (BadReply::Echo, SourceError::IdentityChanged),
        (BadReply::Scope, SourceError::IdentityChanged),
        (BadReply::Gap, SourceError::InvalidRequest),
    ] {
        let (addr, _) = run(move || Ok(BadSource(bad.clone())));
        let mut client = LoopbackClient::new(addr, id("read")).unwrap();
        assert_eq!(client.page(&request()), Err(expected));
    }
}
