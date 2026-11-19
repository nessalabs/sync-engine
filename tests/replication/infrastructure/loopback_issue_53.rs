//! Benign fixture regressions for issue #53.
use super::*;
use std::net::SocketAddr;
use std::time::Instant;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}

fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("first"),
        id("schema"),
        id("epoch"),
    )
}

fn config(path: PathBuf) -> LoopbackConfig {
    LoopbackConfig {
        source_path: path,
        catalogue_source_path: Some(PathBuf::new()),
        artifact_source_path: None,
        catalogue_fault: None,
        origin: id("origin"),
        stream: id("stream"),
        incarnation: id("first"),
        schema: id("schema"),
        access_epoch: id("epoch"),
        read_token: id("read"),
        write_token: id("write"),
        allowed_receivers: vec![id("receiver")],
    }
}

fn listener() -> (TcpListener, SocketAddrV4) {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = match listener.local_addr().unwrap() {
        SocketAddr::V4(address) => address,
        _ => unreachable!(),
    };
    (listener, address)
}

fn real_client(config: LoopbackConfig, token: &str) -> (LoopbackClient, thread::JoinHandle<()>) {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        serve_one(
            socket,
            &config,
            &(Mutex::new(0), Condvar::new()),
            &ServerCounters::default(),
        )
        .unwrap();
    });
    (LoopbackClient::new(address, id(token)).unwrap(), server)
}

fn page_request() -> PageRequest {
    PageRequest {
        scope: scope(),
        after: 0,
        target: 1,
        max_records: 1,
        max_payload_bytes: MAX_RECORD_BYTES,
        max_record_bytes: MAX_RECORD_BYTES,
    }
}

fn tail_request() -> TailRequest {
    TailRequest {
        scope: scope(),
        generation: 1,
        max_records: 1,
        max_payload_bytes: MAX_RECORD_BYTES,
        max_record_bytes: MAX_RECORD_BYTES,
    }
}

#[test]
fn append_ceiling_round_trips_page_and_tail() {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("issue53-{}-{unique}.db", std::process::id()));
    let config = config(path.clone());
    {
        let mut source = config.open_source().unwrap();
        assert!(source
            .append(&id("large"), &vec![0; MAX_RECORD_BYTES + 1])
            .is_err());
        assert_eq!(
            source.append(&id("exact"), &vec![1; MAX_RECORD_BYTES]),
            Ok(1)
        );
    }
    let (mut client, server) = real_client(config.clone(), "read");
    let page = client.page(&page_request()).unwrap();
    assert_eq!(page.records[0].payload, vec![1; MAX_RECORD_BYTES]);
    server.join().unwrap();
    let (mut client, server) = real_client(config, "read");
    let tail = client.tail(&tail_request()).unwrap();
    assert_eq!(tail.records[0].payload, vec![1; MAX_RECORD_BYTES]);
    server.join().unwrap();
    std::fs::remove_file(path).unwrap();
}

// Normalize only the value type; preserve the public error variant in every family.
fn public_result(client: &mut LoopbackClient, operation: u8) -> Result<(), String> {
    let pass = CataloguePass {
        scope: scope(),
        completed: 0,
        boundary: 1,
        cursor: None,
        generation: 1,
    };
    match operation {
        1 => match client.authorize(&scope()) {
            Access::Allowed(_) => Ok(()),
            error => Err(format!("{error:?}")),
        },
        2 => RecordSource::head(client, &scope())
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        3 => client
            .page(&page_request())
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        7 => client
            .tail(&tail_request())
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        8 => client
            .older(&OlderRequest {
                scope: scope(),
                generation: 1,
                before: 2,
                max_records: 1,
                max_payload_bytes: 128,
                max_record_bytes: 128,
            })
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        11 => CatalogueSource::head(client, &scope())
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        12 => CatalogueSource::manifest(
            client,
            &ManifestRequest {
                pass,
                max_entries: 1,
            },
        )
        .map(|_| ())
        .map_err(|e| format!("{e:?}")),
        13 => client
            .resolve(&pass, &id("entry"), 128)
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        _ => unreachable!(),
    }
}

#[test]
fn public_refusal_matrix_precedes_source_open() {
    for mismatch in ["epoch", "incarnation", "schema", "token", "receiver"] {
        for operation in [1, 2, 3, 7, 8, 11, 12, 13] {
            // Empty fixture paths avoid referencing user data. The generic-server
            // factory-count regression separately verifies source acquisition ordering.
            let mut config = config(PathBuf::new());
            match mismatch {
                "epoch" => config.access_epoch = id("new"),
                "incarnation" => config.incarnation = id("new"),
                "schema" => config.schema = id("new"),
                "receiver" => config.allowed_receivers.clear(),
                _ => {}
            }
            let denied = matches!(mismatch, "token" | "receiver");
            let (mut client, server) =
                real_client(config, if mismatch == "token" { "bad" } else { "read" });
            assert_eq!(
                public_result(&mut client, operation),
                Err(if denied { "Denied" } else { "IdentityChanged" }.to_owned()),
                "{mismatch}/{operation}"
            );
            server.join().unwrap();
        }
    }
}

#[test]
fn temporary_unavailability_remains_retryable_on_every_public_read() {
    for operation in [1, 2, 3, 7, 8, 11, 12, 13] {
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_frame(&mut socket).unwrap();
            write_frame(&mut socket, &[2]).unwrap();
        });
        let mut client = LoopbackClient::new(address, id("read")).unwrap();
        let expected = if operation == 1 {
            "Unverifiable"
        } else {
            "Unavailable"
        };
        assert_eq!(
            public_result(&mut client, operation),
            Err(expected.to_owned())
        );
        server.join().unwrap();
    }
}

#[test]
fn unsupported_status_is_non_retryable_on_every_public_read() {
    for operation in [1, 2, 3, 7, 8, 11, 12, 13] {
        let (listener, address) = listener();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            read_frame(&mut socket).unwrap();
            write_frame(&mut socket, &[255]).unwrap();
        });
        let mut client = LoopbackClient::new(address, id("read")).unwrap();
        assert_eq!(
            public_result(&mut client, operation),
            Err(if operation == 1 {
                "InvalidResponse"
            } else {
                "InvalidRequest"
            }
            .to_owned())
        );
        server.join().unwrap();
    }
}

#[test]
fn unsupported_status_does_not_poison_next_exchange() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        for reply in [vec![255], vec![0, 0, 0, 0, 0, 0, 0, 0, 1]] {
            let (mut socket, _) = listener.accept().unwrap();
            read_frame(&mut socket).unwrap();
            write_frame(&mut socket, &reply).unwrap();
        }
    });
    let mut client = LoopbackClient::new(address, id("read")).unwrap();
    assert_eq!(
        RecordSource::head(&mut client, &scope()),
        Err(SourceError::InvalidRequest)
    );
    assert_eq!(RecordSource::head(&mut client, &scope()), Ok(1));
    server.join().unwrap();
}

#[test]
fn subscribe_times_out_before_ack() {
    let (listener, address) = listener();
    let server = thread::spawn(move || {
        let (_socket, _) = listener.accept().unwrap();
        thread::sleep(IO_TIMEOUT + Duration::from_secs(2));
    });
    let mut client = LoopbackClient::new(address, id("read")).unwrap();
    let start = Instant::now();
    assert!(matches!(client.subscribe(&scope()), Err(FrameError::Io(_))));
    assert!(start.elapsed() < IO_TIMEOUT + Duration::from_secs(1));
    server.join().unwrap();
}

#[test]
fn frame_uses_one_write_for_header_and_body() {
    #[derive(Default)]
    struct Writer {
        calls: usize,
        bytes: Vec<u8>,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer::default();
    write_frame(&mut writer, b"hello").unwrap();
    assert_eq!(writer.calls, 1);
    assert_eq!(writer.bytes, b"\0\0\0\x05hello");
}
