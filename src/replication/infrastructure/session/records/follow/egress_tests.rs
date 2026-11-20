use super::tests::{bytes, fixture, hint, identity, packet};
use super::*;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

const WAIT: Duration = Duration::from_secs(2);
const PREFIX: usize = 7;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Failure {
    None,
    Write,
    Flush,
}

struct ShortWriter {
    out: Arc<Mutex<Vec<u8>>>,
    flushes: usize,
    interrupt: bool,
    failure: Failure,
    hold: Option<(SyncSender<()>, Receiver<()>)>,
}

impl ShortWriter {
    fn new(failure: Failure) -> Self {
        Self {
            out: Arc::new(Mutex::new(Vec::new())),
            flushes: 0,
            interrupt: true,
            failure,
            hold: None,
        }
    }
}

impl Write for ShortWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.interrupt {
            self.interrupt = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        let written = bytes.len().min(PREFIX);
        {
            let mut out = self.out.lock().unwrap();
            if self.failure == Failure::Write && !out.is_empty() {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            out.extend_from_slice(&bytes[..written]);
        }
        if let Some((prefix_written, drain)) = self.hold.take() {
            prefix_written.send(()).unwrap();
            drain.recv_timeout(WAIT).unwrap();
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        if self.failure == Failure::Flush {
            return Err(io::ErrorKind::TimedOut.into());
        }
        Ok(())
    }
}

#[test]
fn outer_wire_admits_and_flushes_one_complete_frame_through_real_follow_facade() {
    let request = packet(HEAD, Vec::new(), 0, 0);
    let golden = bytes(&request);
    let (shared, _peer, reader) = fixture();
    let writer = ShortWriter::new(Failure::None);
    let mut wire = Wire {
        io: Some(FollowIo {
            writer,
            shared: shared.clone(),
            current: Cursor::new(Vec::new()),
            deadline: WAIT,
            deferred_write_error: None,
        }),
        counters: WireCounters::default(),
        limit: 2048,
        record_bytes: MAX_RECORD_BYTES,
        next: Some(2),
    };
    write_packet(&mut wire, &request, 2048).unwrap();
    // The containing Wire flush is only a receipt check, not another send.
    wire.flush().unwrap();
    let facade = wire.io.as_ref().unwrap();
    assert_eq!(*facade.writer.out.lock().unwrap(), golden);
    assert_eq!(facade.writer.flushes, 1);
    assert_eq!(wire.counters.protocol_bytes, golden.len() as u64);
    let dispatch = shared.state.lock().unwrap();
    assert_eq!(dispatch.tx, golden.len() as u64);
    assert!(matches!(
        dispatch.rpc.as_ref().unwrap().expected,
        Expect::Data { kind, meta, .. } if kind == HEAD | REPLY && meta == request.meta
    ));
    drop(dispatch);
    drop(wire);
    reader.join().unwrap();
}

#[test]
fn actual_wire_counts_accepted_prefix_or_full_unflushed_frame_and_then_fences() {
    let request = packet(HEAD, Vec::new(), 0, 0);
    let golden = bytes(&request);
    for (failure, expected, error) in [
        (Failure::Write, &golden[..PREFIX], io::ErrorKind::BrokenPipe),
        (Failure::Flush, golden.as_slice(), io::ErrorKind::TimedOut),
    ] {
        let (shared, _peer, reader) = fixture();
        let writer = ShortWriter::new(failure);
        let out = writer.out.clone();
        let mut wire = Wire {
            io: Some(FollowIo {
                writer,
                shared: shared.clone(),
                current: Cursor::new(Vec::new()),
                deadline: WAIT,
                deferred_write_error: None,
            }),
            counters: WireCounters::default(),
            limit: 2048,
            record_bytes: MAX_RECORD_BYTES,
            next: Some(2),
        };
        assert!(matches!(
            write_packet(&mut wire, &request, 2048).and_then(|()| wire.flush().map_err(Into::into)),
            Err(RecordSessionError::Io(actual)) if actual.kind() == error
        ));
        assert_eq!(shared.error(), Some(FollowError::Unavailable));
        assert_eq!(wire.counters.protocol_bytes, expected.len() as u64);
        assert_eq!(shared.state.lock().unwrap().tx, expected.len() as u64);
        assert_eq!(*out.lock().unwrap(), expected);
        assert!(matches!(
            write_packet(&mut wire, &request, 2048),
            Err(RecordSessionError::Io(actual)) if actual.kind() == io::ErrorKind::NotConnected
        ));
        assert_eq!(*out.lock().unwrap(), expected);
        drop(wire);
        reader.join().unwrap();
    }
}

struct Allow;
impl ScopeAuthorizer for Allow {
    fn authorize(&mut self, scope: &Scope) -> Access {
        Access::Allowed(scope.clone())
    }
}

struct HintSource(SyncSender<()>);
impl RecordSource for HintSource {
    fn head(&mut self, _: &Scope) -> Result<u64, SourceError> {
        self.0.send(()).unwrap();
        Ok(9)
    }
    fn page(&mut self, _: &PageRequest) -> Result<Page, SourceError> {
        unreachable!("hint emits only a head")
    }
}

#[test]
fn held_data_frame_precedes_hint_or_fences_it_before_writer_unlock() {
    let response = packet(HEAD | REPLY, 4_u64.to_be_bytes().to_vec(), 8, 0);
    let golden = bytes(&response);
    for failure in [Failure::None, Failure::Write, Failure::Flush] {
        let response = Packet {
            payload: response.payload.clone(),
            ..response
        };
        let (admitted, prefix_written) = sync_channel(1);
        let (continue_data, drain) = sync_channel(1);
        let mut short = ShortWriter::new(failure);
        short.hold = Some((admitted, drain));
        let out = short.out.clone();
        let writer = Arc::new(Mutex::new(short));
        let stop = Arc::new(AtomicBool::new(false));
        let (hint_constructed, hint_waiting_for_writer) = sync_channel(1);
        let backend = Arc::new(Mutex::new(Backend {
            policy: Allow,
            factory: move |_: &Scope| Ok(HintSource(hint_constructed.clone())),
        }));
        let scope = identity().scope().clone();
        let hint_meta = Meta::for_scope(&identity(), FIRST_REQUEST);
        let limit = 2048;
        let bound = Arc::new(Mutex::new(Some((scope.clone(), hint_meta, limit))));
        let mut data = ServerIo {
            reader: Cursor::new(Vec::<u8>::new()),
            writer: writer.clone(),
            stop: stop.clone(),
            admit: send_admission(backend.clone(), bound, stop.clone()),
        };
        thread::scope(|threads| {
            let sending = threads.spawn(move || {
                write_packet(&mut data, &response, limit)?;
                data.flush()?;
                Ok::<_, RecordSessionError>(())
            });
            prefix_written.recv_timeout(WAIT).unwrap();
            let notifying =
                threads.spawn(|| emit(&backend, &writer, &stop, &scope, hint_meta, limit, None));
            hint_waiting_for_writer.recv_timeout(WAIT).unwrap();
            // Hint acquisition happened but cannot overtake a frame or flush.
            assert_eq!(*out.lock().unwrap(), golden[..PREFIX]);
            continue_data.send(()).unwrap();
            let data = sending.join().unwrap();
            let notified = notifying.join().unwrap().unwrap();
            match failure {
                Failure::None => {
                    data.unwrap();
                    assert_eq!(notified, Some(9));
                    let mut expected = golden.clone();
                    expected.extend(bytes(&hint()));
                    assert_eq!(*out.lock().unwrap(), expected);
                    assert_eq!(writer.lock().unwrap().flushes, 2);
                }
                Failure::Write | Failure::Flush => {
                    assert!(data.is_err());
                    assert_eq!(notified, None);
                    assert!(stop.load(Ordering::Acquire));
                    let expected = if failure == Failure::Write {
                        &golden[..PREFIX]
                    } else {
                        golden.as_slice()
                    };
                    assert_eq!(*out.lock().unwrap(), expected);
                }
            }
        });
    }
}

#[test]
fn a_client_send_admits_one_frame_not_a_buffered_batch() {
    let (shared, _peer, reader) = fixture();
    let writer = ShortWriter::new(Failure::None);
    let out = writer.out.clone();
    let mut facade = FollowIo {
        writer,
        shared: shared.clone(),
        current: Cursor::new(Vec::new()),
        deadline: WAIT,
        deferred_write_error: None,
    };
    let one = bytes(&packet(HEAD, Vec::new(), 0, 0));
    let pair = [one.as_slice(), one.as_slice()].concat();
    assert_eq!(
        facade.write_all(&pair).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(out.lock().unwrap().is_empty());
    let state = shared.state.lock().unwrap();
    assert!(state.rpc.is_none());
    assert_eq!(state.tx, 0);
    drop(state);
    drop(facade);
    reader.join().unwrap();
}

#[test]
fn hint_failure_fences_the_next_data_frame_under_the_same_writer() {
    for failure in [Failure::Write, Failure::Flush] {
        let short = ShortWriter::new(failure);
        let out = short.out.clone();
        let writer = Arc::new(Mutex::new(short));
        let stop = Arc::new(AtomicBool::new(false));
        let (constructed, waiting) = sync_channel(1);
        let backend = Arc::new(Mutex::new(Backend {
            policy: Allow,
            factory: move |_: &Scope| Ok(HintSource(constructed.clone())),
        }));
        let scope = identity().scope().clone();
        let hint_meta = Meta::for_scope(&identity(), FIRST_REQUEST);
        let limit = 2048;
        let bound = Arc::new(Mutex::new(Some((scope.clone(), hint_meta, limit))));
        assert!(emit(&backend, &writer, &stop, &scope, hint_meta, limit, None).is_err());
        waiting.recv_timeout(WAIT).unwrap();
        assert!(stop.load(Ordering::Acquire));
        let golden = bytes(&hint());
        let prefix = if failure == Failure::Write {
            &golden[..PREFIX]
        } else {
            golden.as_slice()
        };
        assert_eq!(*out.lock().unwrap(), prefix);

        let mut data = ServerIo {
            reader: Cursor::new(Vec::<u8>::new()),
            writer,
            stop: stop.clone(),
            admit: send_admission(backend, bound, stop),
        };
        assert!(matches!(
            write_packet(&mut data, &packet(HEAD | REPLY, Vec::new(), 0, 0), limit),
            Err(RecordSessionError::Io(error)) if error.kind() == io::ErrorKind::NotConnected
        ));
        assert_eq!(*out.lock().unwrap(), prefix);
    }
}

#[test]
fn next_client_frame_cannot_admit_over_unread_local_reply_bytes() {
    let (shared, mut peer, reader) = fixture();
    let writer = ShortWriter::new(Failure::None);
    let out = writer.out.clone();
    let request = packet(HEAD, Vec::new(), 0, 0);
    let mut facade = FollowIo {
        writer,
        shared: shared.clone(),
        current: Cursor::new(Vec::new()),
        deadline: WAIT,
        deferred_write_error: None,
    };
    write_packet(&mut facade, &request, 2048).unwrap();
    let original = bytes(&request);
    let reply = bytes(&packet(HEAD | REPLY, 4_u64.to_be_bytes().to_vec(), 8, 0));
    peer.write_all(&reply).unwrap();
    facade.read_exact(&mut [0_u8; 1]).unwrap();
    {
        let state = shared.state.lock().unwrap();
        assert!(state.rpc.as_ref().unwrap().complete);
        assert!(state.frame.is_none());
    }
    assert_eq!(facade.current.position(), 1);
    assert_eq!(facade.current.get_ref().len(), reply.len());
    let next = Packet {
        meta: Meta::for_scope(&identity(), 3),
        ..request
    };
    assert!(matches!(
        write_packet(&mut facade, &next, 2048),
        Err(RecordSessionError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    assert_eq!(*out.lock().unwrap(), original);
    assert_eq!(shared.error(), Some(FollowError::Protocol));
    drop(facade);
    reader.join().unwrap();
}

#[test]
fn later_hint_failure_cannot_rewrite_a_flushed_data_receipt() {
    let short = ShortWriter::new(Failure::None);
    let writer = Arc::new(Mutex::new(short));
    let stop = Arc::new(AtomicBool::new(false));
    let (constructed, waiting) = sync_channel(1);
    let backend = Arc::new(Mutex::new(Backend {
        policy: Allow,
        factory: move |_: &Scope| Ok(HintSource(constructed.clone())),
    }));
    let scope = identity().scope().clone();
    let hint_meta = Meta::for_scope(&identity(), FIRST_REQUEST);
    let limit = 2048;
    let bound = Arc::new(Mutex::new(Some((scope.clone(), hint_meta, limit))));
    let mut data = ServerIo {
        reader: Cursor::new(Vec::<u8>::new()),
        writer: writer.clone(),
        stop: stop.clone(),
        admit: send_admission(backend.clone(), bound, stop.clone()),
    };
    let response = packet(HEAD | REPLY, 4_u64.to_be_bytes().to_vec(), 8, 0);
    write_packet(&mut data, &response, limit).unwrap();
    assert_eq!(writer.lock().unwrap().flushes, 1);

    writer.lock().unwrap().failure = Failure::Flush;
    assert!(emit(&backend, &writer, &stop, &scope, hint_meta, limit, None).is_err());
    waiting.recv_timeout(WAIT).unwrap();
    assert!(stop.load(Ordering::Acquire));
    let mut accepted = bytes(&response);
    accepted.extend(bytes(&hint()));

    // This is the caller's redundant flush for the data which already
    // completed, not a new physical flush or admission check.
    data.flush().unwrap();
    assert_eq!(writer.lock().unwrap().flushes, 2);
    assert!(data.read(&mut [0_u8; 1]).is_err());
    assert!(write_packet(&mut data, &response, limit).is_err());
    assert_eq!(*writer.lock().unwrap().out.lock().unwrap(), accepted);
}
