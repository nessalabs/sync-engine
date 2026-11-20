use super::{id, limits, scope, source, tune, Fixture, Server, MAX_RECORD_BYTES};
use nessa_sync::replication::{
    application::{
        begin_pass, step, step_cancellable, CancellablePageStep, PageStepAttempt,
        PageStepCancelResult, RecordSource, ReplicaStore, SourceError, StoreError, SyncError,
    },
    domain::{Checkpoint, CommitPlan, Limits, Page, PageRequest, Scope},
    infrastructure::{
        session::{connect_follow, FollowSession, TcpSessionIo, TcpSessionWriter},
        SqliteReplicaStore,
    },
};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(3);
const FIRST_BYTES: usize = 9001;

fn one_record() -> Limits {
    Limits::new(1, MAX_RECORD_BYTES, MAX_RECORD_BYTES).unwrap()
}

fn seed(fix: &Fixture) {
    let mut source = source(&fix.path("source.db"));
    for (id_str, bytes) in [
        ("one", vec![17; FIRST_BYTES]),
        ("two", b"second record".to_vec()),
        ("three", b"third record".to_vec()),
    ] {
        source.append(&id(id_str), &bytes).unwrap();
    }
}

fn opaque_pipe(mut from: TcpStream, mut toward: TcpStream) {
    let result = io::copy(&mut from, &mut toward);
    let _ = toward.shutdown(Shutdown::Write);
    if let Err(error) = result {
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ),
            "{error}"
        );
    }
}

struct Link {
    session: FollowSession<TcpSessionWriter>,
    relay: Option<JoinHandle<()>>,
}

impl Link {
    fn connect(server: &Server, relayed: bool) -> Self {
        let (address, relay) = if relayed {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let address = listener.local_addr().unwrap();
            let target = server.address;
            let relay = thread::spawn(move || forward(listener, target));
            (address, Some(relay))
        } else {
            (server.address, None)
        };
        let mut stream = TcpStream::connect(address).unwrap();
        tune(&stream);
        // Same disposable lab handshake as the existing direct fixture. Relay
        // opaque copies all bytes; this is not a TLS/production auth test.
        stream.write_all(&[42]).unwrap();
        let mut ack = [1];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack, [0]);
        let session =
            connect_follow(TcpSessionIo::new(stream).unwrap(), scope(), limits(), WAIT).unwrap();
        Self { session, relay }
    }

    fn finish(self) {
        self.session.close().unwrap();
        if let Some(relay) = self.relay {
            relay.join().unwrap();
        }
    }
}

fn forward(listener: TcpListener, target: SocketAddr) {
    let (incoming, _) = listener.accept().unwrap();
    let outgoing = TcpStream::connect(target).unwrap();
    for socket in [&incoming, &outgoing] {
        socket.set_nodelay(true).unwrap();
        socket.set_write_timeout(Some(WAIT)).unwrap();
        socket.set_read_timeout(Some(WAIT)).unwrap();
    }
    let incoming_read = incoming.try_clone().unwrap();
    let outgoing_read = outgoing.try_clone().unwrap();
    let up = thread::spawn(move || opaque_pipe(incoming_read, outgoing));
    opaque_pipe(outgoing_read, incoming);
    up.join().unwrap();
}

struct Gate {
    entered: SyncSender<()>,
    release: Receiver<()>,
}
impl Gate {
    fn wait(self) {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(WAIT).unwrap();
    }
}
fn gate() -> (Gate, Receiver<()>, SyncSender<()>) {
    let (entered, reached) = sync_channel(1);
    let (release, wait) = sync_channel(1);
    (
        Gate {
            entered,
            release: wait,
        },
        reached,
        release,
    )
}

struct HoldPage<'a, S>(&'a mut S, Option<Gate>);
impl<S: RecordSource> RecordSource for HoldPage<'_, S> {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.0.head(scope)
    }

    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        let page = self.0.page(request)?;
        self.1.take().unwrap().wait();
        Ok(page)
    }
}

struct ObserveApply<'a> {
    store: &'a mut SqliteReplicaStore,
    gate: Option<Gate>,
    lose_confirmation: bool,
}
impl ReplicaStore for ObserveApply<'_> {
    fn load(&mut self, scope: &Scope) -> Result<Option<Checkpoint>, StoreError> {
        self.store.load(scope)
    }

    fn apply(&mut self, plan: CommitPlan) -> Result<(), StoreError> {
        if let Some(gate) = self.gate.take() {
            gate.wait();
        }
        self.store.apply(plan)?;
        if self.lose_confirmation {
            // The real SQLite apply committed. Test the documented lost
            // confirmation boundary without changing the store's ownership.
            Err(StoreError::Uncertain)
        } else {
            Ok(())
        }
    }
}

#[test]
fn cancel_apply_and_uncertain_confirmation_have_distinct_real_follow_outcomes() {
    for relayed in [false, true] {
        let f = Fixture::new();
        seed(&f);
        let server = Server::new(f.path("source.db"), 2);
        let replica = f.path("replica.db");
        let link = Link::connect(&server, relayed);
        let (mut auth, mut records) = link.session.parts();
        let mut store = SqliteReplicaStore::open(&replica).unwrap();
        let budget = one_record();
        let mut pass = begin_pass(&scope(), &mut auth, &mut records, &mut store).unwrap();
        assert_eq!(pass.target(), 3);

        let (attempt, cancel) = PageStepAttempt::pair();
        let (gate, reached, release) = gate();
        thread::scope(|threads| {
            let worker = threads.spawn(|| {
                step_cancellable(
                    &mut pass,
                    budget,
                    &mut auth,
                    &mut HoldPage(&mut records, Some(gate)),
                    &mut store,
                    attempt,
                )
            });
            // The real, complete fragmented reply is at the local seam.
            reached.recv_timeout(WAIT).unwrap();
            assert_eq!(cancel.cancel(), PageStepCancelResult::CancelledBeforeApply);
            release.send(()).unwrap();
            assert_eq!(
                worker.join().unwrap(),
                Ok(CancellablePageStep::CancelledBeforeApply)
            );
        });
        assert_eq!(pass.position(), 0);
        drop(store);
        let mut store = SqliteReplicaStore::open(&replica).unwrap();
        assert_eq!(store.checkpoint(&scope()).unwrap(), None);
        assert!(store
            .read_after(&scope(), 0, 3, MAX_RECORD_BYTES)
            .unwrap()
            .is_empty());

        let (attempt, cancel) = PageStepAttempt::pair();
        let (apply_gate, reached, release) = self::gate();
        thread::scope(|threads| {
            let worker = threads.spawn(|| {
                step_cancellable(
                    &mut pass,
                    budget,
                    &mut auth,
                    &mut records,
                    &mut ObserveApply {
                        store: &mut store,
                        gate: Some(apply_gate),
                        lose_confirmation: false,
                    },
                    attempt,
                )
            });
            // A new authorized step uses the same Follow connection after
            // cancellation. The existing SQLite owner has been invoked.
            reached.recv_timeout(WAIT).unwrap();
            assert_eq!(cancel.cancel(), PageStepCancelResult::TooLate);
            release.send(()).unwrap();
            assert_eq!(
                worker.join().unwrap(),
                Ok(CancellablePageStep::Finished { complete: false })
            );
        });
        assert_eq!(pass.position(), 1);
        drop(store);
        let mut store = SqliteReplicaStore::open(&replica).unwrap();
        assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 1);

        let (attempt, cancel) = PageStepAttempt::pair();
        let outcome = step_cancellable(
            &mut pass,
            budget,
            &mut auth,
            &mut records,
            &mut ObserveApply {
                store: &mut store,
                gate: None,
                lose_confirmation: true,
            },
            attempt,
        );
        assert_eq!(outcome, Err(SyncError::Store(StoreError::Uncertain)));
        assert_eq!(cancel.cancel(), PageStepCancelResult::TooLate);
        assert_eq!(pass.position(), 1);
        drop(store);
        link.finish();

        let rebound = Link::connect(&server, relayed);
        let (mut auth, mut records) = rebound.session.parts();
        let mut reopened = SqliteReplicaStore::open(&replica).unwrap();
        let mut recovered = begin_pass(&scope(), &mut auth, &mut records, &mut reopened).unwrap();
        assert_eq!(recovered.position(), 2);
        assert!(step(
            &mut recovered,
            budget,
            &mut auth,
            &mut records,
            &mut reopened
        )
        .unwrap());
        let after = rebound.session.counters().unwrap();
        let (attempt, cancel) = PageStepAttempt::pair();
        assert_eq!(
            step_cancellable(
                &mut recovered,
                budget,
                &mut auth,
                &mut records,
                &mut reopened,
                attempt
            ),
            Ok(CancellablePageStep::Finished { complete: true })
        );
        assert_eq!(cancel.cancel(), PageStepCancelResult::FinishedWithoutApply);
        assert_eq!(rebound.session.counters().unwrap(), after);
        let got = reopened
            .read_after(&scope(), 0, 3, MAX_RECORD_BYTES)
            .unwrap();
        assert_eq!(
            got.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["one", "two", "three"]
        );
        assert_eq!(got[0].payload, vec![17; FIRST_BYTES]);
        rebound.finish();
        server.finish();
    }
}

#[test]
fn matched_plain_and_cancellable_steps_use_the_same_follow_wire_budget() {
    for relayed in [false, true] {
        let f = Fixture::new();
        seed(&f);
        let server = Server::new(f.path("source.db"), 2);
        let mut metrics = vec![];
        for cancellable in [false, true] {
            let link = Link::connect(&server, relayed);
            let (mut auth, mut records) = link.session.parts();
            let budget = one_record();
            let mut store = SqliteReplicaStore::open(f.path(if cancellable {
                "cancellable.db"
            } else {
                "plain.db"
            }))
            .unwrap();
            let mut pass = begin_pass(&scope(), &mut auth, &mut records, &mut store).unwrap();
            let before = link.session.counters().unwrap();
            if cancellable {
                let (never_start, cancel) = PageStepAttempt::pair();
                assert_eq!(cancel.cancel(), PageStepCancelResult::CancelledBeforeApply);
                assert_eq!(
                    step_cancellable(
                        &mut pass,
                        budget,
                        &mut auth,
                        &mut records,
                        &mut store,
                        never_start
                    ),
                    Ok(CancellablePageStep::CancelledBeforeApply)
                );
                assert_eq!(link.session.counters().unwrap(), before);
                let (attempt, _cancel) = PageStepAttempt::pair();
                assert_eq!(
                    step_cancellable(
                        &mut pass,
                        budget,
                        &mut auth,
                        &mut records,
                        &mut store,
                        attempt
                    ),
                    Ok(CancellablePageStep::Finished { complete: false })
                );
            } else {
                assert!(!step(&mut pass, budget, &mut auth, &mut records, &mut store).unwrap());
            }
            assert_eq!(store.checkpoint(&scope()).unwrap().unwrap().position(), 1);
            let after = link.session.counters().unwrap();
            metrics.push((
                after.wire.exchanges - before.wire.exchanges,
                after.wire.protocol_bytes - before.wire.protocol_bytes,
                after.wire.payload_bytes - before.wire.payload_bytes,
            ));
            link.finish();
        }
        assert_eq!(metrics[0], metrics[1]);
        assert_eq!(metrics[0].0, 2);
        assert_eq!(metrics[0].2, FIRST_BYTES as u64);
        eprintln!(
            "Local cancel opt-in/ordinary page, relayed={relayed}: matched {} exchanges / {} app protocol bytes / {} payload bytes",
            metrics[0].0, metrics[0].1, metrics[0].2,
        );
        server.finish();
    }
}
