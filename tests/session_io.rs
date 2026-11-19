#![cfg(feature = "transport")]
//! Trusted paired sockets exercise only channel ownership, not device trust or hints.
use nessa_sync::replication::{
    domain::{Id, Scope},
    infrastructure::session::{
        read_control, ControlFrame, DuplexSessionIo, InterruptibleSessionIo, RecordSession,
        SessionLimits, TcpSessionIo,
    },
};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (peer, _) = listener.accept().unwrap();
    for socket in [&client, &peer] {
        socket
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        socket.set_nodelay(true).unwrap();
    }
    (client, peer)
}
#[test]
fn split_reader_and_writer_progress_independently_and_share_terminal_fence() {
    let (client, mut peer) = pair();
    let channel = TcpSessionIo::new(client).unwrap();
    let interrupt = channel.interrupt_handle();
    let (mut reader, mut writer) = channel.split().unwrap();
    let read = thread::spawn(move || {
        let mut reply = [0; 4];
        reader.read_exact(&mut reply).unwrap();
        (reader, reply)
    });
    let server = thread::spawn(move || {
        let mut request = [0; 4];
        peer.read_exact(&mut request).unwrap();
        assert_eq!(&request, b"ping");
        peer.write_all(b"pong").unwrap();
        let mut end = [0];
        assert_eq!(peer.read(&mut end).unwrap(), 0);
    });
    writer.write_all(b"ping").unwrap();
    let (mut reader, reply) = read.join().unwrap();
    assert_eq!(&reply, b"pong");
    interrupt.interrupt().unwrap();
    interrupt.interrupt().unwrap();
    assert_eq!(
        reader.read(&mut [0]).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        writer.write(b"late").unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    server.join().unwrap();
}
#[test]
fn interrupt_unblocks_idle_split_reader_before_deadline() {
    let (client, peer) = pair();
    let channel = TcpSessionIo::new(client).unwrap();
    let interrupt = channel.interrupt_handle();
    let (mut reader, _writer) = channel.split().unwrap();
    let (tx, rx) = mpsc::channel();
    let (start, started) = mpsc::channel();
    let worker = thread::spawn(move || {
        start.send(()).unwrap();
        tx.send(reader.read(&mut [0])).unwrap()
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    interrupt.interrupt().unwrap();
    let result = rx.recv_timeout(Duration::from_secs(2));
    drop(peer);
    worker.join().unwrap();
    assert!(matches!(result, Ok(Ok(0)) | Ok(Err(_))));
}
#[test]
fn interrupt_stops_bounded_writer_when_peer_does_not_consume() {
    let (client, peer) = pair();
    let channel = TcpSessionIo::new(client).unwrap();
    let interrupt = channel.interrupt_handle();
    let (_reader, mut writer) = channel.split().unwrap();
    let (tx, rx) = mpsc::channel();
    let (ready, started) = mpsc::channel();
    let worker = thread::spawn(move || {
        let block = [0_u8; 64 * 1024];
        let result = (|| -> io::Result<()> {
            for index in 0..512 {
                writer.write_all(&block)?;
                if index == 3 {
                    ready.send(()).unwrap();
                }
            }
            Ok(())
        })();
        tx.send(result).unwrap()
    });
    let active = started.recv_timeout(Duration::from_secs(2));
    let early = rx.recv_timeout(Duration::from_millis(100));
    let waiting = matches!(early, Err(mpsc::RecvTimeoutError::Timeout));
    interrupt.interrupt().unwrap();
    let result = match early {
        Ok(r) => Ok(r),
        Err(_) => rx.recv_timeout(Duration::from_secs(2)),
    };
    drop(peer);
    worker.join().unwrap();
    assert!(active.is_ok());
    assert!(waiting, "bounded write finished before interruption");
    assert!(matches!(result, Ok(Err(_))));
}
#[test]
fn retained_interrupt_cancels_connecting_handshake_without_fallback() {
    let (client, mut peer) = pair();
    let channel = TcpSessionIo::new(client).unwrap();
    let interrupt = channel.interrupt_handle();
    let (ready, started) = mpsc::channel();
    let server = thread::spawn(move || {
        assert!(matches!(
            read_control(&mut peer, 2048).unwrap(),
            ControlFrame::Open { .. }
        ));
        ready.send(()).unwrap();
        let mut next = [0];
        assert_eq!(peer.read(&mut next).unwrap(), 0)
    });
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let id = || Id::new("fixture").unwrap();
        let result = RecordSession::connect_open_interruptible(
            channel,
            Scope::new(id(), id(), id(), id(), id(), id()),
            SessionLimits {
                control_bytes: 2048,
                bindings: 1,
            },
        );
        tx.send(result.is_err()).unwrap()
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    interrupt.interrupt().unwrap();
    let refused = rx.recv_timeout(Duration::from_secs(2));
    worker.join().unwrap();
    server.join().unwrap();
    assert!(refused.unwrap());
}
