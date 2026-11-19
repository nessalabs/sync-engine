//! Host-owned I/O ownership and interruption, without a reader loop, listener,
//! authentication, hints or retry. The TCP adapter preserves caller socket options.
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Connection-specific interrupt capability, independent of reader/writer locks.
/// Implementations permanently fence future I/O before interrupting outstanding
/// I/O. Success means interruption was accepted, not that a DTO/commit was undone.
/// Errors must return promptly and remain errors on retry until resolved; a set
/// local fence alone cannot turn failed physical interruption into success.
/// A handle must never be retargeted to a replacement connection.
pub trait SessionInterrupt: Send + Sync {
    /// Fence and interrupt this connection. Repeated/concurrent calls are safe.
    fn interrupt(&self) -> io::Result<()>;
}
/// An owned channel with an independently callable connection interrupt handle.
/// The host authenticates it, configures finite I/O deadlines, and guarantees
/// that the handle addresses this exact channel without taking its I/O locks.
pub trait InterruptibleSessionIo: Read + Write {
    /// Retain before moving the I/O into a connecting/reading owner if that work
    /// must be externally interrupted. The handle may extend resource lifetime.
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt>;
}
/// Optional ownership prerequisite for a future single-reader dispatcher.
/// Splitting consumes the whole channel; no cloneable read owner is supplied.
/// This trait creates no background worker or reply/notification queues.
pub trait DuplexSessionIo: InterruptibleSessionIo + Sized {
    /// Sole read owner, transferable to the host's reader task.
    type Reader: Read + Send + 'static;
    /// Sole write owner, independent of blocking reads.
    type Writer: Write + Send + 'static;
    /// Consume the channel into its halves. Retain its interrupt handle first.
    /// On failure, release or interrupt retained handles explicitly.
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)>;
}
struct TcpInterrupt {
    socket: TcpStream,
    closed: AtomicBool,
}
impl TcpInterrupt {
    fn open(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(io::ErrorKind::NotConnected.into())
        } else {
            Ok(())
        }
    }
}
impl SessionInterrupt for TcpInterrupt {
    fn interrupt(&self) -> io::Result<()> {
        self.closed.store(true, Ordering::Release);
        // Always attempt the OS shutdown, including retries after errors. An
        // earlier local fence is not proof that shutdown actually succeeded.
        match self.socket.shutdown(Shutdown::Both) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            Err(error) => Err(error),
        }
    }
}
/// A configured, host-authenticated TCP channel with a permanent local I/O fence.
/// This adds no TLS/device trust and opens no socket. It changes no TCP options or
/// deadlines. One retained shutdown descriptor is shared by interrupt handles;
/// splitting adds one write descriptor. The I/O and its halves are not Clone.
/// Retained handles can keep the underlying resource alive; close explicitly.
pub struct TcpSessionIo {
    socket: TcpStream,
    interrupt: Arc<TcpInterrupt>,
}
impl TcpSessionIo {
    /// Adopt a connected stream, preserving its configured options/deadlines.
    pub fn new(socket: TcpStream) -> io::Result<Self> {
        let shutdown = socket.try_clone()?;
        Ok(Self {
            socket,
            interrupt: Arc::new(TcpInterrupt {
                socket: shutdown,
                closed: AtomicBool::new(false),
            }),
        })
    }
}
impl InterruptibleSessionIo for TcpSessionIo {
    fn interrupt_handle(&self) -> Arc<dyn SessionInterrupt> {
        self.interrupt.clone()
    }
}
impl Read for TcpSessionIo {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.interrupt.open()?;
        self.socket.read(out)
    }
}
impl Write for TcpSessionIo {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.interrupt.open()?;
        self.socket.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.interrupt.open()?;
        self.socket.flush()
    }
}
/// Unique TCP read half. A blocking read does not own the writer or interrupt.
pub struct TcpSessionReader {
    socket: TcpStream,
    interrupt: Arc<TcpInterrupt>,
}
/// Unique TCP write half, sharing only the channel's permanent interrupt fence.
pub struct TcpSessionWriter {
    socket: TcpStream,
    interrupt: Arc<TcpInterrupt>,
}
impl Read for TcpSessionReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.interrupt.open()?;
        self.socket.read(out)
    }
}
impl Write for TcpSessionWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.interrupt.open()?;
        self.socket.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.interrupt.open()?;
        self.socket.flush()
    }
}
impl DuplexSessionIo for TcpSessionIo {
    type Reader = TcpSessionReader;
    type Writer = TcpSessionWriter;
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
        let writer = self.socket.try_clone()?;
        Ok((
            TcpSessionReader {
                socket: self.socket,
                interrupt: self.interrupt.clone(),
            },
            TcpSessionWriter {
                socket: writer,
                interrupt: self.interrupt,
            },
        ))
    }
}
