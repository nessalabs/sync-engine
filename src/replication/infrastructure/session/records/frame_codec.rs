//! Bounded acquisition of one length-prefixed record packet.
//!
//! The prefix is checked before any body bytes are retained. Each read is
//! capped at the bytes remaining in this packet, so the next packet stays on
//! the stream. Only an idle, completely empty reader may wait through timeouts.

use std::io::{self, Read};

use super::{RecordSessionError, HEADER, MAX_CONTROL_BYTES};

pub(super) fn read_frame(
    reader: &mut impl Read,
    limit: usize,
    idle: bool,
) -> Result<Option<Vec<u8>>, RecordSessionError> {
    let mut prefix = [0_u8; 4];
    let mut prefix_len = 0;
    let mut length = None;
    let mut body = Vec::new();
    let mut chunk = [0_u8; 4096];

    loop {
        let wanted = match length {
            None if prefix_len == 0 => 1,
            None => prefix.len() - prefix_len,
            Some(total) if body.len() == total => return Ok(Some(body)),
            Some(total) => (total - body.len()).min(chunk.len()),
        };
        let n = match reader.read(&mut chunk[..wanted]) {
            Ok(0) if prefix_len == 0 => return Ok(None),
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e)
                if idle
                    && prefix_len == 0
                    && matches!(
                        e.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
            {
                if e.kind() == io::ErrorKind::WouldBlock {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if length.is_none() {
            prefix[prefix_len..prefix_len + n].copy_from_slice(&chunk[..n]);
            prefix_len += n;
            if prefix_len == prefix.len() {
                let announced = u32::from_be_bytes(prefix) as usize;
                if announced > limit || announced > MAX_CONTROL_BYTES {
                    return Err(RecordSessionError::TooLarge);
                }
                if announced < HEADER {
                    return Err(RecordSessionError::Protocol);
                }
                length = Some(announced);
            }
        } else {
            body.extend_from_slice(&chunk[..n]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct Fragmented<R> {
        inner: R,
        max: usize,
    }
    impl<R: Read> Read for Fragmented<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let count = buf.len().min(self.max);
            self.inner.read(&mut buf[..count])
        }
    }

    #[test]
    fn fragmented_and_coalesced_frames_preserve_boundaries() {
        let mut bytes = (HEADER as u32).to_be_bytes().to_vec();
        bytes.extend((0..HEADER).map(|i| i as u8));
        let first = bytes.clone();
        bytes.extend_from_slice(&first);
        for max in [1, 2, 3, 7, 4096] {
            let mut reader = Fragmented {
                inner: Cursor::new(&bytes),
                max,
            };
            for _ in 0..2 {
                assert_eq!(
                    read_frame(&mut reader, HEADER, false).unwrap().unwrap(),
                    first[4..]
                );
            }
            assert!(read_frame(&mut reader, HEADER, false).unwrap().is_none());
        }
    }

    #[test]
    fn large_frame_reads_stop_at_its_boundary() {
        for length in [4097, MAX_CONTROL_BYTES] {
            let mut bytes = (length as u32).to_be_bytes().to_vec();
            bytes.resize(4 + length, 0x5a);
            bytes.extend_from_slice(b"next");
            let mut reader = Cursor::new(bytes);
            let body = read_frame(&mut reader, length, false).unwrap().unwrap();
            assert_eq!(body, vec![0x5a; length]);
            assert_eq!(reader.position(), (4 + length) as u64);
            let mut next = [0; 4];
            reader.read_exact(&mut next).unwrap();
            assert_eq!(&next, b"next");
        }
    }

    #[test]
    fn empty_eof_is_clean_but_every_partial_frame_is_truncated() {
        let mut bytes = (HEADER as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0; HEADER]);
        assert!(read_frame(&mut Cursor::new(&[]), HEADER, false)
            .unwrap()
            .is_none());
        for end in 1..bytes.len() {
            let result = read_frame(&mut Cursor::new(&bytes[..end]), HEADER, false);
            assert!(
                matches!(result, Err(RecordSessionError::Io(ref e)) if e.kind() == io::ErrorKind::UnexpectedEof),
                "cut at {end}"
            );
        }
    }

    #[test]
    fn bad_lengths_are_refused_before_body_read() {
        for (announced, limit, too_large) in [
            ((HEADER - 1) as u32, HEADER, false),
            ((HEADER + 1) as u32, HEADER, true),
            ((MAX_CONTROL_BYTES + 1) as u32, usize::MAX, true),
        ] {
            let mut bytes = announced.to_be_bytes().to_vec();
            bytes.extend_from_slice(b"unread");
            let mut reader = Cursor::new(bytes);
            let result = read_frame(&mut reader, limit, false);
            if too_large {
                assert!(matches!(result, Err(RecordSessionError::TooLarge)));
            } else {
                assert!(matches!(result, Err(RecordSessionError::Protocol)));
            }
            assert_eq!(reader.position(), 4);
        }
        let mut bytes = (MAX_CONTROL_BYTES as u32).to_be_bytes().to_vec();
        bytes.resize(4 + MAX_CONTROL_BYTES, 7);
        assert_eq!(
            read_frame(&mut Cursor::new(bytes), MAX_CONTROL_BYTES, false)
                .unwrap()
                .unwrap()
                .len(),
            MAX_CONTROL_BYTES
        );
    }

    #[test]
    fn idle_wait_and_partial_frame_timeouts_keep_existing_semantics() {
        struct FaultReader {
            bytes: Cursor<Vec<u8>>,
            first_error: Option<io::ErrorKind>,
            fail_after: Option<u64>,
        }
        impl Read for FaultReader {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if let Some(kind) = self.first_error.take() {
                    return Err(io::Error::from(kind));
                }
                if self.fail_after == Some(self.bytes.position()) {
                    return Err(io::Error::from(io::ErrorKind::TimedOut));
                }
                let cap = if self.fail_after.is_some() {
                    1
                } else {
                    out.len()
                };
                let count = out.len().min(cap);
                self.bytes.read(&mut out[..count])
            }
        }
        let mut frame = (HEADER as u32).to_be_bytes().to_vec();
        frame.resize(4 + HEADER, 9);
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::TimedOut,
            io::ErrorKind::WouldBlock,
        ] {
            let mut reader = FaultReader {
                bytes: Cursor::new(frame.clone()),
                first_error: Some(kind),
                fail_after: None,
            };
            assert_eq!(
                read_frame(&mut reader, HEADER, true)
                    .unwrap()
                    .unwrap()
                    .len(),
                HEADER
            );
        }
        for offset in [1, 4, 5] {
            let mut reader = FaultReader {
                bytes: Cursor::new(frame.clone()),
                first_error: None,
                fail_after: Some(offset),
            };
            assert!(
                matches!(read_frame(&mut reader, HEADER, true), Err(RecordSessionError::Io(ref e)) if e.kind() == io::ErrorKind::TimedOut)
            );
        }
    }

    #[test]
    fn interruption_is_retried_and_would_block_is_returned_after_prefix() {
        struct FaultAt {
            bytes: Cursor<Vec<u8>>,
            offset: u64,
            error: Option<io::ErrorKind>,
        }
        impl Read for FaultAt {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if self.bytes.position() == self.offset {
                    if let Some(kind) = self.error.take() {
                        return Err(kind.into());
                    }
                }
                let cap = out.len().min(1);
                self.bytes.read(&mut out[..cap])
            }
        }
        let mut frame = (HEADER as u32).to_be_bytes().to_vec();
        frame.resize(4 + HEADER, 7);
        for offset in [1, 4, 5] {
            let mut interrupted = FaultAt {
                bytes: Cursor::new(frame.clone()),
                offset,
                error: Some(io::ErrorKind::Interrupted),
            };
            assert_eq!(
                read_frame(&mut interrupted, HEADER, true).unwrap().unwrap(),
                vec![7; HEADER]
            );
            let mut blocked = FaultAt {
                bytes: Cursor::new(frame.clone()),
                offset,
                error: Some(io::ErrorKind::WouldBlock),
            };
            assert!(
                matches!(read_frame(&mut blocked, HEADER, true), Err(RecordSessionError::Io(ref e)) if e.kind() == io::ErrorKind::WouldBlock)
            );
        }
    }
}
