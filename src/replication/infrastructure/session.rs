//! Bounded, experimental session control codec. These messages are untrusted
//! metadata, not authenticated grants or source responses. The host must still
//! authenticate its channel, authorize every operation, correlate replies and
//! validate domain values before apply. No socket, source, store or retry owner
//! is created here. After any decode/I/O error, discard the connection.

use std::io::{self, Read, Write};

use crate::replication::application::MAX_RECORD_BYTES;
use crate::replication::domain::{Id, Scope};

/// Largest control body accepted before allocating a receive buffer.
pub const MAX_CONTROL_BYTES: usize = 8 * 1024;
/// Smallest supported advertised control-body budget.
pub const MIN_CONTROL_BYTES: usize = 2 * 1024;
/// Largest advertised scope-binding count for this initial codec.
pub const MAX_SESSION_BINDINGS: u16 = 32;
const MAGIC: &[u8; 4] = b"NSC1";
const VERSION: u16 = 1;
// Only explicit, current per-operation authorization is supported. A binding
// never becomes an implementation of ScopeAuthorizer.
const PER_OPERATION: u8 = 1;

/// Proposed resource bounds. Public values are revalidated at each codec edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionLimits {
    /// Maximum control-body bytes, excluding the four-byte length prefix.
    pub control_bytes: u32,
    /// Maximum binding metadata entries a session owner may retain.
    pub bindings: u16,
}

impl SessionLimits {
    fn validate(self) -> Result<(), ControlError> {
        if !(MIN_CONTROL_BYTES..=MAX_CONTROL_BYTES).contains(&(self.control_bytes as usize))
            || self.bindings == 0
            || self.bindings > MAX_SESSION_BINDINGS
        {
            return Err(ControlError::InvalidLimits);
        }
        Ok(())
    }
}

/// Correlation metadata. IDs must be nonzero; uniqueness and freshness are
/// obligations of the session owner, not established by decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Correlation {
    /// Host-selected session namespace, bound to its authenticated channel.
    pub session: u64,
    /// Nonreused request ID within that session.
    pub request: u64,
}

/// Passive binding metadata. This value grants no authority and cannot advance
/// a checkpoint. The session owner must correlate exact scope and request first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingMetadata {
    /// Nonzero session-local scope handle.
    pub handle: u32,
    /// Nonzero binding generation retained with the exact handle by the lifecycle owner.
    pub generation: u64,
    /// Exact receiver/source identity and access epoch.
    pub scope: Scope,
    /// Advisory committed head observed by the authorized host.
    pub head: u64,
}

/// Initial control vocabulary only; no data, refusal, cancel or grant cache.
/// The encoded hello and welcome explicitly select per-operation authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlFrame {
    /// Offer supported limits. Host channel authentication is a prerequisite
    /// for acting on the offer, rather than a property of these bytes.
    Hello(SessionLimits),
    /// Return accepted bounds, session namespace and reference record ceiling.
    Welcome {
        /// Nonzero host-selected session namespace.
        session: u64,
        /// Accepted limits; the lifecycle owner compares these to the retained offer.
        limits: SessionLimits,
        /// Maximum source payload, no larger than the supported owner ceiling.
        record_bytes: u32,
    },
    /// Request binding context, without granting permission to read the scope.
    Bind {
        /// Exact session/request namespace.
        correlation: Correlation,
        /// Requested exact scope.
        scope: Scope,
    },
    /// Carry authorized host metadata. Decode alone does not prove authorization.
    Bound {
        /// Echoed session/request namespace.
        correlation: Correlation,
        /// Passive metadata to correlate before use.
        binding: BindingMetadata,
    },
}

/// Refusal of a control frame; no retry or recovery is performed by the codec.
#[derive(Debug)]
pub enum ControlError {
    /// Socket/reader/writer failure, including truncated frames.
    Io(io::Error),
    /// Announced or encoded body exceeds the caller's admitted budget.
    TooLarge,
    /// Caller or advertised bounds are outside this codec's hard limits.
    InvalidLimits,
    /// Unknown protocol version, operation or authorization mode.
    Incompatible,
    /// Invalid identity, zero correlation/handle/generation, or trailing bytes.
    Malformed,
}

impl From<io::Error> for ControlError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn budget(limit: usize) -> Result<(), ControlError> {
    if !(MIN_CONTROL_BYTES..=MAX_CONTROL_BYTES).contains(&limit) {
        return Err(ControlError::InvalidLimits);
    }
    Ok(())
}

fn put_limits(out: &mut Vec<u8>, limits: SessionLimits) -> Result<(), ControlError> {
    limits.validate()?;
    out.push(PER_OPERATION);
    out.extend_from_slice(&limits.control_bytes.to_be_bytes());
    out.extend_from_slice(&limits.bindings.to_be_bytes());
    Ok(())
}

fn put_correlation(out: &mut Vec<u8>, value: Correlation) -> Result<(), ControlError> {
    if value.session == 0 || value.request == 0 {
        return Err(ControlError::Malformed);
    }
    out.extend_from_slice(&value.session.to_be_bytes());
    out.extend_from_slice(&value.request.to_be_bytes());
    Ok(())
}

fn put_scope(out: &mut Vec<u8>, scope: &Scope) {
    for id in [
        scope.receiver(),
        scope.origin(),
        scope.stream(),
        scope.incarnation(),
        scope.schema(),
        scope.access_epoch(),
    ] {
        out.push(id.as_str().len() as u8);
        out.extend_from_slice(id.as_str().as_bytes());
    }
}

/// Write one bounded frame using one buffered `write_all`. Caller owns flushing,
/// deadlines and connection disposal on error. Invalid values emit no bytes.
pub fn write_control(
    writer: &mut impl Write,
    frame: &ControlFrame,
    limit: usize,
) -> Result<(), ControlError> {
    budget(limit)?;
    let mut body = Vec::from(MAGIC.as_slice());
    body.extend_from_slice(&VERSION.to_be_bytes());
    match frame {
        ControlFrame::Hello(limits) => {
            body.push(1);
            put_limits(&mut body, *limits)?;
        }
        ControlFrame::Welcome {
            session,
            limits,
            record_bytes,
        } => {
            if *session == 0 || *record_bytes == 0 || *record_bytes as usize > MAX_RECORD_BYTES {
                return Err(ControlError::Malformed);
            }
            body.push(2);
            body.extend_from_slice(&session.to_be_bytes());
            put_limits(&mut body, *limits)?;
            body.extend_from_slice(&record_bytes.to_be_bytes());
        }
        ControlFrame::Bind { correlation, scope } => {
            body.push(3);
            put_correlation(&mut body, *correlation)?;
            put_scope(&mut body, scope);
        }
        ControlFrame::Bound {
            correlation,
            binding,
        } => {
            if binding.handle == 0 || binding.generation == 0 {
                return Err(ControlError::Malformed);
            }
            body.push(4);
            put_correlation(&mut body, *correlation)?;
            body.extend_from_slice(&binding.handle.to_be_bytes());
            body.extend_from_slice(&binding.generation.to_be_bytes());
            put_scope(&mut body, &binding.scope);
            body.extend_from_slice(&binding.head.to_be_bytes());
        }
    }
    if body.len() > limit {
        return Err(ControlError::TooLarge);
    }
    let mut encoded = Vec::with_capacity(4 + body.len());
    encoded.extend_from_slice(&(body.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&body);
    writer.write_all(&encoded)?;
    Ok(())
}

struct Decoder<'a>(&'a [u8]);

impl<'a> Decoder<'a> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], ControlError> {
        let value = self.0.get(..N).ok_or(ControlError::Malformed)?;
        let value = value.try_into().map_err(|_| ControlError::Malformed)?;
        self.0 = &self.0[N..];
        Ok(value)
    }
    fn limits(&mut self) -> Result<SessionLimits, ControlError> {
        if self.take::<1>()? != [PER_OPERATION] {
            return Err(ControlError::Incompatible);
        }
        let limits = SessionLimits {
            control_bytes: u32::from_be_bytes(self.take()?),
            bindings: u16::from_be_bytes(self.take()?),
        };
        limits.validate()?;
        Ok(limits)
    }
    fn correlation(&mut self) -> Result<Correlation, ControlError> {
        let value = Correlation {
            session: u64::from_be_bytes(self.take()?),
            request: u64::from_be_bytes(self.take()?),
        };
        if value.session == 0 || value.request == 0 {
            return Err(ControlError::Malformed);
        }
        Ok(value)
    }
    fn id(&mut self) -> Result<Id, ControlError> {
        let length = self.take::<1>()?[0] as usize;
        let text = self.0.get(..length).ok_or(ControlError::Malformed)?;
        let id = Id::new(std::str::from_utf8(text).map_err(|_| ControlError::Malformed)?)
            .map_err(|_| ControlError::Malformed)?;
        self.0 = &self.0[length..];
        Ok(id)
    }
    fn scope(&mut self) -> Result<Scope, ControlError> {
        Ok(Scope::new(
            self.id()?,
            self.id()?,
            self.id()?,
            self.id()?,
            self.id()?,
            self.id()?,
        ))
    }
}

/// Read one frame, checking its announced length before allocating or reading
/// its body. Unknown vocabulary is incompatible; decoded metadata grants nothing.
/// Caller must discard the connection on any error, even if more bytes remain.
pub fn read_control(reader: &mut impl Read, limit: usize) -> Result<ControlFrame, ControlError> {
    budget(limit)?;
    let mut header = [0; 4];
    reader.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > limit {
        return Err(ControlError::TooLarge);
    }
    if length < 7 {
        return Err(ControlError::Malformed);
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    let mut decoder = Decoder(&body);
    if decoder.take::<4>()? != *MAGIC || u16::from_be_bytes(decoder.take()?) != VERSION {
        return Err(ControlError::Incompatible);
    }
    let frame = match decoder.take::<1>()?[0] {
        1 => ControlFrame::Hello(decoder.limits()?),
        2 => ControlFrame::Welcome {
            session: u64::from_be_bytes(decoder.take()?),
            limits: decoder.limits()?,
            record_bytes: u32::from_be_bytes(decoder.take()?),
        },
        3 => ControlFrame::Bind {
            correlation: decoder.correlation()?,
            scope: decoder.scope()?,
        },
        4 => ControlFrame::Bound {
            correlation: decoder.correlation()?,
            binding: BindingMetadata {
                handle: u32::from_be_bytes(decoder.take()?),
                generation: u64::from_be_bytes(decoder.take()?),
                scope: decoder.scope()?,
                head: u64::from_be_bytes(decoder.take()?),
            },
        },
        _ => return Err(ControlError::Incompatible),
    };
    if !decoder.0.is_empty() {
        return Err(ControlError::Malformed);
    }
    // Apply the same value admission to both directions. This bounded re-encode
    // performs no I/O and cannot turn metadata into an authority decision.
    write_control(&mut io::sink(), &frame, limit)?;
    Ok(frame)
}

mod lifecycle;
pub use lifecycle::{
    ConnectionId, LifecycleError, RequestIdentity, ScopeIdentity, SessionId, SessionLifecycle,
    SessionPhase,
};
