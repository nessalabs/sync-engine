//! Versioned, passive head compatibility metadata. No authority or progress effect.
use super::Scope;

/// Metadata contract version for [`SourceHead`]; independent of transport versions.
pub const SOURCE_HEAD_VERSION: u16 = 1;
/// Reader features currently implemented by this core. No optional bits are assigned yet.
pub const KNOWN_READER_CAPABILITIES: u64 = 0;
/// Writer features currently implemented by this core. Host schema bits are not core bits.
pub const KNOWN_WRITER_CAPABILITIES: u64 = 0;

/// Untrusted advertised metadata for one exact receiving/source scope.
/// Only the position is a potential finite boundary; metadata grants no access
/// and cannot acknowledge apply. Validate before using its position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceHead {
    scope: Scope,
    version: u16,
    position: u64,
    reader_required: u64,
    writer_required: u64,
}
impl SourceHead {
    /// Advertise this metadata version with explicit reader/writer requirements.
    pub fn new(scope: Scope, position: u64, reader_required: u64, writer_required: u64) -> Self {
        Self::advertised(
            scope,
            SOURCE_HEAD_VERSION,
            position,
            reader_required,
            writer_required,
        )
    }
    /// Retain received metadata without trusting its version, scope or bits.
    /// Transport adapters must independently correlate the complete wire echo.
    pub fn advertised(
        scope: Scope,
        version: u16,
        position: u64,
        reader_required: u64,
        writer_required: u64,
    ) -> Self {
        Self {
            scope,
            version,
            position,
            reader_required,
            writer_required,
        }
    }
    /// Exact advertised scope, including receiver and authorization epoch.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Advertised metadata version; an unsupported version is incompatible.
    pub fn version(&self) -> u16 {
        self.version
    }
    /// Committed position/revision, never a receiver checkpoint receipt.
    pub fn position(&self) -> u64 {
        self.position
    }
    /// Features every reader of this source incarnation must understand.
    pub fn reader_required(&self) -> u64 {
        self.reader_required
    }
    /// Additional features writers must understand; read-only consumers ignore these.
    pub fn writer_required(&self) -> u64 {
        self.writer_required
    }
}

/// Typed incompatibility; none of these outcomes implies reset or cache deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityError {
    /// Advertised head metadata uses an unsupported version.
    UnsupportedVersion(u16),
    /// Required reader bits are not supported by this consumer.
    MissingReader(u64),
    /// Required writer bits are not supported by a writing consumer.
    MissingWriter(u64),
    /// A support declaration claims undefined core features.
    InvalidSupport {
        /// Unsupported declared reader bits.
        reader: u64,
        /// Unsupported declared writer bits.
        writer: u64,
    },
}

/// Implemented core support. Construction cannot claim undefined feature bits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapabilitySupport {
    reader: u64,
    writer: u64,
}
impl CapabilitySupport {
    /// Support supplied by this core version, without speculative future features.
    pub const fn current() -> Self {
        Self {
            reader: KNOWN_READER_CAPABILITIES,
            writer: KNOWN_WRITER_CAPABILITIES,
        }
    }
    /// Reader support declared by this consumer; never includes undefined bits.
    pub fn reader_bits(&self) -> u64 {
        self.reader
    }
    /// Validate version and reader requirements independently of scope authority.
    pub fn validate_reader(self, version: u16, reader: u64) -> Result<(), CapabilityError> {
        if version != SOURCE_HEAD_VERSION {
            return Err(CapabilityError::UnsupportedVersion(version));
        }
        let missing = reader & !self.reader;
        if missing != 0 {
            Err(CapabilityError::MissingReader(missing))
        } else {
            Ok(())
        }
    }
    /// Validate source-writer format requirements; this never grants write authority.
    pub fn validate_writer(
        self,
        version: u16,
        reader: u64,
        writer: u64,
    ) -> Result<(), CapabilityError> {
        self.validate_reader(version, reader)?;
        let missing = writer & !self.writer;
        if missing != 0 {
            Err(CapabilityError::MissingWriter(missing))
        } else {
            Ok(())
        }
    }
    /// Select a subset of implemented features. Unknown declarations are refused.
    pub fn new(reader: u64, writer: u64) -> Result<Self, CapabilityError> {
        let unknown_reader = reader & !KNOWN_READER_CAPABILITIES;
        let unknown_writer = writer & !KNOWN_WRITER_CAPABILITIES;
        if unknown_reader != 0 || unknown_writer != 0 {
            Err(CapabilityError::InvalidSupport {
                reader: unknown_reader,
                writer: unknown_writer,
            })
        } else {
            Ok(Self { reader, writer })
        }
    }
}
/// Check read-only compatibility. Writer-only requirements never block this path.
/// Scope correlation and current authorization are separate obligations.
pub fn validate_capabilities(
    head: &SourceHead,
    supported: CapabilitySupport,
) -> Result<(), CapabilityError> {
    supported.validate_reader(head.version, head.reader_required)
}
/// Check compatibility for a writer which must understand both requirement sets.
/// Success is not permission to write and supplies no source mutation port.
pub fn validate_writer_capabilities(
    head: &SourceHead,
    supported: CapabilitySupport,
) -> Result<(), CapabilityError> {
    supported.validate_writer(head.version, head.reader_required, head.writer_required)
}

/// Source publication-transition refusal, independent of receiver progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadTransitionError {
    /// Both advertisements must use this metadata version.
    UnsupportedVersion,
    /// The observations identify different origins or streams.
    DifferentSource,
    /// A schema changed without replacing its source incarnation.
    SchemaChanged,
    /// Position/revision moved backward within one source incarnation.
    PositionRegressed,
    /// Required bits were cleared within one source incarnation.
    RequirementsCleared {
        /// Reader requirements which disappeared.
        reader: u64,
        /// Writer requirements which disappeared.
        writer: u64,
    },
}
/// Validate a source-owned publication transition. Receiver and access-epoch
/// changes do not create a new source incarnation and cannot permit bit removal.
/// A new incarnation permits clearing/reset, but grants no reset authority and
/// changes no receiver data. Durable publication/CAS remain source-host duties.
pub fn validate_head_transition(
    previous: &SourceHead,
    next: &SourceHead,
) -> Result<(), HeadTransitionError> {
    if previous.version != SOURCE_HEAD_VERSION || next.version != SOURCE_HEAD_VERSION {
        return Err(HeadTransitionError::UnsupportedVersion);
    }
    if previous.scope.origin() != next.scope.origin()
        || previous.scope.stream() != next.scope.stream()
    {
        return Err(HeadTransitionError::DifferentSource);
    }
    if previous.scope.incarnation() != next.scope.incarnation() {
        return Ok(());
    }
    if previous.scope.schema() != next.scope.schema() {
        return Err(HeadTransitionError::SchemaChanged);
    }
    if next.position < previous.position {
        return Err(HeadTransitionError::PositionRegressed);
    }
    let reader = previous.reader_required & !next.reader_required;
    let writer = previous.writer_required & !next.writer_required;
    if reader != 0 || writer != 0 {
        Err(HeadTransitionError::RequirementsCleared { reader, writer })
    } else {
        Ok(())
    }
}
