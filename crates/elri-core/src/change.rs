//! A Change is one logical write: the operations it added to one or more
//! documents, recorded durably so they can be submitted, accepted, or
//! rejected as a whole.

use crate::model::{ObjectId, ObjectKind};

/// Identity of a Change; the same shape as an Object id.
pub type ChangeId = ObjectId;

/// Where a Change is on its way from this device to acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeStatus {
    /// Durable on this device; not yet submitted.
    Local,
    /// Submitted, awaiting the server's answer.
    Queued,
    /// Waiting for a dependency to be accepted first.
    Waiting,
    /// Accepted by the authority.
    Accepted,
    /// Rejected by the authority; preserved for repair.
    Rejected,
    /// Depends on a rejected Change; preserved until the user decides.
    Paused,
}

impl ChangeStatus {
    /// Stable number used in storage.
    pub fn code(self) -> i64 {
        match self {
            Self::Local => 1,
            Self::Queued => 2,
            Self::Waiting => 3,
            Self::Accepted => 4,
            Self::Rejected => 5,
            Self::Paused => 6,
        }
    }

    /// Inverse of [`ChangeStatus::code`].
    pub fn from_code(code: i64) -> Option<Self> {
        match code {
            1 => Some(Self::Local),
            2 => Some(Self::Queued),
            3 => Some(Self::Waiting),
            4 => Some(Self::Accepted),
            5 => Some(Self::Rejected),
            6 => Some(Self::Paused),
            _ => None,
        }
    }
}

/// The operations a Change added to one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangePart {
    /// Document.
    pub object_id: ObjectId,
    /// Kind of the document, so a receiver can project it before reading.
    pub kind: ObjectKind,
    /// Encoded version vector the operations build on.
    pub from_version: Vec<u8>,
    /// Encoded version vector after the operations.
    pub to_version: Vec<u8>,
    /// Loro update bytes.
    pub bytes: Vec<u8>,
}

/// A recorded Change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Identity.
    pub id: ChangeId,
    /// Device that produced it.
    pub device_id: String,
    /// Creation instant, ms since epoch.
    pub created_at_ms: i64,
    /// Lifecycle state.
    pub status: ChangeStatus,
    /// Position in the Space log once accepted.
    pub accepted_seq: Option<i64>,
    /// Why it was rejected, if it was.
    pub rejection: Option<String>,
    /// Touched documents, one entry each.
    pub parts: Vec<ChangePart>,
}

/// Why the authority refused a Change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// The caller may not write to the Space.
    PermissionDenied,
    /// Operations reference history the authority does not have.
    MissingPredecessors,
    /// The payload could not be decoded or breaks a limit.
    Invalid(String),
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PermissionDenied => f.write_str("permission denied"),
            Self::MissingPredecessors => f.write_str("missing predecessors"),
            Self::Invalid(m) => write!(f, "invalid: {m}"),
        }
    }
}

/// What the authority decided about a submitted Change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Accepted at this position in the Space log.
    Accepted(i64),
    /// Refused.
    Rejected(RejectReason),
}
