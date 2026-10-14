//! A Change is one logical write: the operations it added to one or more
//! documents, recorded durably so they can be submitted, accepted, or
//! rejected as a whole.

use crate::model::ObjectId;

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
    /// Touched documents, one entry each.
    pub parts: Vec<ChangePart>,
}
