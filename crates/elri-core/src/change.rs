//! A Change is one logical write: the operations it added to one or more
//! documents, recorded durably so they can be submitted, accepted, or
//! rejected as a whole. On the wire and in storage it is the protocol
//! `Envelope`; `ChangeStatus`, `Rejection`, and the submit outcome are the
//! protocol types too.

pub use elri_proto::elri::v1::change::{Change, ChangeStatus};
pub use elri_proto::elri::v1::sync::{
    rejection::Reason, submit_response::Outcome, Envelope, Part, Rejection,
};

/// Builds a rejection outcome.
pub fn rejected(reason: Reason, message: impl Into<String>) -> Outcome {
    Outcome::Rejected(Rejection {
        reason: reason.into(),
        message: message.into(),
    })
}

/// Builds an acceptance outcome.
pub fn accepted(seq: i64) -> Outcome {
    Outcome::AcceptedSeq(seq)
}
