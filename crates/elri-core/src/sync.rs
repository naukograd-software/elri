//! Helpers over the sync protocol types shared by the server and the device.

use elri_proto::elri::v1::sync::Envelope;

use crate::model::{ModelError, ObjectId, SpaceId};

/// Space an envelope targets, validated.
pub fn envelope_space(env: &Envelope) -> Result<SpaceId, ModelError> {
    ObjectId::parse(&env.space_id)
}

/// Checks the ids inside an envelope parse; the bytes are checked by
/// importing them.
pub fn validate_envelope(env: &Envelope) -> Result<(), ModelError> {
    ObjectId::parse(&env.id)?;
    ObjectId::parse(&env.space_id)?;
    for p in &env.parts {
        ObjectId::parse(&p.object_id)?;
    }
    Ok(())
}
