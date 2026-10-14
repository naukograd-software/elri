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
        if !p.space_id.is_empty() {
            ObjectId::parse(&p.space_id)?;
        }
    }
    Ok(())
}

/// Every Space an envelope touches, in id order, the envelope's own first
/// among equals.
pub fn envelope_spaces(env: &Envelope) -> Result<Vec<SpaceId>, ModelError> {
    let mut out = vec![envelope_space(env)?];
    for p in &env.parts {
        if !p.space_id.is_empty() {
            out.push(ObjectId::parse(&p.space_id)?);
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// The Space a part belongs to.
pub fn part_space(
    env: &Envelope,
    part: &elri_proto::elri::v1::sync::Part,
) -> Result<SpaceId, ModelError> {
    if part.space_id.is_empty() {
        envelope_space(env)
    } else {
        ObjectId::parse(&part.space_id)
    }
}

/// The envelope restricted to the parts of one Space, with the same id.
pub fn envelope_for(env: &Envelope, space: SpaceId) -> Envelope {
    Envelope {
        id: env.id.clone(),
        space_id: space.to_string(),
        device_id: env.device_id.clone(),
        created_at_ms: env.created_at_ms,
        actor: env.actor.clone(),
        parts: env
            .parts
            .iter()
            .filter(|p| part_space(env, p).ok() == Some(space))
            .cloned()
            .collect(),
    }
}
