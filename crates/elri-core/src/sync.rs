//! Conversions between recorded Changes and the envelopes that travel to and
//! from the server. The sync worker itself lives in [`crate::remote`].

use elri_proto::elri::v1::object::ObjectKind as PbKind;
use elri_proto::elri::v1::sync::{Envelope, Part};

use crate::change::{Change, ChangePart, ChangeStatus};
use crate::model::{ModelError, ObjectId, ObjectKind, SpaceId};

/// Builds the wire form of a Change for `space`.
pub fn to_envelope(space: SpaceId, change: &Change) -> Envelope {
    Envelope {
        id: change.id.to_string(),
        space_id: space.to_string(),
        device_id: change.device_id.clone(),
        created_at_ms: change.created_at_ms,
        parts: change
            .parts
            .iter()
            .map(|p| Part {
                object_id: p.object_id.to_string(),
                kind: match p.kind {
                    ObjectKind::Entity => PbKind::Entity,
                    ObjectKind::Relation => PbKind::Relation,
                    ObjectKind::Layer => PbKind::Layer,
                }
                .into(),
                from_version: p.from_version.clone().into(),
                to_version: p.to_version.clone().into(),
                bytes: p.bytes.clone().into(),
            })
            .collect(),
    }
}

/// Parses an envelope into a Change plus the Space it targets. The status is
/// `Local`; the receiver decides what it becomes.
pub fn from_envelope(env: &Envelope) -> Result<(SpaceId, Change), ModelError> {
    let space = ObjectId::parse(&env.space_id)?;
    let mut parts = Vec::with_capacity(env.parts.len());
    for p in &env.parts {
        let kind = match PbKind::try_from(p.kind) {
            Ok(PbKind::Entity) => ObjectKind::Entity,
            Ok(PbKind::Relation) => ObjectKind::Relation,
            Ok(PbKind::Layer) => ObjectKind::Layer,
            _ => return Err(ModelError::Malformed("part kind")),
        };
        parts.push(ChangePart {
            object_id: ObjectId::parse(&p.object_id)?,
            kind,
            from_version: p.from_version.to_vec(),
            to_version: p.to_version.to_vec(),
            bytes: p.bytes.to_vec(),
        });
    }
    Ok((
        space,
        Change {
            id: ObjectId::parse(&env.id)?,
            device_id: env.device_id.clone(),
            created_at_ms: env.created_at_ms,
            status: ChangeStatus::Local,
            accepted_seq: None,
            rejection: None,
            parts,
        },
    ))
}
