//! Semantic mappings: fields of several Spaces that mean the same thing,
//! named once so a search or a View can treat them as one. A mapping is
//! an Object of a Space; code may propose one, a person approves it. It
//! never merges the fields and grants no right to write across them.

use elri_proto::elri::v1::context::{Mapping, MappingMember};
use elri_proto::elri::v1::object::{Object, ObjectKind};

use crate::model::{
    bool_field, bool_value, int_field, int_value, now_ms, reserved, string_field, string_value,
    ModelError, ObjectDoc, ObjectId, SpaceId,
};
use crate::store::{ObjectFilter, Store, StoreError};

/// Most members a mapping may have.
pub const MEMBERS_LIMIT: usize = 200;

/// Checks a proposal before it is written.
pub fn check(name: &str, members: &[MappingMember]) -> Result<(), String> {
    if name.trim().is_empty() || name.len() > 200 {
        return Err("mapping: a name of 1..200 characters".to_owned());
    }
    if members.is_empty() || members.len() > MEMBERS_LIMIT {
        return Err(format!("mapping: 1..{MEMBERS_LIMIT} members"));
    }
    for m in members {
        ObjectId::parse(&m.space_id)
            .map_err(|_| "mapping: member space is not an id".to_owned())?;
        ObjectId::parse(&m.field_id)
            .map_err(|_| "mapping: member field is not an id".to_owned())?;
    }
    Ok(())
}

fn members_json(members: &[MappingMember]) -> String {
    serde_json::Value::Array(
        members
            .iter()
            .map(|m| serde_json::json!({ "space": m.space_id, "field": m.field_id }))
            .collect(),
    )
    .to_string()
}

fn members_of(o: &Object) -> Vec<MappingMember> {
    let v: serde_json::Value =
        serde_json::from_str(string_field(o, reserved::MEMBERS).unwrap_or("[]"))
            .unwrap_or_default();
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|m| {
                    Some(MappingMember {
                        space_id: m.get("space")?.as_str()?.to_owned(),
                        field_id: m.get("field")?.as_str()?.to_owned(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Writes a proposal onto a fresh mapping document.
pub fn write(
    doc: &ObjectDoc,
    name: &str,
    members: &[MappingMember],
    proposed_by: &str,
    approved: bool,
) -> Result<(), ModelError> {
    doc.set_field(reserved::NAME, &string_value(name.trim()))?;
    doc.set_field(reserved::MEMBERS, &string_value(members_json(members)))?;
    doc.set_field(reserved::PROPOSED_BY, &string_value(proposed_by))?;
    doc.set_field(reserved::APPROVED, &bool_value(approved))?;
    if approved {
        doc.set_field(reserved::APPROVED_AT, &int_value(now_ms()))?;
    }
    Ok(())
}

/// Records a person's approval, or withdraws it.
pub fn approve(doc: &ObjectDoc, approved: bool) -> Result<(), ModelError> {
    doc.set_field(reserved::APPROVED, &bool_value(approved))?;
    if approved {
        doc.set_field(reserved::APPROVED_AT, &int_value(now_ms()))?;
    } else {
        doc.remove_field(reserved::APPROVED_AT)?;
    }
    Ok(())
}

/// The protocol view of a mapping Object.
pub fn mapping_of(o: &Object) -> Mapping {
    Mapping {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        members: members_of(o),
        approved: bool_field(o, reserved::APPROVED) == Some(true),
        proposed_by: string_field(o, reserved::PROPOSED_BY)
            .unwrap_or_default()
            .to_owned(),
        approved_at_ms: int_field(o, reserved::APPROVED_AT).unwrap_or(0),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

/// A fresh mapping document in `space`.
pub fn create(
    space: SpaceId,
    name: &str,
    members: &[MappingMember],
    proposed_by: &str,
    approved: bool,
) -> Result<ObjectDoc, ModelError> {
    let doc = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Mapping, now_ms())?;
    write(&doc, name, members, proposed_by, approved)?;
    doc.touch(now_ms())?;
    Ok(doc)
}

/// The mappings a Space keeps, approved ones only when asked.
pub async fn list(
    store: &Store,
    space: SpaceId,
    approved_only: bool,
    include_deleted: bool,
) -> Result<Vec<Mapping>, StoreError> {
    let objects = store
        .space(space)
        .await?
        .list(&ObjectFilter {
            kind: Some(ObjectKind::Mapping),
            include_deleted,
            limit: 1000,
            ..ObjectFilter::default()
        })
        .await?;
    Ok(objects
        .iter()
        .map(mapping_of)
        .filter(|m| !approved_only || m.approved)
        .collect())
}

/// The field ids an approved mapping names for `space`; empty when the
/// mapping is unknown or not approved.
pub async fn fields_for(
    store: &Store,
    keeper: SpaceId,
    mapping: ObjectId,
    space: SpaceId,
) -> Result<Vec<String>, StoreError> {
    let o = store.space(keeper).await?.get(mapping).await?;
    if o.kind() != ObjectKind::Mapping {
        return Ok(Vec::new());
    }
    let m = mapping_of(&o);
    if !m.approved {
        return Ok(Vec::new());
    }
    Ok(m.members
        .into_iter()
        .filter(|mm| mm.space_id == space.to_string())
        .map(|mm| mm.field_id)
        .collect())
}
