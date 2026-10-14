//! Objects between Spaces: export as a bundle of logical state, import as
//! explicit copies with every internal reference rewritten.

use std::collections::{BTreeSet, HashMap};

use elri_proto::elri::v1::object::{value, Object, ObjectKind, Participant, Value};
use elri_proto::elri::v1::transfer::{Bundle, NodeList};
use elri_proto::elri::v1::ui::Node;

use crate::model::{
    now_ms, reserved, string_field, string_value, ModelError, ObjectDoc, ObjectId, SpaceId,
};
use crate::rules;
use crate::store::{Edit, SpaceStore, StoreError};

/// Reserved field: where an imported copy came from, `space/id`.
pub const ORIGIN: &str = "$origin";
/// Reserved field: where a moved Object went, the destination Space id.
pub const MOVED_TO: &str = "$moved_to";

/// Transfer failures.
#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    /// Storage failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A document could not be built or read.
    #[error(transparent)]
    Model(#[from] ModelError),
    /// The bundle could not be decoded.
    #[error("bundle: {0}")]
    Bundle(String),
    /// An id to keep is already in the Space.
    #[error("object {0} already exists in this space")]
    Exists(String),
}

/// Ids an Object refers to, in no particular order.
fn references(o: &Object) -> Vec<String> {
    let mut out = Vec::new();
    for (k, v) in &o.fields {
        // A field keyed by a definition id refers to that definition.
        if ObjectId::parse(k).is_ok() {
            out.push(k.clone());
        }
        if let Some(value::Value::Ref(r)) = &v.value {
            out.push(r.clone());
        }
    }
    for p in &o.participants {
        out.push(p.object_id.clone());
        if !p.role_id.is_empty() {
            out.push(p.role_id.clone());
        }
    }
    for p in &o.placements {
        out.push(p.block_id.clone());
    }
    if let Some(t) = &o.target_id {
        out.push(t.clone());
    }
    out.extend(o.types.iter().cloned());
    out.extend(o.type_fields.iter().map(|f| f.field_id.clone()));
    for key in [rules::SCRIPT, reserved::OF] {
        if let Some(s) = string_field(o, key) {
            out.push(s.to_owned());
        }
    }
    out
}

/// Builds a bundle of `ids`, closing over in-Space references when asked.
pub async fn export(
    space: &SpaceStore,
    ids: &[ObjectId],
    with_dependencies: bool,
) -> Result<Bundle, TransferError> {
    let mut wanted: BTreeSet<ObjectId> = ids.iter().copied().collect();
    let mut queue: Vec<ObjectId> = ids.to_vec();
    let mut objects: Vec<Object> = Vec::new();
    let mut interface_nodes: HashMap<String, NodeList> = HashMap::new();
    while let Some(id) = queue.pop() {
        let doc = space.load(id).await?;
        let o = doc.read()?;
        if o.kind() == ObjectKind::Interface {
            interface_nodes.insert(
                o.id.clone(),
                NodeList {
                    nodes: doc.ui_nodes()?,
                },
            );
        }
        if with_dependencies {
            for r in references(&o) {
                let Ok(rid) = ObjectId::parse(&r) else {
                    continue;
                };
                if wanted.insert(rid) && space.exists(rid).await? {
                    queue.push(rid);
                } else if !space.exists(rid).await? {
                    // Not in this Space: stays an external reference.
                    wanted.remove(&rid);
                }
            }
        }
        objects.push(o);
    }
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(Bundle {
        exported_from: space.id().to_string(),
        exported_at_ms: now_ms(),
        objects,
        interface_nodes,
    })
}

/// Original id → id in the destination.
pub type IdMap = HashMap<String, String>;

/// Rewrites an id through the mapping, leaving unknown ids alone.
fn map_id(ids: &IdMap, id: &str) -> String {
    ids.get(id).cloned().unwrap_or_else(|| id.to_owned())
}

fn map_value(ids: &IdMap, v: &Value) -> Value {
    match &v.value {
        Some(value::Value::Ref(r)) => Value {
            value: Some(value::Value::Ref(map_id(ids, r))),
        },
        _ => v.clone(),
    }
}

/// The edits that import a bundle: every object as a fresh document with
/// references rewritten through `ids`. Node ids of interfaces are minted
/// by the tree; parents are mapped as the nodes are created in order.
pub fn plan(
    bundle: &Bundle,
    dest: SpaceId,
    ids: &IdMap,
    origin: bool,
) -> Result<Vec<(SpaceId, Edit)>, ModelError> {
    let now = now_ms();
    let mut edits = Vec::with_capacity(bundle.objects.len());
    for o in &bundle.objects {
        let id = ObjectId::parse(&map_id(ids, &o.id))?;
        let doc = match (o.kind(), &o.target_id) {
            (ObjectKind::Layer, Some(target)) => {
                ObjectDoc::create_layer(id, dest, ObjectId::parse(&map_id(ids, target))?, now)?
            }
            (kind, _) => ObjectDoc::create(id, dest, kind, now)?,
        };
        for (k, v) in &o.fields {
            let v = match k.as_str() {
                rules::SCRIPT | reserved::OF => match &v.value {
                    Some(value::Value::String(s)) => string_value(map_id(ids, s)),
                    _ => v.clone(),
                },
                _ => map_value(ids, v),
            };
            doc.set_field(&map_id(ids, k), &v)?;
        }
        for t in &o.types {
            doc.add_type(&map_id(ids, t))?;
        }
        for f in &o.type_fields {
            doc.set_type_field(&map_id(ids, &f.field_id), f.required)?;
        }
        for p in &o.participants {
            doc.set_participant(&Participant {
                slot: p.slot.clone(),
                object_id: map_id(ids, &p.object_id),
                role_id: if p.role_id.is_empty() {
                    String::new()
                } else {
                    map_id(ids, &p.role_id)
                },
            })?;
        }
        // Placements: parents before children in the exported order.
        let mut placed: HashMap<String, String> = HashMap::new();
        for p in &o.placements {
            let parent = if p.parent_id.is_empty() {
                String::new()
            } else {
                placed.get(&p.parent_id).cloned().unwrap_or_default()
            };
            let new = doc.place_block(
                &parent,
                usize::try_from(p.index).unwrap_or(0),
                ObjectId::parse(&map_id(ids, &p.block_id))?,
            )?;
            placed.insert(p.id.clone(), new);
        }
        if let Some(nodes) = bundle.interface_nodes.get(&o.id) {
            let mut mapped: HashMap<String, String> = HashMap::new();
            for n in &nodes.nodes {
                let parent = if n.parent_id.is_empty() {
                    String::new()
                } else {
                    mapped.get(&n.parent_id).cloned().unwrap_or_default()
                };
                let node = Node {
                    id: String::new(),
                    ..n.clone()
                };
                let new =
                    doc.create_ui_node(&parent, usize::try_from(n.index).unwrap_or(0), &node)?;
                mapped.insert(n.id.clone(), new);
            }
        }
        if origin {
            doc.set_field(
                ORIGIN,
                &string_value(format!("{}/{}", bundle.exported_from, o.id)),
            )?;
        }
        if o.deleted_at_ms.is_some() {
            doc.mark_deleted(now)?;
        }
        doc.touch(now)?;
        edits.push((dest, Edit::Create(doc)));
    }
    Ok(edits)
}

/// Fresh ids for every object of a bundle, or the originals when kept.
pub fn mapping(bundle: &Bundle, keep_ids: bool) -> IdMap {
    bundle
        .objects
        .iter()
        .map(|o| {
            let new = if keep_ids {
                o.id.clone()
            } else {
                ObjectId::new().to_string()
            };
            (o.id.clone(), new)
        })
        .collect()
}
