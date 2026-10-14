//! Objects and their documents.
//!
//! Every Object is one Loro document with these root containers:
//!
//! - `meta`: `kind`, `space`, `created_at`, `updated_at`, `deleted_at`, and
//!   `target` for Layers
//! - `types`: type-definition id → `true`
//! - `fields`: field-definition id → value
//! - `participants` (Relations): slot id → `{object, role}`
//! - `type_fields` (type definitions): field-definition id → required
//! - `placements` (Entities): a movable tree whose nodes place blocks;
//!   node meta `block` is the placed Object's id
//!
//! Definitions keep their attributes in reserved `$`-prefixed field ids.
//!
//! Scalar values live directly in `fields`. References and timestamps are
//! tagged maps (`{"$ref": id}`, `{"$time": ms}`) so that they survive the
//! round trip through Loro's value model unambiguously.
//!
//! The materialized form of a document is the protocol message
//! [`Object`]; there is no separate in-memory record type.

use std::collections::HashMap;

use elri_proto::elri::v1::definition::{FieldKind, Publication, TypeField};
use elri_proto::elri::v1::object::{
    value, Mark, Object, ObjectKind, Participant, Placement, RichText, Value, ValueList, ValueMap,
};
use elri_proto::elri::v1::ui::Node as UiNode;
use loro::{
    Container, ExpandType, ExportMode, LoroDoc, LoroMap, LoroMovableList, LoroText, LoroTree,
    LoroValue, StyleConfig, StyleConfigMap, TextDelta, TreeID, TreeParentId, UpdateOptions,
    ValueOrContainer, VersionVector,
};
use uuid::Uuid;

/// Identity of an Object: a `UUIDv7`. The protocol carries it as its
/// canonical string; storage keys by the raw UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId(Uuid);

impl ObjectId {
    /// A fresh, time-ordered id.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID.
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// The underlying UUID.
    pub fn uuid(&self) -> Uuid {
        self.0
    }

    /// Parses the canonical string form.
    pub fn parse(s: &str) -> Result<Self, ModelError> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|_| ModelError::InvalidId(s.to_owned()))
    }
}

impl Default for ObjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identity of a Space; the same shape as an Object id.
pub type SpaceId = ObjectId;

/// Whether a kind is a definition rather than user content.
pub fn is_definition(kind: ObjectKind) -> bool {
    matches!(
        kind,
        ObjectKind::FieldDef | ObjectKind::TypeDef | ObjectKind::RoleDef
    )
}

/// Whether a kind is created through its own service rather than
/// `ObjectService`: definitions and attachments.
pub fn is_managed(kind: ObjectKind) -> bool {
    is_definition(kind)
        || matches!(
            kind,
            ObjectKind::Attachment
                | ObjectKind::Script
                | ObjectKind::Rule
                | ObjectKind::Interface
                | ObjectKind::Publication
        )
}

/// The protocol view of a publication Object.
pub fn publication_of(o: &Object) -> Option<Publication> {
    if o.kind() != ObjectKind::Publication {
        return None;
    }
    let deps: HashMap<String, i32> =
        serde_json::from_str(string_field(o, reserved::DEPS).unwrap_or("{}")).unwrap_or_default();
    Some(Publication {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        definition_id: string_field(o, reserved::OF).unwrap_or_default().to_owned(),
        version: i32::try_from(int_field(o, reserved::VERSION).unwrap_or(0)).unwrap_or(0),
        snapshot_json: string_field(o, reserved::SNAPSHOT)
            .unwrap_or("{}")
            .to_owned(),
        dependencies: deps,
        digest: string_field(o, reserved::DIGEST)
            .unwrap_or_default()
            .to_owned(),
        created_at_ms: o.created_at_ms,
    })
}

/// An integer field, if present.
pub fn int_field(o: &Object, id: &str) -> Option<i64> {
    match o.fields.get(id).and_then(|v| v.value.as_ref()) {
        Some(value::Value::Int(i)) => Some(*i),
        _ => None,
    }
}

/// The attachment digest recorded on an Object, if it is an attachment.
pub fn attachment_sha256(o: &Object) -> Option<&str> {
    if o.kind() != ObjectKind::Attachment {
        return None;
    }
    string_field(o, reserved::SHA256)
}

/// Whether `value` is of `kind`.
pub fn kind_accepts(kind: FieldKind, v: &Value) -> bool {
    matches!(
        (kind, &v.value),
        (FieldKind::Bool, Some(value::Value::Bool(_)))
            | (FieldKind::Int, Some(value::Value::Int(_)))
            | (FieldKind::Double, Some(value::Value::Double(_)))
            | (FieldKind::String, Some(value::Value::String(_)))
            | (FieldKind::Time, Some(value::Value::TimeMs(_)))
            | (FieldKind::Ref, Some(value::Value::Ref(_)))
            | (FieldKind::Text, Some(value::Value::Text(_)))
            | (FieldKind::List, Some(value::Value::List(_)))
            | (FieldKind::Map, Some(value::Value::Map(_)))
            | (
                FieldKind::RichText,
                Some(value::Value::Rich(_) | value::Value::Text(_))
            )
    )
}

/// Whether a value is a scalar (stored directly in the fields map) rather
/// than a container (text, list).
pub fn is_scalar(v: &Value) -> bool {
    !matches!(
        v.value,
        Some(
            value::Value::Text(_)
                | value::Value::List(_)
                | value::Value::Map(_)
                | value::Value::Rich(_)
        ) | None
    )
}

/// Reserved field ids that definitions use for their own attributes.
pub mod reserved {
    /// Display name of a definition.
    pub const NAME: &str = "$name";
    /// Free-text description of a definition.
    pub const DESCRIPTION: &str = "$description";
    /// `FieldKind` code of a field definition, as an integer.
    pub const FIELD_KIND: &str = "$field_kind";
    /// Hex SHA-256 of an attachment's bytes.
    pub const SHA256: &str = "$sha256";
    /// Size of an attachment's bytes.
    pub const SIZE: &str = "$size";
    /// Media type of an attachment.
    pub const MEDIA_TYPE: &str = "$media_type";
    /// Publications: the definition published.
    pub const OF: &str = "$of";
    /// Publications: the version number.
    pub const VERSION: &str = "$version";
    /// Publications: canonical JSON snapshot of the definition.
    pub const SNAPSHOT: &str = "$snapshot";
    /// Publications: dependency versions as JSON.
    pub const DEPS: &str = "$deps";
    /// Publications: hex SHA-256 over snapshot and dependencies.
    pub const DIGEST: &str = "$digest";
}

const REF_TAG: &str = "$ref";
const TIME_TAG: &str = "$time";

fn tagged(tag: &str, value: LoroValue) -> LoroValue {
    let mut m = HashMap::new();
    m.insert(tag.to_owned(), value);
    LoroValue::from(m)
}

/// Loro form of a scalar field value; `None` for containers and empties.
pub fn value_to_loro(v: &Value) -> Option<LoroValue> {
    Some(match v.value.as_ref()? {
        value::Value::Bool(b) => LoroValue::from(*b),
        value::Value::Int(i) => LoroValue::from(*i),
        value::Value::Double(d) => LoroValue::from(*d),
        value::Value::String(s) => LoroValue::from(s.as_str()),
        value::Value::TimeMs(ms) => tagged(TIME_TAG, LoroValue::from(*ms)),
        value::Value::Ref(id) => tagged(REF_TAG, LoroValue::from(id.as_str())),
        value::Value::Text(_)
        | value::Value::List(_)
        | value::Value::Map(_)
        | value::Value::Rich(_) => return None,
    })
}

/// Shorthand for a collaborative text value.
pub fn text_value(s: impl Into<String>) -> Value {
    Value {
        value: Some(value::Value::Text(s.into())),
    }
}

/// Shorthand for a list value.
pub fn list_value(items: Vec<Value>) -> Value {
    Value {
        value: Some(value::Value::List(ValueList { items })),
    }
}

/// Field value of a Loro value; `None` for shapes a field cannot hold.
pub fn value_from_loro(v: &LoroValue) -> Option<Value> {
    let inner = match v {
        LoroValue::Bool(b) => value::Value::Bool(*b),
        LoroValue::I64(i) => value::Value::Int(*i),
        LoroValue::Double(d) => value::Value::Double(*d),
        LoroValue::String(s) => value::Value::String(s.to_string()),
        LoroValue::Map(m) => {
            if let Some(LoroValue::I64(ms)) = m.get(TIME_TAG) {
                value::Value::TimeMs(*ms)
            } else if let Some(LoroValue::String(id)) = m.get(REF_TAG) {
                value::Value::Ref(id.to_string())
            } else {
                return None;
            }
        }
        _ => return None,
    };
    Some(Value { value: Some(inner) })
}

/// Shorthand for a string value.
pub fn string_value(s: impl Into<String>) -> Value {
    Value {
        value: Some(value::Value::String(s.into())),
    }
}

/// Shorthand for an integer value.
pub fn int_value(i: i64) -> Value {
    Value {
        value: Some(value::Value::Int(i)),
    }
}

/// Shorthand for a boolean value.
pub fn bool_value(b: bool) -> Value {
    Value {
        value: Some(value::Value::Bool(b)),
    }
}

/// Shorthand for a rich text value.
pub fn rich_value(text: impl Into<String>, marks: Vec<Mark>) -> Value {
    Value {
        value: Some(value::Value::Rich(RichText {
            text: text.into(),
            marks,
        })),
    }
}

/// Shorthand for a map value.
pub fn map_value(entries: impl IntoIterator<Item = (String, Value)>) -> Value {
    Value {
        value: Some(value::Value::Map(ValueMap {
            entries: entries.into_iter().collect(),
        })),
    }
}

/// Shorthand for a reference value.
pub fn ref_value(id: ObjectId) -> Value {
    Value {
        value: Some(value::Value::Ref(id.to_string())),
    }
}

/// Text of a string field, if present.
pub fn string_field<'a>(o: &'a Object, id: &str) -> Option<&'a str> {
    match o.fields.get(id).and_then(|v| v.value.as_ref()) {
        Some(value::Value::String(s)) => Some(s),
        _ => None,
    }
}

/// The kind a field definition fixes; `None` for other kinds or a
/// malformed definition.
pub fn field_def_kind(o: &Object) -> Option<FieldKind> {
    if o.kind() != ObjectKind::FieldDef {
        return None;
    }
    match o
        .fields
        .get(reserved::FIELD_KIND)
        .and_then(|v| v.value.as_ref())
    {
        Some(value::Value::Int(code)) => i32::try_from(*code)
            .ok()
            .and_then(|c| FieldKind::try_from(c).ok())
            .filter(|k| *k != FieldKind::Unspecified),
        _ => None,
    }
}

/// Errors from building or reading Object documents.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// Not a UUID.
    #[error("invalid object id: {0}")]
    InvalidId(String),
    /// The document lacks the expected `meta` entries.
    #[error("object document is malformed: {0}")]
    Malformed(&'static str),
    /// A value shape a field cannot hold.
    #[error("field {0}: empty value")]
    EmptyValue(String),
    /// A list item that is not a scalar.
    #[error("field {0}: list items must be scalars")]
    NestedContainer(String),
    /// Text update did not finish in time.
    #[error("field {0}: text update timed out")]
    TextTimeout(String),
    /// A placement operation the tree refuses: unknown id, unknown parent,
    /// or a move into the placement's own subtree.
    #[error("placement {0}: {1}")]
    Placement(String, String),
    /// Loro refused an operation.
    #[error(transparent)]
    Loro(#[from] loro::LoroError),
    /// Loro could not encode the document.
    #[error(transparent)]
    Encode(#[from] loro::LoroEncodeError),
}

/// An Object's Loro document with typed accessors.
pub struct ObjectDoc {
    id: ObjectId,
    doc: LoroDoc,
}

impl ObjectDoc {
    /// Creates the document of a new Object.
    pub fn create(
        id: ObjectId,
        space: SpaceId,
        kind: ObjectKind,
        now_ms: i64,
    ) -> Result<Self, ModelError> {
        let doc = LoroDoc::new();
        configure(&doc);
        let meta = doc.get_map("meta");
        meta.insert("kind", i64::from(kind as i32))?;
        meta.insert("space", space.to_string())?;
        meta.insert("created_at", now_ms)?;
        meta.insert("updated_at", now_ms)?;
        doc.commit();
        Ok(Self { id, doc })
    }

    /// Creates the document of a new Layer over `target`.
    pub fn create_layer(
        id: ObjectId,
        space: SpaceId,
        target: ObjectId,
        now_ms: i64,
    ) -> Result<Self, ModelError> {
        let doc = Self::create(id, space, ObjectKind::Layer, now_ms)?;
        doc.meta().insert("target", target.to_string())?;
        doc.doc.commit();
        Ok(doc)
    }

    /// Loads a document from a snapshot.
    pub fn from_snapshot(id: ObjectId, bytes: &[u8]) -> Result<Self, ModelError> {
        let doc = LoroDoc::from_snapshot(bytes)?;
        configure(&doc);
        Ok(Self { id, doc })
    }

    /// Loads a document from a snapshot plus the updates recorded after it.
    pub fn from_parts(
        id: ObjectId,
        snapshot: &[u8],
        updates: &[Vec<u8>],
    ) -> Result<Self, ModelError> {
        let doc = Self::from_snapshot(id, snapshot)?;
        if !updates.is_empty() {
            let status = doc.doc.import_batch(updates)?;
            if status.pending.is_some() {
                return Err(ModelError::Malformed("updates with missing predecessors"));
            }
        }
        Ok(doc)
    }

    /// Builds a document that exists only as updates so far (a remote
    /// creation). Fails if the updates do not form a complete history.
    pub fn from_updates(id: ObjectId, updates: &[Vec<u8>]) -> Result<Self, ModelError> {
        let doc = LoroDoc::new();
        configure(&doc);
        let status = doc.import_batch(updates)?;
        if status.pending.is_some() {
            return Err(ModelError::Malformed("updates with missing predecessors"));
        }
        Ok(Self { id, doc })
    }

    /// Imports remote operations. Returns whether everything applied; `false`
    /// means some operations wait for predecessors this document lacks.
    pub fn import(&self, bytes: &[u8]) -> Result<bool, ModelError> {
        let status = self.doc.import(bytes)?;
        Ok(status.pending.is_none())
    }

    /// Version vector of the operation log.
    pub fn version_vector(&self) -> VersionVector {
        self.doc.oplog_vv()
    }

    /// Operations added since `from`.
    pub fn updates_since(&self, from: &VersionVector) -> Result<Vec<u8>, ModelError> {
        Ok(self.doc.export(ExportMode::updates(from))?)
    }

    /// Identity.
    pub fn id(&self) -> ObjectId {
        self.id
    }

    /// Full snapshot: state plus history.
    pub fn snapshot(&self) -> Result<Vec<u8>, ModelError> {
        Ok(self.doc.export(ExportMode::Snapshot)?)
    }

    /// Encoded version vector of the operation log.
    pub fn version(&self) -> Vec<u8> {
        self.doc.oplog_vv().encode()
    }

    fn meta(&self) -> LoroMap {
        self.doc.get_map("meta")
    }

    fn types(&self) -> LoroMap {
        self.doc.get_map("types")
    }

    fn fields(&self) -> LoroMap {
        self.doc.get_map("fields")
    }

    fn participants(&self) -> LoroMap {
        self.doc.get_map("participants")
    }

    fn type_fields(&self) -> LoroMap {
        self.doc.get_map("type_fields")
    }

    /// A movable tree of this document with positional operations enabled.
    /// Positions are fractional indexes; two nodes that land on the same
    /// index from concurrent edits are ordered by their ids, so every
    /// replica agrees.
    fn tree(&self, name: &str) -> LoroTree {
        let tree = self.doc.get_tree(name);
        tree.enable_fractional_index(0);
        tree
    }

    /// The placements tree of a document Entity.
    fn placements(&self) -> LoroTree {
        self.tree("placements")
    }

    /// The node tree of an interface.
    fn ui_tree(&self) -> LoroTree {
        self.tree("ui")
    }

    /// Places `block` under `parent_id` (empty: top level) at `index`,
    /// clamped to the end of the siblings. Returns the placement id.
    pub fn place_block(
        &self,
        parent_id: &str,
        index: usize,
        block: ObjectId,
    ) -> Result<String, ModelError> {
        let tree = self.placements();
        let id = create_node(&tree, parent_id, index)?;
        tree.get_meta(id)?.insert("block", block.to_string())?;
        Ok(id.to_string())
    }

    /// Moves placement `id` under `parent_id` (empty: top level) at `index`,
    /// clamped to the end of the siblings. Its subtree moves with it.
    pub fn move_placement(
        &self,
        id: &str,
        parent_id: &str,
        index: usize,
    ) -> Result<(), ModelError> {
        move_node(&self.placements(), id, parent_id, index)
    }

    /// Removes placement `id` with its subtree. The placed blocks stay.
    pub fn remove_placement(&self, id: &str) -> Result<(), ModelError> {
        remove_node(&self.placements(), id)
    }

    /// Placements in document order: parents before children, siblings by
    /// position.
    fn read_placements(&self) -> Result<Vec<Placement>, ModelError> {
        walk_tree(&self.placements(), |id, parent_id, index, meta| {
            let block_id = match meta.get("block") {
                Some(ValueOrContainer::Value(LoroValue::String(s))) => {
                    ObjectId::parse(&s)?.to_string()
                }
                _ => return Err(ModelError::Malformed("placement without block")),
            };
            Ok(Placement {
                id,
                parent_id,
                index,
                block_id,
            })
        })
    }

    /// Interfaces: creates a node under `parent_id` (empty: top level) at
    /// `index` with the given type and JSON attributes. Returns its id.
    pub fn create_ui_node(
        &self,
        parent_id: &str,
        index: usize,
        node: &UiNode,
    ) -> Result<String, ModelError> {
        let tree = self.ui_tree();
        let id = create_node(&tree, parent_id, index)?;
        write_ui_meta(&tree.get_meta(id)?, node)?;
        Ok(id.to_string())
    }

    /// Interfaces: moves node `id` under `parent_id` at `index` and rewrites
    /// its type and JSON attributes.
    pub fn set_ui_node(
        &self,
        id: &str,
        parent_id: &str,
        index: usize,
        node: &UiNode,
    ) -> Result<(), ModelError> {
        let tree = self.ui_tree();
        move_node(&tree, id, parent_id, index)?;
        write_ui_meta(&tree.get_meta(placement_id(id)?)?, node)?;
        Ok(())
    }

    /// Interfaces: removes node `id` with its subtree.
    pub fn remove_ui_node(&self, id: &str) -> Result<(), ModelError> {
        remove_node(&self.ui_tree(), id)
    }

    /// Interfaces: the nodes in document order.
    pub fn ui_nodes(&self) -> Result<Vec<UiNode>, ModelError> {
        walk_tree(&self.ui_tree(), |id, parent_id, index, meta| {
            let text = |key: &str| match meta.get(key) {
                Some(ValueOrContainer::Value(LoroValue::String(s))) => s.to_string(),
                _ => String::new(),
            };
            Ok(UiNode {
                id,
                parent_id,
                index,
                r#type: text("type"),
                props_json: text("props"),
                bindings_json: text("bindings"),
                handlers_json: text("handlers"),
            })
        })
    }

    /// Sets a field. Scalars replace the previous value; a text applies the
    /// minimal edit to the existing collaborative text; a list applies the
    /// minimal insertions and deletions to the existing list.
    pub fn set_field(&self, field_id: &str, value: &Value) -> Result<(), ModelError> {
        match value.value.as_ref() {
            None => Err(ModelError::EmptyValue(field_id.to_owned())),
            Some(value::Value::Text(s)) => {
                let text = self.text_container(field_id)?;
                text.update(s, UpdateOptions::default())
                    .map_err(|_| ModelError::TextTimeout(field_id.to_owned()))?;
                Ok(())
            }
            Some(value::Value::Rich(r)) => {
                let text = self.text_container(field_id)?;
                text.update(&r.text, UpdateOptions::default())
                    .map_err(|_| ModelError::TextTimeout(field_id.to_owned()))?;
                apply_marks(&text, &r.marks)?;
                Ok(())
            }
            Some(value::Value::List(l)) => {
                let mut items = Vec::with_capacity(l.items.len());
                for it in &l.items {
                    items.push(
                        value_to_loro(it)
                            .ok_or_else(|| ModelError::NestedContainer(field_id.to_owned()))?,
                    );
                }
                let list = self.list_container(field_id)?;
                apply_list_diff(&list, &items)?;
                Ok(())
            }
            Some(value::Value::Map(m)) => {
                let map = self.map_container(field_id)?;
                let current: HashMap<String, LoroValue> = match map.get_value() {
                    LoroValue::Map(cur) => {
                        cur.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                    }
                    _ => HashMap::new(),
                };
                for (k, v) in &m.entries {
                    let v = value_to_loro(v)
                        .ok_or_else(|| ModelError::NestedContainer(field_id.to_owned()))?;
                    if current.get(k) != Some(&v) {
                        map.insert(k, v)?;
                    }
                }
                for k in current.keys() {
                    if !m.entries.contains_key(k) {
                        map.delete(k)?;
                    }
                }
                Ok(())
            }
            Some(_) => {
                let v = value_to_loro(value)
                    .ok_or_else(|| ModelError::EmptyValue(field_id.to_owned()))?;
                self.fields().insert(field_id, v)?;
                Ok(())
            }
        }
    }

    /// The text container of a field, replacing a scalar or a container of
    /// another type if one is there.
    fn text_container(&self, field_id: &str) -> Result<LoroText, ModelError> {
        if !matches!(
            self.fields().get(field_id),
            None | Some(ValueOrContainer::Container(Container::Text(_)))
        ) {
            self.fields().delete(field_id)?;
        }
        Ok(self.fields().ensure_mergeable_text(field_id)?)
    }

    /// The list container of a field, replacing a scalar or a container of
    /// another type if one is there.
    fn list_container(&self, field_id: &str) -> Result<LoroMovableList, ModelError> {
        if !matches!(
            self.fields().get(field_id),
            None | Some(ValueOrContainer::Container(Container::MovableList(_)))
        ) {
            self.fields().delete(field_id)?;
        }
        Ok(self.fields().ensure_mergeable_movable_list(field_id)?)
    }

    /// The map container of a field, replacing a scalar or a container of
    /// another type if one is there.
    fn map_container(&self, field_id: &str) -> Result<LoroMap, ModelError> {
        if !matches!(
            self.fields().get(field_id),
            None | Some(ValueOrContainer::Container(Container::Map(_)))
        ) {
            self.fields().delete(field_id)?;
        }
        Ok(self.fields().ensure_mergeable_map(field_id)?)
    }

    /// Removes a field.
    pub fn remove_field(&self, field_id: &str) -> Result<(), ModelError> {
        self.fields().delete(field_id)?;
        Ok(())
    }

    /// Applies a type definition.
    pub fn add_type(&self, type_id: &str) -> Result<(), ModelError> {
        self.types().insert(type_id, true)?;
        Ok(())
    }

    /// Removes a type definition.
    pub fn remove_type(&self, type_id: &str) -> Result<(), ModelError> {
        self.types().delete(type_id)?;
        Ok(())
    }

    /// Adds or replaces a participant by slot.
    pub fn set_participant(&self, p: &Participant) -> Result<(), ModelError> {
        let mut m = HashMap::new();
        m.insert("object".to_owned(), LoroValue::from(p.object_id.as_str()));
        m.insert("role".to_owned(), LoroValue::from(p.role_id.as_str()));
        self.participants().insert(&p.slot, LoroValue::from(m))?;
        Ok(())
    }

    /// Removes a participant slot.
    pub fn remove_participant(&self, slot: &str) -> Result<(), ModelError> {
        self.participants().delete(slot)?;
        Ok(())
    }

    /// Type definitions: adds or replaces a field of the type.
    pub fn set_type_field(&self, field_id: &str, required: bool) -> Result<(), ModelError> {
        self.type_fields().insert(field_id, required)?;
        Ok(())
    }

    /// Type definitions: removes a field from the type.
    pub fn remove_type_field(&self, field_id: &str) -> Result<(), ModelError> {
        self.type_fields().delete(field_id)?;
        Ok(())
    }

    /// Marks the Object deleted.
    pub fn mark_deleted(&self, now_ms: i64) -> Result<(), ModelError> {
        self.meta().insert("deleted_at", now_ms)?;
        Ok(())
    }

    /// Records the change instant and commits pending operations.
    pub fn touch(&self, now_ms: i64) -> Result<(), ModelError> {
        self.meta().insert("updated_at", now_ms)?;
        self.doc.commit();
        Ok(())
    }

    /// The fields of the document as protocol values.
    fn read_fields(&self) -> HashMap<String, Value> {
        let mut fields: HashMap<String, Value> = HashMap::new();
        if let LoroValue::Map(m) = self.fields().get_value() {
            for key in m.keys() {
                let value = match self.fields().get(key) {
                    Some(ValueOrContainer::Value(v)) => value_from_loro(&v),
                    Some(ValueOrContainer::Container(Container::Text(t))) => {
                        let marks = marks_of(&t);
                        if marks.is_empty() {
                            Some(text_value(t.to_string()))
                        } else {
                            Some(rich_value(t.to_string(), marks))
                        }
                    }
                    Some(ValueOrContainer::Container(Container::MovableList(l))) => {
                        let items: Option<Vec<Value>> =
                            l.to_vec().iter().map(value_from_loro).collect();
                        items.map(list_value)
                    }
                    Some(ValueOrContainer::Container(Container::Map(m))) => match m.get_value() {
                        LoroValue::Map(entries) => {
                            let entries: Option<HashMap<String, Value>> = entries
                                .iter()
                                .map(|(k, v)| value_from_loro(v).map(|v| (k.clone(), v)))
                                .collect();
                            entries.map(map_value)
                        }
                        _ => None,
                    },
                    _ => None,
                };
                if let Some(v) = value {
                    fields.insert(key.clone(), v);
                }
            }
        }
        fields
    }

    /// Materializes the current state.
    pub fn read(&self) -> Result<Object, ModelError> {
        let LoroValue::Map(meta) = self.meta().get_value() else {
            return Err(ModelError::Malformed("meta is not a map"));
        };
        let int = |key: &str| match meta.get(key) {
            Some(LoroValue::I64(i)) => Some(*i),
            _ => None,
        };
        let kind = int("kind")
            .and_then(|c| i32::try_from(c).ok())
            .and_then(|c| ObjectKind::try_from(c).ok())
            .filter(|k| *k != ObjectKind::Unspecified)
            .ok_or(ModelError::Malformed("meta.kind"))?;
        let space_id = match meta.get("space") {
            Some(LoroValue::String(s)) => ObjectId::parse(s)?.to_string(),
            _ => return Err(ModelError::Malformed("meta.space")),
        };
        let created_at_ms = int("created_at").ok_or(ModelError::Malformed("meta.created_at"))?;
        let updated_at_ms = int("updated_at").unwrap_or(created_at_ms);
        let deleted_at_ms = int("deleted_at");
        let target_id = match meta.get("target") {
            Some(LoroValue::String(s)) => Some(ObjectId::parse(s)?.to_string()),
            _ => None,
        };
        if kind == ObjectKind::Layer && target_id.is_none() {
            return Err(ModelError::Malformed("layer without target"));
        }

        let mut types: Vec<String> = match self.types().get_value() {
            LoroValue::Map(m) => m.keys().cloned().collect(),
            _ => Vec::new(),
        };
        types.sort();

        let fields = self.read_fields();

        let mut participants = Vec::new();
        if let LoroValue::Map(m) = self.participants().get_value() {
            for (slot, v) in m.iter() {
                let LoroValue::Map(p) = v else {
                    return Err(ModelError::Malformed("participant is not a map"));
                };
                let object_id = match p.get("object") {
                    Some(LoroValue::String(s)) => ObjectId::parse(s)?.to_string(),
                    _ => return Err(ModelError::Malformed("participant.object")),
                };
                let role_id = match p.get("role") {
                    Some(LoroValue::String(s)) => s.to_string(),
                    _ => String::new(),
                };
                participants.push(Participant {
                    slot: slot.clone(),
                    object_id,
                    role_id,
                });
            }
        }
        participants.sort_by(|a, b| a.slot.cmp(&b.slot));

        let mut type_fields: Vec<TypeField> = match self.type_fields().get_value() {
            LoroValue::Map(m) => m
                .iter()
                .map(|(k, v)| TypeField {
                    field_id: k.clone(),
                    required: matches!(v, LoroValue::Bool(true)),
                })
                .collect(),
            _ => Vec::new(),
        };
        type_fields.sort_by(|a, b| a.field_id.cmp(&b.field_id));

        let placements = self.read_placements()?;

        Ok(Object {
            id: self.id.to_string(),
            space_id,
            kind: kind.into(),
            types,
            fields,
            created_at_ms,
            updated_at_ms,
            deleted_at_ms,
            participants,
            target_id,
            type_fields,
            placements,
        })
    }
}

fn placement_id(id: &str) -> Result<TreeID, ModelError> {
    TreeID::try_from(id)
        .map_err(|_| ModelError::Placement(id.to_owned(), "not a placement id".to_owned()))
}

fn tree_parent(tree: &LoroTree, parent_id: &str) -> Result<TreeParentId, ModelError> {
    if parent_id.is_empty() {
        return Ok(TreeParentId::Root);
    }
    Ok(TreeParentId::Node(live_node(tree, parent_id)?))
}

/// Resolves an id that must name a live node of the tree.
fn live_node(tree: &LoroTree, id: &str) -> Result<TreeID, ModelError> {
    let target = placement_id(id)?;
    if !tree.contains(target) || tree.is_node_deleted(&target).unwrap_or(true) {
        return Err(ModelError::Placement(
            id.to_owned(),
            "node does not exist".to_owned(),
        ));
    }
    Ok(target)
}

fn create_node(tree: &LoroTree, parent_id: &str, index: usize) -> Result<TreeID, ModelError> {
    let parent = tree_parent(tree, parent_id)?;
    let index = index.min(tree.children_num(parent).unwrap_or(0));
    tree.create_at(parent, index)
        .map_err(|e| ModelError::Placement(parent_id.to_owned(), e.to_string()))
}

fn move_node(tree: &LoroTree, id: &str, parent_id: &str, index: usize) -> Result<(), ModelError> {
    let target = live_node(tree, id)?;
    let parent = tree_parent(tree, parent_id)?;
    let mut siblings = tree.children_num(parent).unwrap_or(0);
    if tree.parent(target) == Some(parent) {
        siblings = siblings.saturating_sub(1);
    }
    tree.mov_to(target, parent, index.min(siblings))
        .map_err(|e| ModelError::Placement(id.to_owned(), e.to_string()))
}

fn remove_node(tree: &LoroTree, id: &str) -> Result<(), ModelError> {
    let target = live_node(tree, id)?;
    tree.delete(target)
        .map_err(|e| ModelError::Placement(id.to_owned(), e.to_string()))
}

/// Visits every live node in document order: each sibling group whole,
/// then its subtrees in order.
fn walk_tree<T>(
    tree: &LoroTree,
    mut make: impl FnMut(String, String, i32, &LoroMap) -> Result<T, ModelError>,
) -> Result<Vec<T>, ModelError> {
    if tree.is_empty() {
        return Ok(Vec::new());
    }
    let mut by_parent: HashMap<TreeParentId, Vec<(usize, TreeID)>> = HashMap::new();
    for node in tree.get_nodes(false) {
        by_parent
            .entry(node.parent)
            .or_default()
            .push((node.index, node.id));
    }
    for children in by_parent.values_mut() {
        children.sort_unstable();
    }
    let mut out = Vec::new();
    let mut stack: Vec<(TreeParentId, String)> = vec![(TreeParentId::Root, String::new())];
    while let Some((parent, parent_id)) = stack.pop() {
        let Some(children) = by_parent.get(&parent) else {
            continue;
        };
        for &(index, id) in children {
            let index = i32::try_from(index).map_err(|_| ModelError::Malformed("node index"))?;
            out.push(make(
                id.to_string(),
                parent_id.clone(),
                index,
                &tree.get_meta(id)?,
            )?);
        }
        for &(_, id) in children.iter().rev() {
            stack.push((TreeParentId::Node(id), id.to_string()));
        }
    }
    Ok(out)
}

fn write_ui_meta(meta: &LoroMap, node: &UiNode) -> Result<(), ModelError> {
    if node.r#type.trim().is_empty() {
        return Err(ModelError::Placement(
            node.id.clone(),
            "a node needs a type".to_owned(),
        ));
    }
    meta.insert("type", node.r#type.trim())?;
    meta.insert("props", node.props_json.as_str())?;
    meta.insert("bindings", node.bindings_json.as_str())?;
    meta.insert("handlers", node.handlers_json.as_str())?;
    Ok(())
}

/// Per-document settings every instance gets: timestamps on commits and
/// the rich-text styles. Known styles expand the way editors expect; any
/// other key behaves like a plain style that typing at its end extends.
fn configure(doc: &LoroDoc) {
    doc.set_record_timestamp(true);
    doc.config_text_style(StyleConfigMap::default_rich_text_config());
    doc.config_default_text_style(Some(StyleConfig {
        expand: ExpandType::After,
    }));
}

/// The marks of a text as ranges in Unicode scalars, adjacent equal runs
/// merged, in order of start.
fn marks_of(text: &LoroText) -> Vec<Mark> {
    let mut out: Vec<Mark> = Vec::new();
    let mut pos: usize = 0;
    for delta in text.to_delta() {
        let TextDelta::Insert { insert, attributes } = delta else {
            continue;
        };
        let len = insert.chars().count();
        if let Some(attrs) = attributes {
            let mut keys: Vec<_> = attrs.iter().collect();
            keys.sort_by(|a, b| a.0.cmp(b.0));
            for (key, value) in keys {
                if matches!(value, LoroValue::Null) {
                    continue;
                }
                let value_json = loro_json(value);
                let start = i32::try_from(pos).unwrap_or(i32::MAX);
                let end = i32::try_from(pos + len).unwrap_or(i32::MAX);
                if let Some(last) = out
                    .iter_mut()
                    .find(|m| m.key == *key && m.value_json == value_json && m.end == start)
                {
                    last.end = end;
                } else {
                    out.push(Mark {
                        start,
                        end,
                        key: key.clone(),
                        value_json,
                    });
                }
            }
        }
        pos += len;
    }
    out.sort_by_key(|m| (m.start, m.key.clone()));
    out
}

/// Makes the text carry exactly `target`: marks not wanted are lifted,
/// wanted ones not there are set. Untouched marks keep their history.
fn apply_marks(text: &LoroText, target: &[Mark]) -> Result<(), ModelError> {
    let current = marks_of(text);
    let same = |a: &Mark, b: &Mark| {
        a.start == b.start && a.end == b.end && a.key == b.key && a.value_json == b.value_json
    };
    let len = text.len_unicode();
    for m in &current {
        if !target.iter().any(|t| same(t, m)) {
            let (s, e) = clamp(m, len);
            if s < e {
                text.unmark(s..e, &m.key)?;
            }
        }
    }
    for m in target {
        if !current.iter().any(|c| same(c, m)) {
            let (s, e) = clamp(m, len);
            if s < e {
                text.mark(s..e, &m.key, json_loro(&m.value_json))?;
            }
        }
    }
    Ok(())
}

fn clamp(m: &Mark, len: usize) -> (usize, usize) {
    let s = usize::try_from(m.start).unwrap_or(0).min(len);
    let e = usize::try_from(m.end).unwrap_or(0).min(len);
    (s, e)
}

fn loro_json(v: &LoroValue) -> String {
    match v {
        LoroValue::Bool(b) => b.to_string(),
        LoroValue::I64(i) => i.to_string(),
        LoroValue::Double(d) => d.to_string(),
        LoroValue::String(s) => serde_json::Value::String(s.to_string()).to_string(),
        other => format!("{other:?}"),
    }
}

fn json_loro(s: &str) -> LoroValue {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Bool(b)) => LoroValue::from(b),
        Ok(serde_json::Value::Number(n)) => n.as_i64().map_or_else(
            || LoroValue::from(n.as_f64().unwrap_or(0.0)),
            LoroValue::from,
        ),
        Ok(serde_json::Value::String(t)) => LoroValue::from(t.as_str()),
        _ => LoroValue::from(true),
    }
}

/// Turns `list` into `target` with the fewest insertions and deletions, so
/// that concurrent edits to untouched items still merge.
fn apply_list_diff(list: &LoroMovableList, target: &[LoroValue]) -> Result<(), ModelError> {
    let current = list.to_vec();
    let (n, m) = (current.len(), target.len());
    // Longest common subsequence table.
    let mut lcs = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if current[i] == target[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    // Walk the table, editing the live list in place; `pos` tracks the
    // position in the live list as it changes.
    let (mut i, mut j, mut pos) = (0usize, 0usize, 0usize);
    while i < n || j < m {
        if i < n && j < m && current[i] == target[j] {
            i += 1;
            j += 1;
            pos += 1;
        } else if j < m && (i >= n || lcs[i][j + 1] >= lcs[i + 1][j]) {
            list.insert(pos, target[j].clone())?;
            j += 1;
            pos += 1;
        } else {
            list.delete(pos, 1)?;
            i += 1;
        }
    }
    Ok(())
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn bool_value(b: bool) -> Value {
        Value {
            value: Some(value::Value::Bool(b)),
        }
    }

    fn time_value(ms: i64) -> Value {
        Value {
            value: Some(value::Value::TimeMs(ms)),
        }
    }

    #[test]
    fn round_trips_through_snapshot() {
        let id = ObjectId::new();
        let space = ObjectId::new();
        let doc = ObjectDoc::create(id, space, ObjectKind::Entity, 1_000).expect("ok");
        doc.add_type("task").expect("ok");
        doc.set_field("title", &string_value("hello")).expect("ok");
        doc.set_field("done", &bool_value(false)).expect("ok");
        doc.set_field("due", &time_value(42)).expect("ok");
        doc.set_field("parent", &ref_value(space)).expect("ok");
        doc.touch(2_000).expect("ok");

        let bytes = doc.snapshot().expect("ok");
        let loaded = ObjectDoc::from_snapshot(id, &bytes).expect("ok");
        let rec = loaded.read().expect("ok");

        assert_eq!(rec.id, id.to_string());
        assert_eq!(rec.space_id, space.to_string());
        assert_eq!(rec.kind(), ObjectKind::Entity);
        assert_eq!(rec.types, vec!["task".to_owned()]);
        assert_eq!(rec.fields["title"], string_value("hello"));
        assert_eq!(rec.fields["done"], bool_value(false));
        assert_eq!(rec.fields["due"], time_value(42));
        assert_eq!(rec.fields["parent"], ref_value(space));
        assert_eq!(rec.created_at_ms, 1_000);
        assert_eq!(rec.updated_at_ms, 2_000);
        assert_eq!(rec.deleted_at_ms, None);
        assert!(rec.participants.is_empty());
        assert_eq!(rec.target_id, None);
        assert!(rec.type_fields.is_empty());
    }

    #[test]
    fn relations_carry_participants_and_layers_carry_targets() {
        let space = ObjectId::new();
        let a = ObjectId::new();
        let b = ObjectId::new();
        let rel = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Relation, 1).expect("ok");
        rel.set_participant(&Participant {
            slot: "from".into(),
            object_id: a.to_string(),
            role_id: "parent".into(),
        })
        .expect("ok");
        rel.set_participant(&Participant {
            slot: "to".into(),
            object_id: b.to_string(),
            role_id: String::new(),
        })
        .expect("ok");
        rel.touch(2).expect("ok");
        let rec = rel.read().expect("ok");
        assert_eq!(rec.participants.len(), 2);
        assert_eq!(rec.participants[0].slot, "from");
        assert_eq!(rec.participants[0].object_id, a.to_string());
        assert_eq!(rec.participants[0].role_id, "parent");
        assert_eq!(rec.participants[1].object_id, b.to_string());
        rel.remove_participant("to").expect("ok");
        assert_eq!(rel.read().expect("ok").participants.len(), 1);

        let layer = ObjectDoc::create_layer(ObjectId::new(), space, a, 1).expect("ok");
        let rec = layer.read().expect("ok");
        assert_eq!(rec.kind(), ObjectKind::Layer);
        assert_eq!(rec.target_id.as_deref(), Some(a.to_string().as_str()));
    }

    #[test]
    fn rich_text_marks_survive_edits_and_merge() {
        let space = ObjectId::new();
        let doc = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Entity, 1).expect("ok");
        let bold = Mark {
            start: 0,
            end: 5,
            key: "bold".into(),
            value_json: "true".into(),
        };
        let link = Mark {
            start: 6,
            end: 11,
            key: "link".into(),
            value_json: "\"https://x\"".into(),
        };
        doc.set_field(
            "body",
            &rich_value("hello world", vec![bold.clone(), link.clone()]),
        )
        .expect("ok");
        doc.touch(2).expect("ok");
        let Some(value::Value::Rich(r)) = doc.read().expect("ok").fields["body"].value.clone()
        else {
            unreachable!("rich text was written");
        };
        assert_eq!(r.text, "hello world");
        assert_eq!(r.marks, vec![bold.clone(), link.clone()]);

        // Another replica inserts before the marks; they move with the text.
        let other = ObjectDoc::from_snapshot(doc.id(), &doc.snapshot().expect("ok")).expect("ok");
        let before = other.version_vector();
        other
            .set_field("body", &text_value("oh hello world"))
            .expect("ok");
        other.touch(3).expect("ok");
        // Meanwhile this replica lifts the link and sets italic.
        let italic = Mark {
            start: 6,
            end: 11,
            key: "italic".into(),
            value_json: "true".into(),
        };
        doc.set_field(
            "body",
            &rich_value("hello world", vec![bold.clone(), italic]),
        )
        .expect("ok");
        doc.touch(3).expect("ok");
        doc.import(&other.updates_since(&before).expect("ok"))
            .expect("ok");
        let Some(value::Value::Rich(r)) = doc.read().expect("ok").fields["body"].value.clone()
        else {
            unreachable!("rich text");
        };
        assert_eq!(r.text, "oh hello world");
        let keys: Vec<(String, i32, i32)> = r
            .marks
            .iter()
            .map(|m| (m.key.clone(), m.start, m.end))
            .collect();
        assert_eq!(
            keys,
            vec![("bold".into(), 3, 8), ("italic".into(), 9, 14)],
            "{:?}",
            r.marks
        );

        // Plain text written over rich text keeps the marks; a rich value
        // without marks lifts them.
        doc.set_field("body", &text_value("oh hello world!"))
            .expect("ok");
        doc.touch(4).expect("ok");
        assert!(matches!(
            doc.read().expect("ok").fields["body"].value,
            Some(value::Value::Rich(_))
        ));
        doc.set_field("body", &rich_value("oh hello world!", vec![]))
            .expect("ok");
        doc.touch(5).expect("ok");
        assert_eq!(
            doc.read().expect("ok").fields["body"],
            text_value("oh hello world!")
        );
        assert!(kind_accepts(FieldKind::RichText, &text_value("x")));
        assert!(!kind_accepts(FieldKind::Text, &rich_value("x", vec![])));
    }

    #[test]
    fn map_fields_merge_key_by_key() {
        let space = ObjectId::new();
        let doc = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Entity, 1).expect("ok");
        let mut m = HashMap::new();
        m.insert("a".to_owned(), int_value(1));
        m.insert("b".to_owned(), string_value("x"));
        doc.set_field("tags", &map_value(m.clone())).expect("ok");
        doc.touch(2).expect("ok");
        assert_eq!(doc.read().expect("ok").fields["tags"], map_value(m.clone()));

        // Two replicas touch different keys; both land. The same key goes
        // last-writer-wins.
        let other = ObjectDoc::from_snapshot(doc.id(), &doc.snapshot().expect("ok")).expect("ok");
        let before = other.version_vector();
        let mut theirs = m.clone();
        theirs.insert("c".to_owned(), int_value(3));
        theirs.remove("b");
        other.set_field("tags", &map_value(theirs)).expect("ok");
        other.touch(3).expect("ok");
        let mut mine = m.clone();
        mine.insert("a".to_owned(), int_value(10));
        doc.set_field("tags", &map_value(mine)).expect("ok");
        doc.touch(3).expect("ok");
        doc.import(&other.updates_since(&before).expect("ok"))
            .expect("ok");
        let Some(value::Value::Map(mm)) = doc.read().expect("ok").fields["tags"].value.clone()
        else {
            unreachable!("a map was written");
        };
        let merged = mm.entries;
        assert_eq!(merged.get("a"), Some(&int_value(10)));
        assert_eq!(merged.get("c"), Some(&int_value(3)));
        assert!(!merged.contains_key("b"));
        assert!(kind_accepts(FieldKind::Map, &map_value(HashMap::new())));
        assert!(!is_scalar(&map_value(HashMap::new())));
    }

    #[test]
    fn text_fields_merge_and_lists_diff() {
        let space = ObjectId::new();
        let doc = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Entity, 1).expect("ok");
        doc.set_field("body", &text_value("hello world"))
            .expect("ok");
        doc.set_field(
            "tags",
            &list_value(vec![string_value("a"), string_value("b")]),
        )
        .expect("ok");
        doc.touch(2).expect("ok");
        let rec = doc.read().expect("ok");
        assert_eq!(rec.fields["body"], text_value("hello world"));
        assert_eq!(
            rec.fields["tags"],
            list_value(vec![string_value("a"), string_value("b")])
        );

        // A concurrent replica edits the same text and list; both sides keep
        // their changes after merging because edits are minimal.
        let other = ObjectDoc::from_snapshot(doc.id(), &doc.snapshot().expect("ok")).expect("ok");
        other
            .set_field("body", &text_value("hello big world"))
            .expect("ok");
        other
            .set_field(
                "tags",
                &list_value(vec![
                    string_value("a"),
                    string_value("b"),
                    string_value("c"),
                ]),
            )
            .expect("ok");
        other.touch(3).expect("ok");
        doc.set_field("body", &text_value("hello world!"))
            .expect("ok");
        doc.set_field("tags", &list_value(vec![string_value("b")]))
            .expect("ok");
        doc.touch(3).expect("ok");

        let from_other = other.updates_since(&VersionVector::default()).expect("ok");
        assert!(doc.import(&from_other).expect("ok"));
        let merged = doc.read().expect("ok");
        assert_eq!(merged.fields["body"], text_value("hello big world!"));
        assert_eq!(
            merged.fields["tags"],
            list_value(vec![string_value("b"), string_value("c")])
        );

        // Changing a field's shape replaces it.
        doc.set_field("body", &string_value("plain")).expect("ok");
        doc.set_field("tags", &text_value("now text")).expect("ok");
        let rec = doc.read().expect("ok");
        assert_eq!(rec.fields["body"], string_value("plain"));
        assert_eq!(rec.fields["tags"], text_value("now text"));
        assert!(matches!(
            doc.set_field("bad", &list_value(vec![text_value("nested")])),
            Err(ModelError::NestedContainer(_))
        ));
    }

    #[test]
    fn definitions_carry_their_attributes() {
        let space = ObjectId::new();
        let fdef = ObjectDoc::create(ObjectId::new(), space, ObjectKind::FieldDef, 1).expect("ok");
        fdef.set_field(reserved::NAME, &string_value("Due"))
            .expect("ok");
        fdef.set_field(
            reserved::FIELD_KIND,
            &int_value(i64::from(FieldKind::Time as i32)),
        )
        .expect("ok");
        fdef.touch(2).expect("ok");
        let rec = fdef.read().expect("ok");
        assert_eq!(field_def_kind(&rec), Some(FieldKind::Time));
        assert_eq!(string_field(&rec, reserved::NAME), Some("Due"));
        assert!(kind_accepts(FieldKind::Time, &time_value(1)));
        assert!(!kind_accepts(FieldKind::Time, &int_value(1)));

        let tdef = ObjectDoc::create(ObjectId::new(), space, ObjectKind::TypeDef, 1).expect("ok");
        tdef.set_type_field(&fdef.id().to_string(), true)
            .expect("ok");
        tdef.set_type_field("free", false).expect("ok");
        tdef.touch(2).expect("ok");
        let rec = tdef.read().expect("ok");
        assert_eq!(rec.type_fields.len(), 2);
        assert!(rec
            .type_fields
            .iter()
            .any(|f| f.field_id == fdef.id().to_string() && f.required));
        tdef.remove_type_field("free").expect("ok");
        assert_eq!(tdef.read().expect("ok").type_fields.len(), 1);
    }

    #[test]
    fn placements_form_a_tree_and_merge() {
        let space = ObjectId::new();
        let doc = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Entity, 1).expect("ok");
        let (b1, b2, b3) = (ObjectId::new(), ObjectId::new(), ObjectId::new());
        let p1 = doc.place_block("", 0, b1).expect("ok");
        let p2 = doc.place_block("", 99, b2).expect("ok");
        let p3 = doc.place_block(&p1, 0, b3).expect("ok");
        doc.touch(2).expect("ok");

        let rec = doc.read().expect("ok");
        let ids: Vec<_> = rec.placements.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, [p1.as_str(), p2.as_str(), p3.as_str()]);
        assert_eq!(rec.placements[1].index, 1);
        assert_eq!(rec.placements[2].parent_id, p1);
        assert_eq!(rec.placements[2].block_id, b3.to_string());

        // Move p2 to the front; a move into its own subtree is refused.
        doc.move_placement(&p2, "", 0).expect("ok");
        assert!(doc.move_placement(&p1, &p3, 0).is_err());
        assert!(doc.place_block("nope", 0, b1).is_err());
        doc.touch(3).expect("ok");
        let rec = doc.read().expect("ok");
        assert_eq!(rec.placements[0].id, p2);
        assert_eq!(rec.placements[0].index, 0);
        assert_eq!(rec.placements[1].id, p1);

        // Survives the snapshot round trip and a concurrent placement.
        let other = ObjectDoc::from_snapshot(doc.id(), &doc.snapshot().expect("ok")).expect("ok");
        let before = other.version_vector();
        let p4 = other.place_block("", 0, b1).expect("ok");
        other.touch(4).expect("ok");
        doc.remove_placement(&p1).expect("ok");
        doc.touch(4).expect("ok");
        doc.import(&other.updates_since(&before).expect("ok"))
            .expect("ok");
        let rec = doc.read().expect("ok");
        let ids: Vec<_> = rec.placements.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, [p4.as_str(), p2.as_str()]);
        assert!(doc.remove_placement(&p3).is_err());
    }
}
