//! Objects and their documents.
//!
//! Every Object is one Loro document with three root maps:
//!
//! - `meta`: `kind`, `space`, `created_at`, `updated_at`, `deleted_at`, and
//!   `target` for Layers
//! - `types`: type-definition id → `true`
//! - `fields`: field-definition id → value
//! - `participants` (Relations): slot id → `{object, role}`
//!
//! Scalar values live directly in `fields`. References and timestamps are
//! tagged maps (`{"$ref": id}`, `{"$time": ms}`) so that they survive the
//! round trip through Loro's value model unambiguously.

use std::collections::{BTreeMap, HashMap};

use loro::{ExportMode, LoroDoc, LoroMap, LoroValue, VersionVector};
use uuid::Uuid;

/// Identity of an Object: a `UUIDv7`.
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

/// What an Object is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectKind {
    /// A thing in the graph.
    Entity,
    /// A connection between participants; an Object in its own right.
    Relation,
    /// Independent values over another Object.
    Layer,
}

impl ObjectKind {
    /// Stable number used in documents and projections.
    pub fn code(self) -> i64 {
        match self {
            Self::Entity => 1,
            Self::Relation => 2,
            Self::Layer => 3,
        }
    }

    /// Inverse of [`ObjectKind::code`].
    pub fn from_code(code: i64) -> Option<Self> {
        match code {
            1 => Some(Self::Entity),
            2 => Some(Self::Relation),
            3 => Some(Self::Layer),
            _ => None,
        }
    }
}

/// A scalar field value.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// Boolean.
    Bool(bool),
    /// 64-bit integer.
    Int(i64),
    /// Double-precision float.
    Double(f64),
    /// Text.
    String(String),
    /// Instant, milliseconds since the Unix epoch.
    TimeMs(i64),
    /// Reference to another Object.
    Ref(ObjectId),
}

const REF_TAG: &str = "$ref";
const TIME_TAG: &str = "$time";

impl FieldValue {
    fn to_loro(&self) -> LoroValue {
        match self {
            Self::Bool(b) => LoroValue::from(*b),
            Self::Int(i) => LoroValue::from(*i),
            Self::Double(d) => LoroValue::from(*d),
            Self::String(s) => LoroValue::from(s.as_str()),
            Self::TimeMs(ms) => tagged(TIME_TAG, LoroValue::from(*ms)),
            Self::Ref(id) => tagged(REF_TAG, LoroValue::from(id.to_string())),
        }
    }

    fn from_loro(v: &LoroValue) -> Option<Self> {
        match v {
            LoroValue::Bool(b) => Some(Self::Bool(*b)),
            LoroValue::I64(i) => Some(Self::Int(*i)),
            LoroValue::Double(d) => Some(Self::Double(*d)),
            LoroValue::String(s) => Some(Self::String(s.to_string())),
            LoroValue::Map(m) => {
                if let Some(LoroValue::I64(ms)) = m.get(TIME_TAG) {
                    return Some(Self::TimeMs(*ms));
                }
                if let Some(LoroValue::String(id)) = m.get(REF_TAG) {
                    return ObjectId::parse(id).ok().map(Self::Ref);
                }
                None
            }
            _ => None,
        }
    }
}

fn tagged(tag: &str, value: LoroValue) -> LoroValue {
    let mut m = HashMap::new();
    m.insert(tag.to_owned(), value);
    LoroValue::from(m)
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
    /// Loro refused an operation.
    #[error(transparent)]
    Loro(#[from] loro::LoroError),
    /// Loro could not encode the document.
    #[error(transparent)]
    Encode(#[from] loro::LoroEncodeError),
}

/// One participant of a Relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Participant {
    /// Stable slot id, unique within the Relation.
    pub slot: String,
    /// The participating Object.
    pub object: ObjectId,
    /// Participant-role definition id; empty when none.
    pub role: String,
}

/// A plain, fully materialized view of an Object.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectRecord {
    /// Identity.
    pub id: ObjectId,
    /// Owning Space.
    pub space: SpaceId,
    /// Kind.
    pub kind: ObjectKind,
    /// Applied type-definition ids, sorted.
    pub types: Vec<String>,
    /// Field-definition id → value, sorted by key.
    pub fields: BTreeMap<String, FieldValue>,
    /// Creation instant, ms since epoch.
    pub created_at_ms: i64,
    /// Last change instant, ms since epoch.
    pub updated_at_ms: i64,
    /// Deletion instant, if deleted.
    pub deleted_at_ms: Option<i64>,
    /// Relations only, sorted by slot.
    pub participants: Vec<Participant>,
    /// Layers only: the enriched Object.
    pub target: Option<ObjectId>,
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
        doc.set_record_timestamp(true);
        let meta = doc.get_map("meta");
        meta.insert("kind", kind.code())?;
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
        doc.set_record_timestamp(true);
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
        doc.set_record_timestamp(true);
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

    /// Adds or replaces a participant by slot.
    pub fn set_participant(&self, p: &Participant) -> Result<(), ModelError> {
        let mut m = HashMap::new();
        m.insert("object".to_owned(), LoroValue::from(p.object.to_string()));
        m.insert("role".to_owned(), LoroValue::from(p.role.as_str()));
        self.participants().insert(&p.slot, LoroValue::from(m))?;
        Ok(())
    }

    /// Removes a participant slot.
    pub fn remove_participant(&self, slot: &str) -> Result<(), ModelError> {
        self.participants().delete(slot)?;
        Ok(())
    }

    /// Sets a scalar field.
    pub fn set_field(&self, field_id: &str, value: &FieldValue) -> Result<(), ModelError> {
        self.fields().insert(field_id, value.to_loro())?;
        Ok(())
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

    /// Materializes the current state.
    pub fn read(&self) -> Result<ObjectRecord, ModelError> {
        let meta = self.meta().get_value();
        let LoroValue::Map(meta) = meta else {
            return Err(ModelError::Malformed("meta is not a map"));
        };
        let int = |key: &str| match meta.get(key) {
            Some(LoroValue::I64(i)) => Some(*i),
            _ => None,
        };
        let kind = int("kind")
            .and_then(ObjectKind::from_code)
            .ok_or(ModelError::Malformed("meta.kind"))?;
        let space = match meta.get("space") {
            Some(LoroValue::String(s)) => ObjectId::parse(s)?,
            _ => return Err(ModelError::Malformed("meta.space")),
        };
        let created_at_ms = int("created_at").ok_or(ModelError::Malformed("meta.created_at"))?;
        let updated_at_ms = int("updated_at").unwrap_or(created_at_ms);
        let deleted_at_ms = int("deleted_at");
        let target = match meta.get("target") {
            Some(LoroValue::String(s)) => Some(ObjectId::parse(s)?),
            _ => None,
        };
        if kind == ObjectKind::Layer && target.is_none() {
            return Err(ModelError::Malformed("layer without target"));
        }

        let mut participants = Vec::new();
        if let LoroValue::Map(m) = self.participants().get_value() {
            for (slot, v) in m.iter() {
                let LoroValue::Map(p) = v else {
                    return Err(ModelError::Malformed("participant is not a map"));
                };
                let object = match p.get("object") {
                    Some(LoroValue::String(s)) => ObjectId::parse(s)?,
                    _ => return Err(ModelError::Malformed("participant.object")),
                };
                let role = match p.get("role") {
                    Some(LoroValue::String(s)) => s.to_string(),
                    _ => String::new(),
                };
                participants.push(Participant {
                    slot: slot.clone(),
                    object,
                    role,
                });
            }
        }
        participants.sort_by(|a, b| a.slot.cmp(&b.slot));

        let mut types: Vec<String> = match self.types().get_value() {
            LoroValue::Map(m) => m.keys().cloned().collect(),
            _ => Vec::new(),
        };
        types.sort();

        let fields = match self.fields().get_value() {
            LoroValue::Map(m) => m
                .iter()
                .filter_map(|(k, v)| FieldValue::from_loro(v).map(|v| (k.clone(), v)))
                .collect(),
            _ => BTreeMap::new(),
        };

        Ok(ObjectRecord {
            id: self.id,
            space,
            kind,
            types,
            fields,
            created_at_ms,
            updated_at_ms,
            deleted_at_ms,
            participants,
            target,
        })
    }
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

    #[test]
    fn round_trips_through_snapshot() {
        let id = ObjectId::new();
        let space = ObjectId::new();
        let doc = ObjectDoc::create(id, space, ObjectKind::Entity, 1_000).expect("ok");
        doc.add_type("task").expect("ok");
        doc.set_field("title", &FieldValue::String("hello".into()))
            .expect("ok");
        doc.set_field("done", &FieldValue::Bool(false)).expect("ok");
        doc.set_field("due", &FieldValue::TimeMs(42)).expect("ok");
        doc.set_field("parent", &FieldValue::Ref(space))
            .expect("ok");
        doc.touch(2_000).expect("ok");

        let bytes = doc.snapshot().expect("ok");
        let loaded = ObjectDoc::from_snapshot(id, &bytes).expect("ok");
        let rec = loaded.read().expect("ok");

        assert_eq!(rec.id, id);
        assert_eq!(rec.space, space);
        assert_eq!(rec.kind, ObjectKind::Entity);
        assert_eq!(rec.types, vec!["task".to_owned()]);
        assert_eq!(rec.fields["title"], FieldValue::String("hello".into()));
        assert_eq!(rec.fields["done"], FieldValue::Bool(false));
        assert_eq!(rec.fields["due"], FieldValue::TimeMs(42));
        assert_eq!(rec.fields["parent"], FieldValue::Ref(space));
        assert_eq!(rec.created_at_ms, 1_000);
        assert_eq!(rec.updated_at_ms, 2_000);
        assert_eq!(rec.deleted_at_ms, None);
        assert!(rec.participants.is_empty());
        assert_eq!(rec.target, None);
    }

    #[test]
    fn relations_carry_participants_and_layers_carry_targets() {
        let space = ObjectId::new();
        let a = ObjectId::new();
        let b = ObjectId::new();
        let rel = ObjectDoc::create(ObjectId::new(), space, ObjectKind::Relation, 1).expect("ok");
        rel.set_participant(&Participant {
            slot: "from".into(),
            object: a,
            role: "parent".into(),
        })
        .expect("ok");
        rel.set_participant(&Participant {
            slot: "to".into(),
            object: b,
            role: String::new(),
        })
        .expect("ok");
        rel.touch(2).expect("ok");
        let rec = rel.read().expect("ok");
        assert_eq!(rec.participants.len(), 2);
        assert_eq!(rec.participants[0].slot, "from");
        assert_eq!(rec.participants[0].object, a);
        assert_eq!(rec.participants[0].role, "parent");
        assert_eq!(rec.participants[1].object, b);
        rel.remove_participant("to").expect("ok");
        assert_eq!(rel.read().expect("ok").participants.len(), 1);

        let layer = ObjectDoc::create_layer(ObjectId::new(), space, a, 1).expect("ok");
        let rec = layer.read().expect("ok");
        assert_eq!(rec.kind, ObjectKind::Layer);
        assert_eq!(rec.target, Some(a));
    }
}
