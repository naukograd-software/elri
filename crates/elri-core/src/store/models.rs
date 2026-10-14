//! Table definitions. The device database holds the Space registry; every
//! Space database holds documents, their update log, Changes, and the
//! projections that queries use.

use uuid::Uuid;

/// A Space registered on this device.
#[derive(Debug, toasty::Model)]
#[table = "spaces"]
pub struct SpaceRow {
    /// Space id.
    #[key]
    pub id: Uuid,
    /// Display name.
    pub name: String,
    /// `Authority` code.
    pub authority: i64,
    /// Creation instant, ms since epoch.
    #[index]
    pub created_at: i64,
}

/// Compacted snapshot of one document.
#[derive(Debug, toasty::Model)]
#[table = "docs"]
pub struct DocRow {
    /// Object id.
    #[key]
    pub object_id: Uuid,
    /// `ObjectKind` code.
    pub kind: i64,
    /// Loro snapshot.
    pub snapshot: Vec<u8>,
    /// Encoded version vector of the snapshot.
    pub version: Vec<u8>,
    /// Last change instant, ms since epoch.
    pub updated_at: i64,
}

/// Operations logged after a document's snapshot, replayed on load.
#[derive(Debug, toasty::Model)]
#[table = "doc_updates"]
#[index(object_id, seq)]
pub struct DocUpdateRow {
    /// Log position.
    #[key]
    #[auto]
    pub seq: u64,
    /// Object id.
    pub object_id: Uuid,
    /// The Change that produced these operations.
    pub change_id: Uuid,
    /// Loro update bytes.
    pub bytes: Vec<u8>,
}

/// A recorded Change.
#[derive(Debug, toasty::Model)]
#[table = "changes"]
pub struct ChangeRow {
    /// Local order of recording.
    #[key]
    #[auto]
    pub seq: u64,
    /// Change id.
    #[unique]
    pub id: Uuid,
    /// Producing device.
    pub device_id: String,
    /// Creation instant, ms since epoch.
    pub created_at: i64,
    /// `ChangeStatus` code.
    #[index]
    pub status: i64,
    /// Position in the Space log once accepted.
    pub accepted_seq: Option<i64>,
    /// Why it was rejected, if it was.
    pub rejection: Option<String>,
}

/// The operations a Change added to one document.
#[derive(Debug, toasty::Model)]
#[table = "change_parts"]
pub struct ChangePartRow {
    /// Change id.
    #[key]
    pub change_id: Uuid,
    /// Object id.
    #[key]
    pub object_id: Uuid,
    /// `ObjectKind` code of the document.
    pub kind: i64,
    /// Encoded version vector the operations build on.
    #[column("from_version")]
    pub base_version: Vec<u8>,
    /// Encoded version vector after the operations.
    #[column("to_version")]
    pub next_version: Vec<u8>,
    /// Loro update bytes.
    pub bytes: Vec<u8>,
}

/// Projection: one row per Object.
#[derive(Debug, toasty::Model)]
#[table = "objects"]
#[index(kind, deleted_at, created_at)]
pub struct ObjectRow {
    /// Object id.
    #[key]
    pub object_id: Uuid,
    /// `ObjectKind` code.
    pub kind: i64,
    /// Creation instant.
    pub created_at: i64,
    /// Last change instant.
    pub updated_at: i64,
    /// Deletion instant, if deleted.
    pub deleted_at: Option<i64>,
    /// Layers only: the enriched Object.
    #[index]
    pub target_id: Option<Uuid>,
}

/// Projection: type definitions applied to an Object.
#[derive(Debug, toasty::Model)]
#[table = "object_types"]
#[index(type_id, object_id)]
pub struct ObjectTypeRow {
    /// Object id.
    #[key]
    pub object_id: Uuid,
    /// Type-definition id.
    #[key]
    pub type_id: String,
}

/// Projection: one scalar field value, typed by column.
#[derive(Debug, toasty::Model)]
#[table = "field_values"]
#[index(field_id, object_id)]
pub struct FieldValueRow {
    /// Object id.
    #[key]
    pub object_id: Uuid,
    /// Field-definition id.
    #[key]
    pub field_id: String,
    /// Boolean value.
    pub v_bool: Option<bool>,
    /// Integer value.
    pub v_int: Option<i64>,
    /// Float value.
    pub v_real: Option<f64>,
    /// Text value.
    pub v_text: Option<String>,
    /// Instant value, ms since epoch.
    pub v_time: Option<i64>,
    /// Reference value.
    #[index]
    pub v_ref: Option<Uuid>,
}

/// Projection: participants of a Relation.
#[derive(Debug, toasty::Model)]
#[table = "participants"]
#[index(object_id, relation_id)]
pub struct ParticipantRow {
    /// Relation id.
    #[key]
    pub relation_id: Uuid,
    /// Slot within the Relation.
    #[key]
    pub slot: String,
    /// Participating Object.
    pub object_id: Uuid,
    /// Participant-role definition id; empty when none.
    pub role_id: String,
}
