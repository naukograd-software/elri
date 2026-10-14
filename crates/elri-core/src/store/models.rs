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
#[index(object_id, change_id)]
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

/// A Change that builds on another: it writes to a document the other
/// changed before acceptance, or refers to an Object the other created.
#[derive(Debug, toasty::Model)]
#[table = "change_deps"]
#[index(depends_on, change_id)]
pub struct ChangeDepRow {
    /// The dependent Change.
    #[key]
    pub change_id: Uuid,
    /// The Change it needs accepted first.
    #[key]
    pub depends_on: Uuid,
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

/// Projection: fields of a type definition.
#[derive(Debug, toasty::Model)]
#[table = "type_fields"]
#[index(field_id, type_id)]
pub struct TypeFieldRow {
    /// Type-definition id.
    #[key]
    pub type_id: Uuid,
    /// Field-definition id (a string: ids of unknown definitions are allowed).
    #[key]
    pub field_id: String,
    /// Whether objects of the type must carry the field.
    pub required: bool,
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

/// Projection: placements of blocks inside Entities.
#[derive(Debug, toasty::Model)]
#[table = "placements"]
#[index(block_id, document_id)]
pub struct PlacementRow {
    /// The Entity holding the placement.
    #[key]
    pub document_id: Uuid,
    /// Placement id within the document.
    #[key]
    pub placement_id: String,
    /// Parent placement id; empty at the top level.
    pub parent_id: String,
    /// Position among siblings.
    pub index: i64,
    /// The placed block.
    pub block_id: Uuid,
}

/// Accepted Changes in log order; the server's discovery sequence and the
/// client's record of what it has installed.
#[derive(Debug, toasty::Model)]
#[table = "space_log"]
pub struct SpaceLogRow {
    /// Position in the Space log.
    #[key]
    pub seq: i64,
    /// Accepted Change id.
    #[unique]
    pub change_id: Uuid,
    /// Acceptance instant, ms since epoch.
    pub accepted_at: i64,
}

/// Small per-Space state: the highest server sequence installed locally.
#[derive(Debug, toasty::Model)]
#[table = "sync_state"]
pub struct SyncStateRow {
    /// Key, e.g. `cursor`.
    #[key]
    pub key: String,
    /// Value.
    pub value: i64,
}

/// The server this device synchronizes with; at most one row.
#[derive(Debug, toasty::Model)]
#[table = "remote"]
pub struct RemoteRow {
    /// Always `server`.
    #[key]
    pub key: String,
    /// WebSocket URL.
    pub url: String,
    /// Access token.
    pub token: String,
    /// User id on the server.
    pub user_id: String,
    /// User name on the server.
    pub user_name: String,
}
