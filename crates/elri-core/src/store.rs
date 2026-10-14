//! Storage: one Turso database per device and one per Space.
//!
//! The device database holds the registry of Spaces. A Space database holds
//! the source of truth (Object document snapshots) together with projections
//! that queries use; both change in one transaction.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value};

use crate::change::{Change, ChangeId, ChangePart, ChangeStatus};
use crate::device::DeviceId;
use crate::model::{now_ms, ModelError, ObjectDoc, ObjectId, ObjectKind, ObjectRecord, SpaceId};

/// Updates folded into a fresh snapshot once this many accumulate per document.
const COMPACT_AFTER_UPDATES: i64 = 64;

/// Storage failures.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database engine failed.
    #[error(transparent)]
    Db(#[from] turso::Error),
    /// Document handling failed.
    #[error(transparent)]
    Model(#[from] ModelError),
    /// File system failure.
    #[error("{path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// No such Space on this device.
    #[error("space {0} not found")]
    SpaceNotFound(SpaceId),
    /// No such Object in the Space.
    #[error("object {0} not found")]
    ObjectNotFound(ObjectId),
    /// A stored row does not have the expected shape.
    #[error("corrupt row in {table}: {what}")]
    Corrupt {
        /// Table involved.
        table: &'static str,
        /// What was wrong.
        what: &'static str,
    },
}

/// Where a Space's registry lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authority {
    /// Local-only; this core is the registry.
    Local,
    /// Replicated from the server.
    Server,
}

impl Authority {
    fn code(self) -> i64 {
        match self {
            Self::Local => 1,
            Self::Server => 2,
        }
    }

    fn from_code(code: i64) -> Option<Self> {
        match code {
            1 => Some(Self::Local),
            2 => Some(Self::Server),
            _ => None,
        }
    }
}

/// A Space as the device registry knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceInfo {
    /// Identity.
    pub id: SpaceId,
    /// Display name.
    pub name: String,
    /// Registry location.
    pub authority: Authority,
    /// Creation instant, ms since epoch.
    pub created_at_ms: i64,
}

const DEVICE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS spaces (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    authority   INTEGER NOT NULL,
    created_at  INTEGER NOT NULL
);
";

const SPACE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS docs (
    object_id   TEXT PRIMARY KEY,
    kind        INTEGER NOT NULL,
    snapshot    BLOB NOT NULL,
    version     BLOB NOT NULL,
    updated_at  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS doc_updates (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id   TEXT NOT NULL,
    change_id   TEXT NOT NULL,
    bytes       BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS doc_updates_object ON doc_updates (object_id, seq);
CREATE TABLE IF NOT EXISTS changes (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    id          TEXT NOT NULL UNIQUE,
    device_id   TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    status      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS change_parts (
    change_id   TEXT NOT NULL,
    object_id   TEXT NOT NULL,
    from_version BLOB NOT NULL,
    to_version  BLOB NOT NULL,
    bytes       BLOB NOT NULL,
    PRIMARY KEY (change_id, object_id)
);
CREATE TABLE IF NOT EXISTS objects (
    object_id   TEXT PRIMARY KEY,
    kind        INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    deleted_at  INTEGER,
    target_id   TEXT
);
CREATE INDEX IF NOT EXISTS objects_kind ON objects (kind, deleted_at, created_at);
CREATE INDEX IF NOT EXISTS objects_target ON objects (target_id) WHERE target_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS participants (
    relation_id TEXT NOT NULL,
    slot        TEXT NOT NULL,
    object_id   TEXT NOT NULL,
    role_id     TEXT NOT NULL,
    PRIMARY KEY (relation_id, slot)
);
CREATE INDEX IF NOT EXISTS participants_object ON participants (object_id, relation_id);
CREATE TABLE IF NOT EXISTS object_types (
    object_id   TEXT NOT NULL,
    type_id     TEXT NOT NULL,
    PRIMARY KEY (object_id, type_id)
);
CREATE INDEX IF NOT EXISTS object_types_type ON object_types (type_id, object_id);
CREATE TABLE IF NOT EXISTS field_values (
    object_id   TEXT NOT NULL,
    field_id    TEXT NOT NULL,
    v_bool      INTEGER,
    v_int       INTEGER,
    v_real      REAL,
    v_text      TEXT,
    v_time      INTEGER,
    v_ref       TEXT,
    PRIMARY KEY (object_id, field_id)
);
CREATE INDEX IF NOT EXISTS field_values_field ON field_values (field_id, object_id);
CREATE INDEX IF NOT EXISTS field_values_ref ON field_values (v_ref) WHERE v_ref IS NOT NULL;
";

async fn open_db(path: &Path, schema: &str) -> Result<Database, StoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| StoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let db = Builder::new_local(&path.to_string_lossy()).build().await?;
    let conn = db.connect()?;
    for stmt in schema.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        conn.execute(stmt, ()).await?;
    }
    Ok(db)
}

fn text(v: &Value, table: &'static str, what: &'static str) -> Result<String, StoreError> {
    match v {
        Value::Text(s) => Ok(s.clone()),
        _ => Err(StoreError::Corrupt { table, what }),
    }
}

fn int(v: &Value, table: &'static str, what: &'static str) -> Result<i64, StoreError> {
    match v {
        Value::Integer(i) => Ok(*i),
        _ => Err(StoreError::Corrupt { table, what }),
    }
}

fn blob(v: &Value, table: &'static str, what: &'static str) -> Result<Vec<u8>, StoreError> {
    match v {
        Value::Blob(b) => Ok(b.clone()),
        _ => Err(StoreError::Corrupt { table, what }),
    }
}

/// Device-level storage: the Space registry and the open Space databases.
pub struct Store {
    root: PathBuf,
    device_id: DeviceId,
    device: Mutex<Connection>,
    spaces: Mutex<HashMap<SpaceId, Arc<SpaceStore>>>,
}

impl Store {
    /// Opens `root/device.db` and prepares `root/spaces/` for Space files.
    pub async fn open(root: impl Into<PathBuf>, device_id: DeviceId) -> Result<Self, StoreError> {
        let root = root.into();
        let db = open_db(&root.join("device.db"), DEVICE_SCHEMA).await?;
        Ok(Self {
            root,
            device_id,
            device: Mutex::new(db.connect()?),
            spaces: Mutex::new(HashMap::new()),
        })
    }

    /// Every Space registered on this device, newest first.
    pub async fn list_spaces(&self) -> Result<Vec<SpaceInfo>, StoreError> {
        let conn = self.device.lock().await;
        let mut rows = conn
            .query(
                "SELECT id, name, authority, created_at FROM spaces ORDER BY created_at DESC",
                (),
            )
            .await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(SpaceInfo {
                id: ObjectId::parse(&text(&row.get_value(0)?, "spaces", "id")?)?,
                name: text(&row.get_value(1)?, "spaces", "name")?,
                authority: Authority::from_code(int(&row.get_value(2)?, "spaces", "authority")?)
                    .ok_or(StoreError::Corrupt {
                        table: "spaces",
                        what: "authority",
                    })?,
                created_at_ms: int(&row.get_value(3)?, "spaces", "created_at")?,
            });
        }
        Ok(out)
    }

    /// Registers a new local-only Space and creates its database.
    pub async fn create_space(&self, name: &str) -> Result<SpaceInfo, StoreError> {
        let info = SpaceInfo {
            id: SpaceId::new(),
            name: name.to_owned(),
            authority: Authority::Local,
            created_at_ms: now_ms(),
        };
        let space = SpaceStore::open(&self.space_path(info.id), self.device_id).await?;
        {
            let conn = self.device.lock().await;
            conn.execute(
                "INSERT INTO spaces (id, name, authority, created_at) VALUES (?, ?, ?, ?)",
                (
                    info.id.to_string(),
                    info.name.clone(),
                    info.authority.code(),
                    info.created_at_ms,
                ),
            )
            .await?;
        }
        self.spaces.lock().await.insert(info.id, Arc::new(space));
        Ok(info)
    }

    /// The storage of one Space, opened on first use.
    pub async fn space(&self, id: SpaceId) -> Result<Arc<SpaceStore>, StoreError> {
        if let Some(s) = self.spaces.lock().await.get(&id) {
            return Ok(Arc::clone(s));
        }
        let known = {
            let conn = self.device.lock().await;
            let mut rows = conn
                .query("SELECT 1 FROM spaces WHERE id = ?", (id.to_string(),))
                .await?;
            rows.next().await?.is_some()
        };
        if !known {
            return Err(StoreError::SpaceNotFound(id));
        }
        let space = Arc::new(SpaceStore::open(&self.space_path(id), self.device_id).await?);
        self.spaces
            .lock()
            .await
            .entry(id)
            .or_insert_with(|| Arc::clone(&space));
        Ok(space)
    }

    fn space_path(&self, id: SpaceId) -> PathBuf {
        self.root.join("spaces").join(format!("{id}.db"))
    }
}

/// Filter for listing Objects.
#[derive(Debug, Clone, Default)]
pub struct ObjectFilter {
    /// Only this kind, if set.
    pub kind: Option<ObjectKind>,
    /// Only Objects with this type definition, if set.
    pub type_id: Option<String>,
    /// Include deleted Objects.
    pub include_deleted: bool,
    /// Maximum rows.
    pub limit: usize,
    /// Only Relations in which this Object participates, if set.
    pub participant: Option<ObjectId>,
    /// Only Layers over this Object, if set.
    pub target: Option<ObjectId>,
}

/// Storage of one Space: documents, their update log, Changes, and
/// projections, with one writer.
pub struct SpaceStore {
    device_id: DeviceId,
    conn: Mutex<Connection>,
}

impl SpaceStore {
    async fn open(path: &Path, device_id: DeviceId) -> Result<Self, StoreError> {
        let db = open_db(path, SPACE_SCHEMA).await?;
        Ok(Self {
            device_id,
            conn: Mutex::new(db.connect()?),
        })
    }

    /// Stores a freshly created document as one Change with a full snapshot.
    pub async fn insert(&self, doc: &ObjectDoc) -> Result<ObjectRecord, StoreError> {
        let rec = doc.read()?;
        let part = ChangePart {
            object_id: rec.id,
            from_version: loro::VersionVector::default().encode(),
            to_version: doc.version(),
            bytes: doc.updates_since(&loro::VersionVector::default())?,
        };
        let change = self.new_change(vec![part]);
        let mut conn = self.conn.lock().await;
        let tx = conn.transaction().await?;
        write_change(&tx, &change).await?;
        write_snapshot(&tx, doc, &rec).await?;
        write_projections(&tx, &rec).await?;
        tx.commit().await?;
        Ok(rec)
    }

    fn new_change(&self, parts: Vec<ChangePart>) -> Change {
        Change {
            id: ChangeId::new(),
            device_id: self.device_id.as_string(),
            created_at_ms: now_ms(),
            status: ChangeStatus::Local,
            parts,
        }
    }

    /// Loads a document by id.
    pub async fn load(&self, id: ObjectId) -> Result<ObjectDoc, StoreError> {
        let conn = self.conn.lock().await;
        load_doc(&conn, id).await
    }

    /// Materializes an Object by id.
    pub async fn get(&self, id: ObjectId) -> Result<ObjectRecord, StoreError> {
        Ok(self.load(id).await?.read()?)
    }

    /// Loads a document, applies `edit`, and records the result as one
    /// Change: the new operations go to the update log and the outbox, the
    /// projections are rewritten, all in one transaction. The snapshot is
    /// refreshed once enough updates have accumulated.
    pub async fn update<F>(&self, id: ObjectId, edit: F) -> Result<ObjectRecord, StoreError>
    where
        F: FnOnce(&ObjectDoc) -> Result<(), ModelError>,
    {
        let mut conn = self.conn.lock().await;
        let (doc, pending) = load_doc_with_count(&conn, id).await?;
        let before = doc.version_vector();
        edit(&doc)?;
        doc.touch(now_ms())?;
        let rec = doc.read()?;
        let part = ChangePart {
            object_id: id,
            from_version: before.encode(),
            to_version: doc.version(),
            bytes: doc.updates_since(&before)?,
        };
        let change = self.new_change(vec![part.clone()]);

        let tx = conn.transaction().await?;
        write_change(&tx, &change).await?;
        tx.execute(
            "INSERT INTO doc_updates (object_id, change_id, bytes) VALUES (?, ?, ?)",
            (id.to_string(), change.id.to_string(), part.bytes),
        )
        .await?;
        if pending + 1 >= COMPACT_AFTER_UPDATES {
            write_snapshot(&tx, &doc, &rec).await?;
            tx.execute(
                "DELETE FROM doc_updates WHERE object_id = ?",
                (id.to_string(),),
            )
            .await?;
        }
        write_projections(&tx, &rec).await?;
        tx.commit().await?;
        Ok(rec)
    }

    /// Changes recorded in this Space, newest first.
    pub async fn list_changes(&self, limit: usize) -> Result<Vec<Change>, StoreError> {
        let limit = if limit == 0 { 50 } else { limit };
        let conn = self.conn.lock().await;
        let mut rows = conn
            .query(
                "SELECT id, device_id, created_at, status FROM changes ORDER BY seq DESC LIMIT ?",
                (i64::try_from(limit).unwrap_or(i64::MAX),),
            )
            .await?;
        let mut changes = Vec::new();
        while let Some(row) = rows.next().await? {
            changes.push(Change {
                id: ObjectId::parse(&text(&row.get_value(0)?, "changes", "id")?)?,
                device_id: text(&row.get_value(1)?, "changes", "device_id")?,
                created_at_ms: int(&row.get_value(2)?, "changes", "created_at")?,
                status: ChangeStatus::from_code(int(&row.get_value(3)?, "changes", "status")?)
                    .ok_or(StoreError::Corrupt {
                        table: "changes",
                        what: "status",
                    })?,
                parts: Vec::new(),
            });
        }
        drop(rows);
        for change in &mut changes {
            let mut rows = conn
                .query(
                    "SELECT object_id, from_version, to_version, bytes FROM change_parts WHERE change_id = ? ORDER BY object_id",
                    (change.id.to_string(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                change.parts.push(ChangePart {
                    object_id: ObjectId::parse(&text(
                        &row.get_value(0)?,
                        "change_parts",
                        "object_id",
                    )?)?,
                    from_version: blob(&row.get_value(1)?, "change_parts", "from_version")?,
                    to_version: blob(&row.get_value(2)?, "change_parts", "to_version")?,
                    bytes: blob(&row.get_value(3)?, "change_parts", "bytes")?,
                });
            }
        }
        Ok(changes)
    }

    /// Lists Objects matching `filter`, newest first.
    pub async fn list(&self, filter: &ObjectFilter) -> Result<Vec<ObjectRecord>, StoreError> {
        let mut sql = String::from("SELECT o.object_id FROM objects o");
        let mut params: Vec<Value> = Vec::new();
        let mut conds: Vec<&str> = Vec::new();
        if let Some(t) = &filter.type_id {
            sql.push_str(" JOIN object_types t ON t.object_id = o.object_id AND t.type_id = ?");
            params.push(Value::Text(t.clone()));
        }
        if let Some(p) = filter.participant {
            sql.push_str(" JOIN participants p ON p.relation_id = o.object_id AND p.object_id = ?");
            params.push(Value::Text(p.to_string()));
        }
        if let Some(t) = filter.target {
            conds.push("o.target_id = ?");
            params.push(Value::Text(t.to_string()));
        }
        if let Some(k) = filter.kind {
            conds.push("o.kind = ?");
            params.push(Value::Integer(k.code()));
        }
        if !filter.include_deleted {
            conds.push("o.deleted_at IS NULL");
        }
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(" ORDER BY o.created_at DESC LIMIT ?");
        let limit = if filter.limit == 0 { 100 } else { filter.limit };
        params.push(Value::Integer(i64::try_from(limit).unwrap_or(i64::MAX)));

        let conn = self.conn.lock().await;
        let mut rows = conn.query(&sql, params).await?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await? {
            ids.push(ObjectId::parse(&text(
                &row.get_value(0)?,
                "objects",
                "object_id",
            )?)?);
        }
        drop(rows);
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(load_doc(&conn, id).await?.read()?);
        }
        Ok(out)
    }
}

async fn load_doc(conn: &Connection, id: ObjectId) -> Result<ObjectDoc, StoreError> {
    Ok(load_doc_with_count(conn, id).await?.0)
}

/// Loads the snapshot plus every update logged after it; also returns how
/// many updates were replayed, for compaction decisions.
async fn load_doc_with_count(
    conn: &Connection,
    id: ObjectId,
) -> Result<(ObjectDoc, i64), StoreError> {
    let mut rows = conn
        .query(
            "SELECT snapshot FROM docs WHERE object_id = ?",
            (id.to_string(),),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Err(StoreError::ObjectNotFound(id));
    };
    let snapshot = blob(&row.get_value(0)?, "docs", "snapshot")?;
    drop(rows);
    let mut rows = conn
        .query(
            "SELECT bytes FROM doc_updates WHERE object_id = ? ORDER BY seq",
            (id.to_string(),),
        )
        .await?;
    let mut updates = Vec::new();
    while let Some(row) = rows.next().await? {
        updates.push(blob(&row.get_value(0)?, "doc_updates", "bytes")?);
    }
    let count = i64::try_from(updates.len()).unwrap_or(i64::MAX);
    Ok((ObjectDoc::from_parts(id, &snapshot, &updates)?, count))
}

async fn write_change(
    tx: &turso::transaction::Transaction<'_>,
    change: &Change,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO changes (id, device_id, created_at, status) VALUES (?, ?, ?, ?)",
        (
            change.id.to_string(),
            change.device_id.clone(),
            change.created_at_ms,
            change.status.code(),
        ),
    )
    .await?;
    for part in &change.parts {
        tx.execute(
            "INSERT INTO change_parts (change_id, object_id, from_version, to_version, bytes) VALUES (?, ?, ?, ?, ?)",
            (
                change.id.to_string(),
                part.object_id.to_string(),
                part.from_version.clone(),
                part.to_version.clone(),
                part.bytes.clone(),
            ),
        )
        .await?;
    }
    Ok(())
}

async fn write_snapshot(
    tx: &turso::transaction::Transaction<'_>,
    doc: &ObjectDoc,
    rec: &ObjectRecord,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT OR REPLACE INTO docs (object_id, kind, snapshot, version, updated_at) VALUES (?, ?, ?, ?, ?)",
        (
            rec.id.to_string(),
            rec.kind.code(),
            doc.snapshot()?,
            doc.version(),
            rec.updated_at_ms,
        ),
    )
    .await?;
    Ok(())
}

async fn write_projections(
    tx: &turso::transaction::Transaction<'_>,
    rec: &ObjectRecord,
) -> Result<(), StoreError> {
    let id = rec.id.to_string();
    tx.execute(
        "INSERT OR REPLACE INTO objects (object_id, kind, created_at, updated_at, deleted_at, target_id) VALUES (?, ?, ?, ?, ?, ?)",
        (
            id.clone(),
            rec.kind.code(),
            rec.created_at_ms,
            rec.updated_at_ms,
            rec.deleted_at_ms.map_or(Value::Null, Value::Integer),
            rec.target.map_or(Value::Null, |t| Value::Text(t.to_string())),
        ),
    )
    .await?;
    tx.execute(
        "DELETE FROM participants WHERE relation_id = ?",
        (id.clone(),),
    )
    .await?;
    for p in &rec.participants {
        tx.execute(
            "INSERT INTO participants (relation_id, slot, object_id, role_id) VALUES (?, ?, ?, ?)",
            (
                id.clone(),
                p.slot.clone(),
                p.object.to_string(),
                p.role.clone(),
            ),
        )
        .await?;
    }
    tx.execute(
        "DELETE FROM object_types WHERE object_id = ?",
        (id.clone(),),
    )
    .await?;
    for t in &rec.types {
        tx.execute(
            "INSERT INTO object_types (object_id, type_id) VALUES (?, ?)",
            (id.clone(), t.clone()),
        )
        .await?;
    }
    tx.execute(
        "DELETE FROM field_values WHERE object_id = ?",
        (id.clone(),),
    )
    .await?;
    for (field, value) in &rec.fields {
        use crate::model::FieldValue as F;
        let mut cols = [
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ];
        match value {
            F::Bool(b) => cols[0] = Value::Integer(i64::from(*b)),
            F::Int(i) => cols[1] = Value::Integer(*i),
            F::Double(d) => cols[2] = Value::Real(*d),
            F::String(s) => cols[3] = Value::Text(s.clone()),
            F::TimeMs(t) => cols[4] = Value::Integer(*t),
            F::Ref(r) => cols[5] = Value::Text(r.to_string()),
        }
        let [v_bool, v_int, v_real, v_text, v_time, v_ref] = cols;
        tx.execute(
            "INSERT INTO field_values (object_id, field_id, v_bool, v_int, v_real, v_text, v_time, v_ref) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (id.clone(), field.clone(), v_bool, v_int, v_real, v_text, v_time, v_ref),
        )
        .await?;
    }
    Ok(())
}
