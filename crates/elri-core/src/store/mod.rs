//! Storage: one Turso database per device and one per Space, accessed
//! through Toasty models.
//!
//! The device database holds the registry of Spaces. A Space database holds
//! the source of truth (document snapshots plus their update log and the
//! Changes that produced them) together with projections that queries use;
//! both change in one transaction.

mod models;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use loro::VersionVector;
use toasty::stmt::Value;
use toasty::Db;
use toasty_driver_turso::Turso;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::change::{Change, ChangeId, ChangePart, ChangeStatus};
use crate::device::DeviceId;
use crate::model::{
    now_ms, FieldValue, ModelError, ObjectDoc, ObjectId, ObjectKind, ObjectRecord, SpaceId,
};
use models::{
    ChangePartRow, ChangeRow, DocRow, DocUpdateRow, FieldValueRow, ObjectRow, ObjectTypeRow,
    ParticipantRow, RemoteRow, SpaceLogRow, SpaceRow, SyncStateRow,
};
use tokio::sync::Notify;

use crate::change::{Admission, RejectReason};

/// Updates folded into a fresh snapshot once this many accumulate per document.
const COMPACT_AFTER_UPDATES: usize = 64;

/// Storage failures.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database layer failed.
    #[error(transparent)]
    Db(#[from] toasty::Error),
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

/// The server this device synchronizes with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConfig {
    /// WebSocket URL.
    pub url: String,
    /// Access token.
    pub token: String,
    /// User id on the server.
    pub user_id: String,
    /// User name on the server.
    pub user_name: String,
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

/// Opens a database file, creating its schema when the file is new.
async fn open_db(path: &Path, models: toasty::ModelSet) -> Result<Db, StoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| StoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let fresh = !path.exists();
    let db = Db::builder()
        .models(models)
        .build(Turso::file(path))
        .await?;
    if fresh {
        db.push_schema().await?;
    }
    Ok(db)
}

/// UUIDs are stored as 16-byte blobs; raw SQL binds them in that form.
fn uuid_param(id: Uuid) -> Value {
    Value::Bytes(id.as_bytes().to_vec())
}

fn uuid_of(v: &Value, table: &'static str, what: &'static str) -> Result<Uuid, StoreError> {
    match v {
        Value::Uuid(u) => Ok(*u),
        Value::Bytes(b) => Uuid::from_slice(b).map_err(|_| StoreError::Corrupt { table, what }),
        Value::String(s) => Uuid::parse_str(s).map_err(|_| StoreError::Corrupt { table, what }),
        _ => Err(StoreError::Corrupt { table, what }),
    }
}

/// Device-level storage: the Space registry and the open Space databases.
pub struct Store {
    root: PathBuf,
    device_id: DeviceId,
    device: Mutex<Db>,
    spaces: Mutex<HashMap<SpaceId, Arc<SpaceStore>>>,
}

impl Store {
    /// Opens `root/device.db` and prepares `root/spaces/` for Space files.
    pub async fn open(root: impl Into<PathBuf>, device_id: DeviceId) -> Result<Self, StoreError> {
        let root = root.into();
        let db = open_db(
            &root.join("device.db"),
            toasty::models!(SpaceRow, RemoteRow),
        )
        .await?;
        Ok(Self {
            root,
            device_id,
            device: Mutex::new(db),
            spaces: Mutex::new(HashMap::new()),
        })
    }

    /// Every Space registered on this device, newest first.
    pub async fn list_spaces(&self) -> Result<Vec<SpaceInfo>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = SpaceRow::all()
            .order_by(SpaceRow::fields().created_at().desc())
            .exec(&mut *db)
            .await?;
        rows.iter().map(space_info).collect()
    }

    /// Registers a new local-only Space and creates its database.
    pub async fn create_space(&self, name: &str) -> Result<SpaceInfo, StoreError> {
        self.register_space(SpaceId::new(), name, Authority::Local)
            .await
    }

    /// Registers a Space with a known id (a server Space being joined, or a
    /// Space the server itself hosts) and creates its database.
    pub async fn register_space(
        &self,
        id: SpaceId,
        name: &str,
        authority: Authority,
    ) -> Result<SpaceInfo, StoreError> {
        let info = SpaceInfo {
            id,
            name: name.to_owned(),
            authority,
            created_at_ms: now_ms(),
        };
        let space = SpaceStore::open(&self.space_path(info.id), self.device_id).await?;
        {
            let mut db = self.device.lock().await;
            toasty::create!(SpaceRow {
                id: info.id.uuid(),
                name: info.name.clone(),
                authority: info.authority.code(),
                created_at: info.created_at_ms,
            })
            .exec(&mut *db)
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
            let mut db = self.device.lock().await;
            SpaceRow::filter_by_id(id.uuid())
                .first()
                .exec(&mut *db)
                .await?
                .is_some()
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

    /// The configured server, if any.
    pub async fn remote(&self) -> Result<Option<RemoteConfig>, StoreError> {
        let mut db = self.device.lock().await;
        Ok(RemoteRow::filter_by_key("server".to_owned())
            .first()
            .exec(&mut *db)
            .await?
            .map(|r| RemoteConfig {
                url: r.url,
                token: r.token,
                user_id: r.user_id,
                user_name: r.user_name,
            }))
    }

    /// Stores the server configuration, replacing any previous one.
    pub async fn set_remote(&self, cfg: &RemoteConfig) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        RemoteRow::filter_by_key("server".to_owned())
            .delete()
            .exec(&mut *db)
            .await?;
        toasty::create!(RemoteRow {
            key: "server".to_owned(),
            url: cfg.url.clone(),
            token: cfg.token.clone(),
            user_id: cfg.user_id.clone(),
            user_name: cfg.user_name.clone(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Forgets the server configuration.
    pub async fn clear_remote(&self) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        RemoteRow::filter_by_key("server".to_owned())
            .delete()
            .exec(&mut *db)
            .await?;
        Ok(())
    }
}

fn space_info(row: &SpaceRow) -> Result<SpaceInfo, StoreError> {
    Ok(SpaceInfo {
        id: ObjectId::from_uuid(row.id),
        name: row.name.clone(),
        authority: Authority::from_code(row.authority).ok_or(StoreError::Corrupt {
            table: "spaces",
            what: "authority",
        })?,
        created_at_ms: row.created_at,
    })
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
    db: Mutex<Db>,
    /// Signalled after every local write, so a sync worker can push.
    dirty: Notify,
}

impl SpaceStore {
    async fn open(path: &Path, device_id: DeviceId) -> Result<Self, StoreError> {
        let db = open_db(
            path,
            toasty::models!(
                DocRow,
                DocUpdateRow,
                ChangeRow,
                ChangePartRow,
                ObjectRow,
                ObjectTypeRow,
                FieldValueRow,
                ParticipantRow,
                SpaceLogRow,
                SyncStateRow
            ),
        )
        .await?;
        Ok(Self {
            device_id,
            db: Mutex::new(db),
            dirty: Notify::new(),
        })
    }

    /// Resolves after the next local write (or immediately if one happened
    /// since the last call).
    pub async fn wait_dirty(&self) {
        self.dirty.notified().await;
    }

    fn new_change(&self, parts: Vec<ChangePart>) -> Change {
        Change {
            id: ChangeId::new(),
            device_id: self.device_id.as_string(),
            created_at_ms: now_ms(),
            status: ChangeStatus::Local,
            accepted_seq: None,
            rejection: None,
            parts,
        }
    }

    /// Stores a freshly created document as one Change with a full snapshot.
    pub async fn insert(&self, doc: &ObjectDoc) -> Result<ObjectRecord, StoreError> {
        let rec = doc.read()?;
        let empty = VersionVector::default();
        let part = ChangePart {
            object_id: rec.id,
            kind: rec.kind,
            from_version: empty.encode(),
            to_version: doc.version(),
            bytes: doc.updates_since(&empty)?,
        };
        let change = self.new_change(vec![part]);
        let mut db = self.db.lock().await;
        let mut tx = db.transaction().await?;
        write_change(&mut tx, &change).await?;
        write_snapshot(&mut tx, doc, &rec).await?;
        write_projections(&mut tx, &rec).await?;
        tx.commit().await?;
        self.dirty.notify_one();
        Ok(rec)
    }

    /// Loads a document by id.
    pub async fn load(&self, id: ObjectId) -> Result<ObjectDoc, StoreError> {
        let mut db = self.db.lock().await;
        Ok(load_doc(&mut db, id).await?.0)
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
        let mut db = self.db.lock().await;
        let (doc, pending) = load_doc(&mut db, id).await?;
        let before = doc.version_vector();
        edit(&doc)?;
        doc.touch(now_ms())?;
        let rec = doc.read()?;
        let part = ChangePart {
            object_id: id,
            kind: rec.kind,
            from_version: before.encode(),
            to_version: doc.version(),
            bytes: doc.updates_since(&before)?,
        };
        let change = self.new_change(vec![part]);

        let mut tx = db.transaction().await?;
        write_change(&mut tx, &change).await?;
        toasty::create!(DocUpdateRow {
            object_id: id.uuid(),
            change_id: change.id.uuid(),
            bytes: change.parts[0].bytes.clone(),
        })
        .exec(&mut tx)
        .await?;
        if pending + 1 >= COMPACT_AFTER_UPDATES {
            write_snapshot(&mut tx, &doc, &rec).await?;
            DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(id.uuid()))
                .delete()
                .exec(&mut tx)
                .await?;
        }
        write_projections(&mut tx, &rec).await?;
        tx.commit().await?;
        self.dirty.notify_one();
        Ok(rec)
    }

    // --- synchronization ------------------------------------------------

    /// Local Changes not yet submitted, oldest first.
    pub async fn outbox(&self, limit: usize) -> Result<Vec<Change>, StoreError> {
        let limit = if limit == 0 { 50 } else { limit };
        let mut db = self.db.lock().await;
        let rows = ChangeRow::filter(ChangeRow::fields().status().eq(ChangeStatus::Local.code()))
            .order_by(ChangeRow::fields().seq().asc())
            .limit(limit)
            .exec(&mut *db)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(read_change(&mut db, row).await?);
        }
        Ok(out)
    }

    /// How many Changes are in each of two states: not yet accepted
    /// (local, queued, waiting) and rejected.
    pub async fn pending_counts(&self) -> Result<(usize, usize), StoreError> {
        let mut db = self.db.lock().await;
        let pending = ChangeRow::filter(ChangeRow::fields().status().in_list([
            ChangeStatus::Local.code(),
            ChangeStatus::Queued.code(),
            ChangeStatus::Waiting.code(),
        ]))
        .exec(&mut *db)
        .await?
        .len();
        let rejected = ChangeRow::filter(
            ChangeRow::fields()
                .status()
                .eq(ChangeStatus::Rejected.code()),
        )
        .exec(&mut *db)
        .await?
        .len();
        Ok((pending, rejected))
    }

    /// Records the authority's answer for a submitted Change.
    pub async fn record_admission(
        &self,
        id: ChangeId,
        admission: &Admission,
    ) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        let (status, seq, rejection) = match admission {
            Admission::Accepted(seq) => (ChangeStatus::Accepted, Some(*seq), None),
            Admission::Rejected(reason) => (ChangeStatus::Rejected, None, Some(reason.to_string())),
        };
        toasty::update!(ChangeRow::filter(ChangeRow::fields().id().eq(id.uuid())) {
            status: status.code(),
            accepted_seq: seq,
            rejection,
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Highest Space-log sequence installed locally; 0 before any.
    pub async fn cursor(&self) -> Result<i64, StoreError> {
        let mut db = self.db.lock().await;
        read_cursor(&mut db).await
    }

    /// Highest sequence in the Space log; 0 when empty.
    pub async fn last_seq(&self) -> Result<i64, StoreError> {
        let mut db = self.db.lock().await;
        last_seq(&mut db).await
    }

    /// Accepted Changes after `after`, oldest first.
    pub async fn log_after(
        &self,
        after: i64,
        limit: usize,
    ) -> Result<Vec<(i64, Change)>, StoreError> {
        let limit = if limit == 0 { 100 } else { limit };
        let mut db = self.db.lock().await;
        let entries = SpaceLogRow::filter(SpaceLogRow::fields().seq().gt(after))
            .order_by(SpaceLogRow::fields().seq().asc())
            .limit(limit)
            .exec(&mut *db)
            .await?;
        let mut out = Vec::with_capacity(entries.len());
        for e in &entries {
            let Some(row) = ChangeRow::filter(ChangeRow::fields().id().eq(e.change_id))
                .first()
                .exec(&mut *db)
                .await?
            else {
                return Err(StoreError::Corrupt {
                    table: "space_log",
                    what: "change missing",
                });
            };
            out.push((e.seq, read_change(&mut db, &row).await?));
        }
        Ok(out)
    }

    /// Authority side: validates a submitted Change by applying its
    /// operations to candidate documents and, if everything applies, installs
    /// it at the next log position. Resubmitting a known id returns the
    /// recorded outcome.
    pub async fn admit(&self, change: &Change) -> Result<Admission, StoreError> {
        let mut db = self.db.lock().await;
        if let Some(existing) = ChangeRow::filter(ChangeRow::fields().id().eq(change.id.uuid()))
            .first()
            .exec(&mut *db)
            .await?
        {
            return Ok(match (existing.accepted_seq, existing.rejection) {
                (Some(seq), _) => Admission::Accepted(seq),
                (None, Some(r)) => Admission::Rejected(RejectReason::Invalid(r)),
                (None, None) => {
                    Admission::Rejected(RejectReason::Invalid("change known but undecided".into()))
                }
            });
        }
        if change.parts.is_empty() {
            return Ok(Admission::Rejected(RejectReason::Invalid(
                "no parts".into(),
            )));
        }

        // Stage every part on a candidate document before touching storage.
        let mut staged: Vec<(ObjectDoc, ObjectRecord, usize)> =
            Vec::with_capacity(change.parts.len());
        for part in &change.parts {
            let (doc, pending) = match load_doc(&mut db, part.object_id).await {
                Ok((doc, pending)) => {
                    match doc.import(&part.bytes) {
                        Ok(true) => {}
                        Ok(false) => {
                            return Ok(Admission::Rejected(RejectReason::MissingPredecessors))
                        }
                        Err(e) => {
                            return Ok(Admission::Rejected(RejectReason::Invalid(e.to_string())))
                        }
                    }
                    (doc, pending)
                }
                Err(StoreError::ObjectNotFound(_)) => {
                    match ObjectDoc::from_updates(part.object_id, std::slice::from_ref(&part.bytes))
                    {
                        Ok(doc) => (doc, 0),
                        Err(ModelError::Malformed(_)) => {
                            return Ok(Admission::Rejected(RejectReason::MissingPredecessors));
                        }
                        Err(e) => {
                            return Ok(Admission::Rejected(RejectReason::Invalid(e.to_string())));
                        }
                    }
                }
                Err(e) => return Err(e),
            };
            let rec = match doc.read() {
                Ok(r) => r,
                Err(e) => return Ok(Admission::Rejected(RejectReason::Invalid(e.to_string()))),
            };
            if rec.kind != part.kind {
                return Ok(Admission::Rejected(RejectReason::Invalid(
                    "kind mismatch".into(),
                )));
            }
            staged.push((doc, rec, pending));
        }

        let seq = last_seq(&mut db).await? + 1;
        let accepted = Change {
            status: ChangeStatus::Accepted,
            accepted_seq: Some(seq),
            rejection: None,
            ..change.clone()
        };
        let mut tx = db.transaction().await?;
        write_change(&mut tx, &accepted).await?;
        toasty::create!(SpaceLogRow {
            seq,
            change_id: change.id.uuid(),
            accepted_at: now_ms(),
        })
        .exec(&mut tx)
        .await?;
        for ((doc, rec, pending), part) in staged.iter().zip(&change.parts) {
            install_part(&mut tx, doc, rec, part, &change.id, *pending).await?;
        }
        tx.commit().await?;
        Ok(Admission::Accepted(seq))
    }

    /// Replica side: installs an accepted Change delivered from the
    /// authority at log position `seq`. A Change this device produced is
    /// only marked accepted; a foreign one is applied to the documents.
    pub async fn apply_accepted(&self, seq: i64, change: &Change) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        let mine = ChangeRow::filter(ChangeRow::fields().id().eq(change.id.uuid()))
            .first()
            .exec(&mut *db)
            .await?;
        let mut tx = db.transaction().await?;
        if let Some(row) = mine {
            if row.accepted_seq != Some(seq) {
                toasty::update!(ChangeRow::filter(ChangeRow::fields().id().eq(change.id.uuid())) {
                    status: ChangeStatus::Accepted.code(),
                    accepted_seq: Some(seq),
                })
                .exec(&mut tx)
                .await?;
            }
        } else {
            let accepted = Change {
                status: ChangeStatus::Accepted,
                accepted_seq: Some(seq),
                rejection: None,
                ..change.clone()
            };
            write_change(&mut tx, &accepted).await?;
            for part in &change.parts {
                let (doc, pending) = match load_doc_tx(&mut tx, part.object_id).await {
                    Ok((doc, pending)) => {
                        if !doc.import(&part.bytes)? {
                            return Err(StoreError::Model(ModelError::Malformed(
                                "accepted change with missing predecessors",
                            )));
                        }
                        (doc, pending)
                    }
                    Err(StoreError::ObjectNotFound(_)) => (
                        ObjectDoc::from_updates(part.object_id, std::slice::from_ref(&part.bytes))?,
                        0,
                    ),
                    Err(e) => return Err(e),
                };
                let rec = doc.read()?;
                install_part(&mut tx, &doc, &rec, part, &change.id, pending).await?;
            }
        }
        SpaceLogRow::filter_by_seq(seq)
            .delete()
            .exec(&mut tx)
            .await?;
        toasty::create!(SpaceLogRow {
            seq,
            change_id: change.id.uuid(),
            accepted_at: now_ms(),
        })
        .exec(&mut tx)
        .await?;
        SyncStateRow::filter_by_key("cursor".to_owned())
            .delete()
            .exec(&mut tx)
            .await?;
        toasty::create!(SyncStateRow {
            key: "cursor".to_owned(),
            value: seq,
        })
        .exec(&mut tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Lists Objects matching `filter`, newest first.
    pub async fn list(&self, filter: &ObjectFilter) -> Result<Vec<ObjectRecord>, StoreError> {
        use std::fmt::Write as _;
        let mut sql = String::from("SELECT o.object_id FROM objects o");
        let mut params: Vec<Value> = Vec::new();
        let mut conds: Vec<String> = Vec::new();
        let mut n = 0usize;
        let mut next = |params: &mut Vec<Value>, v: Value| {
            params.push(v);
            n += 1;
            format!("?{n}")
        };
        if let Some(t) = &filter.type_id {
            let p = next(&mut params, Value::String(t.clone()));
            let _ = write!(
                sql,
                " JOIN object_types t ON t.object_id = o.object_id AND t.type_id = {p}"
            );
        }
        if let Some(pid) = filter.participant {
            let p = next(&mut params, uuid_param(pid.uuid()));
            let _ = write!(
                sql,
                " JOIN participants p ON p.relation_id = o.object_id AND p.object_id = {p}"
            );
        }
        if let Some(t) = filter.target {
            let p = next(&mut params, uuid_param(t.uuid()));
            conds.push(format!("o.target_id = {p}"));
        }
        if let Some(k) = filter.kind {
            let p = next(&mut params, Value::I64(k.code()));
            conds.push(format!("o.kind = {p}"));
        }
        if !filter.include_deleted {
            conds.push("o.deleted_at IS NULL".to_owned());
        }
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        let limit = if filter.limit == 0 { 100 } else { filter.limit };
        let p = next(
            &mut params,
            Value::I64(i64::try_from(limit).unwrap_or(i64::MAX)),
        );
        let _ = write!(sql, " ORDER BY o.created_at DESC LIMIT {p}");

        let mut db = self.db.lock().await;
        let mut q = toasty::sql::query(sql);
        for v in params {
            q = q.bind(v);
        }
        let rows = q.exec(&mut *db).await?;
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let Value::Record(rec) = row else {
                return Err(StoreError::Corrupt {
                    table: "objects",
                    what: "row shape",
                });
            };
            ids.push(ObjectId::from_uuid(uuid_of(
                &rec[0],
                "objects",
                "object_id",
            )?));
        }
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(load_doc(&mut db, id).await?.0.read()?);
        }
        Ok(out)
    }

    /// Changes recorded in this Space, newest first.
    pub async fn list_changes(&self, limit: usize) -> Result<Vec<Change>, StoreError> {
        let limit = if limit == 0 { 50 } else { limit };
        let mut db = self.db.lock().await;
        let rows = ChangeRow::all()
            .order_by(ChangeRow::fields().seq().desc())
            .limit(limit)
            .exec(&mut *db)
            .await?;
        let mut changes = Vec::with_capacity(rows.len());
        for row in &rows {
            changes.push(read_change(&mut db, row).await?);
        }
        Ok(changes)
    }
}

async fn read_change(db: &mut Db, row: &ChangeRow) -> Result<Change, StoreError> {
    let parts = ChangePartRow::filter(ChangePartRow::fields().change_id().eq(row.id))
        .exec(&mut *db)
        .await?;
    let mut parts: Vec<ChangePart> = parts
        .into_iter()
        .map(|p| {
            Ok(ChangePart {
                object_id: ObjectId::from_uuid(p.object_id),
                kind: ObjectKind::from_code(p.kind).ok_or(StoreError::Corrupt {
                    table: "change_parts",
                    what: "kind",
                })?,
                from_version: p.base_version,
                to_version: p.next_version,
                bytes: p.bytes,
            })
        })
        .collect::<Result<_, StoreError>>()?;
    parts.sort_by_key(|p| p.object_id);
    Ok(Change {
        id: ObjectId::from_uuid(row.id),
        device_id: row.device_id.clone(),
        created_at_ms: row.created_at,
        status: ChangeStatus::from_code(row.status).ok_or(StoreError::Corrupt {
            table: "changes",
            what: "status",
        })?,
        accepted_seq: row.accepted_seq,
        rejection: row.rejection.clone(),
        parts,
    })
}

/// Loads the snapshot plus every update logged after it; also returns how
/// many updates were replayed, for compaction decisions.
async fn load_doc(db: &mut Db, id: ObjectId) -> Result<(ObjectDoc, usize), StoreError> {
    let Some(doc) = DocRow::filter_by_object_id(id.uuid())
        .first()
        .exec(&mut *db)
        .await?
    else {
        return Err(StoreError::ObjectNotFound(id));
    };
    let updates = DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(id.uuid()))
        .order_by(DocUpdateRow::fields().seq().asc())
        .exec(&mut *db)
        .await?;
    let bytes: Vec<Vec<u8>> = updates.into_iter().map(|u| u.bytes).collect();
    let count = bytes.len();
    Ok((ObjectDoc::from_parts(id, &doc.snapshot, &bytes)?, count))
}

async fn write_change(tx: &mut toasty::Transaction<'_>, change: &Change) -> Result<(), StoreError> {
    toasty::create!(ChangeRow {
        id: change.id.uuid(),
        device_id: change.device_id.clone(),
        created_at: change.created_at_ms,
        status: change.status.code(),
        accepted_seq: change.accepted_seq,
        rejection: change.rejection.clone(),
    })
    .exec(&mut *tx)
    .await?;
    for part in &change.parts {
        toasty::create!(ChangePartRow {
            change_id: change.id.uuid(),
            object_id: part.object_id.uuid(),
            kind: part.kind.code(),
            base_version: part.from_version.clone(),
            next_version: part.to_version.clone(),
            bytes: part.bytes.clone(),
        })
        .exec(&mut *tx)
        .await?;
    }
    Ok(())
}

async fn write_snapshot(
    tx: &mut toasty::Transaction<'_>,
    doc: &ObjectDoc,
    rec: &ObjectRecord,
) -> Result<(), StoreError> {
    DocRow::filter_by_object_id(rec.id.uuid())
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(DocRow {
        object_id: rec.id.uuid(),
        kind: rec.kind.code(),
        snapshot: doc.snapshot()?,
        version: doc.version(),
        updated_at: rec.updated_at_ms,
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
}

async fn write_projections(
    tx: &mut toasty::Transaction<'_>,
    rec: &ObjectRecord,
) -> Result<(), StoreError> {
    let id = rec.id.uuid();
    ObjectRow::filter_by_object_id(id)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(ObjectRow {
        object_id: id,
        kind: rec.kind.code(),
        created_at: rec.created_at_ms,
        updated_at: rec.updated_at_ms,
        deleted_at: rec.deleted_at_ms,
        target_id: rec.target.map(|t| t.uuid()),
    })
    .exec(&mut *tx)
    .await?;

    ObjectTypeRow::filter(ObjectTypeRow::fields().object_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    for t in &rec.types {
        toasty::create!(ObjectTypeRow {
            object_id: id,
            type_id: t.clone(),
        })
        .exec(&mut *tx)
        .await?;
    }

    FieldValueRow::filter(FieldValueRow::fields().object_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    for (field, value) in &rec.fields {
        let mut row = FieldValueRow {
            object_id: id,
            field_id: field.clone(),
            v_bool: None,
            v_int: None,
            v_real: None,
            v_text: None,
            v_time: None,
            v_ref: None,
        };
        match value {
            FieldValue::Bool(b) => row.v_bool = Some(*b),
            FieldValue::Int(i) => row.v_int = Some(*i),
            FieldValue::Double(d) => row.v_real = Some(*d),
            FieldValue::String(s) => row.v_text = Some(s.clone()),
            FieldValue::TimeMs(t) => row.v_time = Some(*t),
            FieldValue::Ref(r) => row.v_ref = Some(r.uuid()),
        }
        toasty::create!(FieldValueRow {
            object_id: row.object_id,
            field_id: row.field_id,
            v_bool: row.v_bool,
            v_int: row.v_int,
            v_real: row.v_real,
            v_text: row.v_text,
            v_time: row.v_time,
            v_ref: row.v_ref,
        })
        .exec(&mut *tx)
        .await?;
    }

    ParticipantRow::filter(ParticipantRow::fields().relation_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    for p in &rec.participants {
        toasty::create!(ParticipantRow {
            relation_id: id,
            slot: p.slot.clone(),
            object_id: p.object.uuid(),
            role_id: p.role.clone(),
        })
        .exec(&mut *tx)
        .await?;
    }
    Ok(())
}

async fn read_cursor(db: &mut Db) -> Result<i64, StoreError> {
    Ok(SyncStateRow::filter_by_key("cursor".to_owned())
        .first()
        .exec(&mut *db)
        .await?
        .map_or(0, |r| r.value))
}

async fn last_seq(db: &mut Db) -> Result<i64, StoreError> {
    Ok(SpaceLogRow::all()
        .order_by(SpaceLogRow::fields().seq().desc())
        .first()
        .exec(&mut *db)
        .await?
        .map_or(0, |r| r.seq))
}

async fn load_doc_tx(
    tx: &mut toasty::Transaction<'_>,
    id: ObjectId,
) -> Result<(ObjectDoc, usize), StoreError> {
    let Some(doc) = DocRow::filter_by_object_id(id.uuid())
        .first()
        .exec(&mut *tx)
        .await?
    else {
        return Err(StoreError::ObjectNotFound(id));
    };
    let updates = DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(id.uuid()))
        .order_by(DocUpdateRow::fields().seq().asc())
        .exec(&mut *tx)
        .await?;
    let bytes: Vec<Vec<u8>> = updates.into_iter().map(|u| u.bytes).collect();
    let count = bytes.len();
    Ok((ObjectDoc::from_parts(id, &doc.snapshot, &bytes)?, count))
}

/// Writes an applied part: either the first snapshot of a new document or
/// one more entry in its update log (compacting when due), plus the
/// projections.
async fn install_part(
    tx: &mut toasty::Transaction<'_>,
    doc: &ObjectDoc,
    rec: &ObjectRecord,
    part: &ChangePart,
    change_id: &ChangeId,
    pending: usize,
) -> Result<(), StoreError> {
    let is_new = DocRow::filter_by_object_id(part.object_id.uuid())
        .first()
        .exec(&mut *tx)
        .await?
        .is_none();
    if is_new {
        write_snapshot(tx, doc, rec).await?;
    } else {
        toasty::create!(DocUpdateRow {
            object_id: part.object_id.uuid(),
            change_id: change_id.uuid(),
            bytes: part.bytes.clone(),
        })
        .exec(&mut *tx)
        .await?;
        if pending + 1 >= COMPACT_AFTER_UPDATES {
            write_snapshot(tx, doc, rec).await?;
            DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(part.object_id.uuid()))
                .delete()
                .exec(&mut *tx)
                .await?;
        }
    }
    write_projections(tx, rec).await
}
