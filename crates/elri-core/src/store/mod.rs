//! Storage: one Turso database per device and one per Space, accessed
//! through Toasty models.
//!
//! The device database holds the registry of Spaces. A Space database holds
//! the source of truth (document snapshots plus their update log and the
//! Changes that produced them) together with projections that queries use;
//! both change in one transaction. Records come and go as protocol
//! messages.

mod models;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use elri_proto::elri::v1::change::{Change, ChangePart, ChangeStatus};
use elri_proto::elri::v1::definition::FieldKind;
use elri_proto::elri::v1::events::{
    event, ChangeEvent, Event, ObjectEvent, RemoteEvent, SpaceEvent,
};
use elri_proto::elri::v1::object::{value, Object, ObjectKind, Value as PbValue};
use elri_proto::elri::v1::space::{Authority, Role, Space};
use elri_proto::elri::v1::sync::{Entry, Envelope, Part};
use loro::VersionVector;
use toasty::stmt::Value;
use toasty::Db;
use toasty_driver_turso::Turso;
use tokio::sync::{broadcast, Mutex, Notify};
use uuid::Uuid;

use crate::change::{accepted, rejected, Outcome, Reason};
use crate::device::DeviceId;
use crate::model::{field_def_kind, now_ms, ModelError, ObjectDoc, ObjectId, SpaceId};
use models::{
    ChangeDepRow, ChangePartRow, ChangeRow, DocRow, DocUpdateRow, FieldValueRow, ObjectRow,
    ObjectTypeRow, ParticipantRow, PlacementRow, RemoteRow, SpaceLogRow, SpaceRow, SyncStateRow,
    TypeFieldRow,
};

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
    /// Retry or discard of a Change that is not rejected (or paused).
    #[error("change {0} is not rejected")]
    NotRejected(String),
    /// No such Change in the Space.
    #[error("change {0} not found")]
    ChangeNotFound(String),
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
    restrict_to_owner(path);
    Ok(db)
}

/// Database files hold user data and, in the device database, the server
/// token; keep them readable by the owner only. The write-ahead log is
/// created lazily, so it is tightened when present.
fn restrict_to_owner(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for p in [path.to_path_buf(), path.with_extension("db-wal")] {
            if p.exists() {
                let _ = std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
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

fn uuid_arg(s: &str) -> Result<Uuid, StoreError> {
    Ok(ObjectId::parse(s)?.uuid())
}

fn status_code(status: ChangeStatus) -> i64 {
    i64::from(status as i32)
}

fn status_of(code: i64) -> Result<ChangeStatus, StoreError> {
    i32::try_from(code)
        .ok()
        .and_then(|c| ChangeStatus::try_from(c).ok())
        .filter(|s| *s != ChangeStatus::Unspecified)
        .ok_or(StoreError::Corrupt {
            table: "changes",
            what: "status",
        })
}

/// Device-level storage: the Space registry and the open Space databases.
pub struct Store {
    root: PathBuf,
    device_id: DeviceId,
    device: Mutex<Db>,
    spaces: Mutex<HashMap<SpaceId, Arc<SpaceStore>>>,
    events: broadcast::Sender<Event>,
}

/// Events a watcher can miss before it is told it lagged.
const EVENT_BUFFER: usize = 1024;

fn emit(events: &broadcast::Sender<Event>, body: event::Event) {
    // No receivers is not an error: nobody is watching.
    let _ = events.send(Event {
        at_ms: now_ms(),
        event: Some(body),
    });
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
            events: broadcast::channel(EVENT_BUFFER).0,
        })
    }

    /// Subscribes to everything that happens in this store from now on.
    pub fn watch(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Announces a change of the server link.
    pub fn emit_remote(&self) {
        emit(&self.events, event::Event::Remote(RemoteEvent {}));
    }

    /// Every Space registered on this device, newest first. The role is the
    /// device's own view: Owner of local Spaces, Owner placeholder for
    /// server Spaces until the server says otherwise.
    pub async fn list_spaces(&self) -> Result<Vec<Space>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = SpaceRow::all()
            .order_by(SpaceRow::fields().created_at().desc())
            .exec(&mut *db)
            .await?;
        rows.iter().map(space_of).collect()
    }

    /// Registers a new local-only Space and creates its database.
    pub async fn create_space(&self, name: &str) -> Result<Space, StoreError> {
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
    ) -> Result<Space, StoreError> {
        let space =
            SpaceStore::open(&self.space_path(id), self.device_id, self.events.clone()).await?;
        {
            let mut db = self.device.lock().await;
            toasty::create!(SpaceRow {
                id: id.uuid(),
                name: name.to_owned(),
                authority: i64::from(authority as i32),
                created_at: now_ms(),
            })
            .exec(&mut *db)
            .await?;
        }
        self.spaces.lock().await.insert(id, Arc::new(space));
        emit(
            &self.events,
            event::Event::Space(SpaceEvent {
                space_id: id.to_string(),
            }),
        );
        Ok(Space {
            id: id.to_string(),
            name: name.to_owned(),
            role: Role::Owner.into(),
            authority: authority.into(),
        })
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
        let space = Arc::new(
            SpaceStore::open(&self.space_path(id), self.device_id, self.events.clone()).await?,
        );
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

fn space_of(row: &SpaceRow) -> Result<Space, StoreError> {
    let authority = i32::try_from(row.authority)
        .ok()
        .and_then(|c| Authority::try_from(c).ok())
        .filter(|a| *a != Authority::Unspecified)
        .ok_or(StoreError::Corrupt {
            table: "spaces",
            what: "authority",
        })?;
    Ok(Space {
        id: ObjectId::from_uuid(row.id).to_string(),
        name: row.name.clone(),
        role: Role::Owner.into(),
        authority: authority.into(),
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
    /// Only Entities that place this block, if set.
    pub placed_block: Option<ObjectId>,
}

/// Storage of one Space: documents, their update log, Changes, and
/// projections, with one writer.
pub struct SpaceStore {
    id: SpaceId,
    device_id: DeviceId,
    db: Mutex<Db>,
    /// Signalled after every local write, so a sync worker can push.
    dirty: Notify,
    events: broadcast::Sender<Event>,
}

impl SpaceStore {
    async fn open(
        path: &Path,
        device_id: DeviceId,
        events: broadcast::Sender<Event>,
    ) -> Result<Self, StoreError> {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or(StoreError::Corrupt {
                table: "spaces",
                what: "file name",
            })?;
        let id = ObjectId::parse(stem)?;
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
                ChangeDepRow,
                ParticipantRow,
                TypeFieldRow,
                PlacementRow,
                SpaceLogRow,
                SyncStateRow
            ),
        )
        .await?;
        Ok(Self {
            id,
            device_id,
            db: Mutex::new(db),
            dirty: Notify::new(),
            events,
        })
    }

    fn emit_object(&self, rec: &Object) {
        emit(
            &self.events,
            event::Event::Object(ObjectEvent {
                space_id: self.id.to_string(),
                object_id: rec.id.clone(),
                kind: rec.kind,
                removed: false,
            }),
        );
    }

    fn emit_objects(&self, touched: &[(Uuid, i64, bool)]) {
        for (object, kind, live) in touched {
            emit(
                &self.events,
                event::Event::Object(ObjectEvent {
                    space_id: self.id.to_string(),
                    object_id: object.to_string(),
                    kind: i32::try_from(*kind).unwrap_or_default(),
                    removed: !live,
                }),
            );
        }
    }

    fn emit_change(&self, change_id: Uuid, status: ChangeStatus) {
        emit(
            &self.events,
            event::Event::Change(ChangeEvent {
                space_id: self.id.to_string(),
                change_id: change_id.to_string(),
                status: status.into(),
            }),
        );
    }

    /// Resolves after the next local write (or immediately if one happened
    /// since the last call).
    pub async fn wait_dirty(&self) {
        self.dirty.notified().await;
    }

    fn new_envelope(&self, parts: Vec<Part>) -> Envelope {
        Envelope {
            id: ObjectId::new().to_string(),
            space_id: self.id.to_string(),
            device_id: self.device_id.as_string(),
            created_at_ms: now_ms(),
            parts,
        }
    }

    /// Stores a freshly created document as one Change with a full snapshot.
    pub async fn insert(&self, doc: &ObjectDoc) -> Result<Object, StoreError> {
        let rec = doc.read()?;
        let empty = VersionVector::default();
        let part = Part {
            object_id: rec.id.clone(),
            kind: rec.kind,
            from_version: empty.encode().into(),
            to_version: doc.version().into(),
            bytes: doc.updates_since(&empty)?.into(),
        };
        let env = self.new_envelope(vec![part]);
        let mut db = self.db.lock().await;
        let deps = dependencies(&mut db, &rec).await?;
        let mut tx = db.transaction().await?;
        write_change(&mut tx, &env, ChangeStatus::Local, None, "").await?;
        write_deps(&mut tx, uuid_arg(&env.id)?, &deps).await?;
        write_snapshot(&mut tx, doc, &rec).await?;
        write_projections(&mut tx, &rec).await?;
        tx.commit().await?;
        self.dirty.notify_one();
        self.emit_change(uuid_arg(&env.id)?, ChangeStatus::Local);
        self.emit_object(&rec);
        Ok(rec)
    }

    /// Loads a document by id.
    pub async fn load(&self, id: ObjectId) -> Result<ObjectDoc, StoreError> {
        let mut db = self.db.lock().await;
        Ok(load_doc(&mut db, id).await?.0)
    }

    /// Materializes an Object by id.
    pub async fn get(&self, id: ObjectId) -> Result<Object, StoreError> {
        Ok(self.load(id).await?.read()?)
    }

    /// Loads a document, applies `edit`, and records the result as one
    /// Change: the new operations go to the update log and the outbox, the
    /// projections are rewritten, all in one transaction. The snapshot is
    /// refreshed once enough updates have accumulated.
    pub async fn update<F>(&self, id: ObjectId, edit: F) -> Result<Object, StoreError>
    where
        F: FnOnce(&ObjectDoc) -> Result<(), ModelError>,
    {
        let mut db = self.db.lock().await;
        let (doc, pending) = load_doc(&mut db, id).await?;
        let before = doc.version_vector();
        edit(&doc)?;
        doc.touch(now_ms())?;
        let rec = doc.read()?;
        let bytes = doc.updates_since(&before)?;
        let part = Part {
            object_id: id.to_string(),
            kind: rec.kind,
            from_version: before.encode().into(),
            to_version: doc.version().into(),
            bytes: bytes.clone().into(),
        };
        let env = self.new_envelope(vec![part]);
        let deps = dependencies(&mut db, &rec).await?;

        let mut tx = db.transaction().await?;
        write_change(&mut tx, &env, ChangeStatus::Local, None, "").await?;
        write_deps(&mut tx, uuid_arg(&env.id)?, &deps).await?;
        toasty::create!(DocUpdateRow {
            object_id: id.uuid(),
            change_id: uuid_arg(&env.id)?,
            bytes,
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
        self.emit_change(uuid_arg(&env.id)?, ChangeStatus::Local);
        self.emit_object(&rec);
        Ok(rec)
    }

    /// The kind a field definition fixes, if `field_id` names a known,
    /// live field definition of this Space.
    pub async fn field_kind(&self, field_id: &str) -> Result<Option<FieldKind>, StoreError> {
        let Ok(id) = ObjectId::parse(field_id) else {
            return Ok(None);
        };
        let mut db = self.db.lock().await;
        let Some(row) = ObjectRow::filter_by_object_id(id.uuid())
            .first()
            .exec(&mut *db)
            .await?
        else {
            return Ok(None);
        };
        if row.kind != i64::from(ObjectKind::FieldDef as i32) || row.deleted_at.is_some() {
            return Ok(None);
        }
        Ok(field_def_kind(&load_doc(&mut db, id).await?.0.read()?))
    }

    /// Whether a live Object with this id is in the Space.
    pub async fn exists(&self, id: ObjectId) -> Result<bool, StoreError> {
        let mut db = self.db.lock().await;
        Ok(ObjectRow::filter_by_object_id(id.uuid())
            .first()
            .exec(&mut *db)
            .await?
            .is_some_and(|row| row.deleted_at.is_none()))
    }

    /// Lists Objects matching `filter`, newest first.
    pub async fn list(&self, filter: &ObjectFilter) -> Result<Vec<Object>, StoreError> {
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
        if let Some(b) = filter.placed_block {
            let p = next(&mut params, uuid_param(b.uuid()));
            let _ = write!(
                sql,
                " JOIN (SELECT DISTINCT document_id FROM placements WHERE block_id = {p}) pl ON pl.document_id = o.object_id"
            );
        }
        if let Some(t) = filter.target {
            let p = next(&mut params, uuid_param(t.uuid()));
            conds.push(format!("o.target_id = {p}"));
        }
        if let Some(k) = filter.kind {
            let p = next(&mut params, Value::I64(i64::from(k as i32)));
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

    /// Changes recorded in this Space, newest first, without payloads.
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
            changes.push(read_change(&mut db, self.id, row).await?);
        }
        Ok(changes)
    }

    /// One Change by id, without payload.
    pub async fn change(&self, id: &str) -> Result<Change, StoreError> {
        let change_id = uuid_arg(id)?;
        let mut db = self.db.lock().await;
        let row = change_row(&mut db, change_id).await?;
        read_change(&mut db, self.id, &row).await
    }

    /// Resubmits a rejected Change unchanged: it goes back to the outbox,
    /// its paused dependents follow it, and their operations return to the
    /// live documents.
    pub async fn retry_change(&self, id: &str) -> Result<Change, StoreError> {
        let change_id = uuid_arg(id)?;
        let mut db = self.db.lock().await;
        let row = change_row(&mut db, change_id).await?;
        if status_of(row.status)? != ChangeStatus::Rejected {
            return Err(StoreError::NotRejected(id.to_owned()));
        }
        let mut revived = dependents(&mut db, change_id, &[ChangeStatus::Paused]).await?;
        revived.insert(0, change_id);
        let touched = objects_of(&mut db, &revived).await?;
        let mut tx = db.transaction().await?;
        for id in &revived {
            toasty::update!(ChangeRow::filter(ChangeRow::fields().id().eq(*id)) {
                status: status_code(ChangeStatus::Local),
                rejection: None,
            })
            .exec(&mut tx)
            .await?;
        }
        let mut rebuilt = Vec::with_capacity(touched.len());
        for (object, kind) in touched {
            let live = rebuild_doc(&mut tx, object).await?;
            rebuilt.push((object, kind, live));
        }
        tx.commit().await?;
        self.dirty.notify_one();
        for id in &revived {
            self.emit_change(*id, ChangeStatus::Local);
        }
        self.emit_objects(&rebuilt);
        let row = change_row(&mut db, change_id).await?;
        read_change(&mut db, self.id, &row).await
    }

    /// Drops a rejected or paused Change and its paused dependents for
    /// good. Returns the ids dropped.
    pub async fn discard_change(&self, id: &str) -> Result<Vec<String>, StoreError> {
        let change_id = uuid_arg(id)?;
        let mut db = self.db.lock().await;
        let row = change_row(&mut db, change_id).await?;
        if !matches!(
            status_of(row.status)?,
            ChangeStatus::Rejected | ChangeStatus::Paused
        ) {
            return Err(StoreError::NotRejected(id.to_owned()));
        }
        let mut dropped = dependents(&mut db, change_id, &[ChangeStatus::Paused]).await?;
        dropped.insert(0, change_id);
        let mut tx = db.transaction().await?;
        for id in &dropped {
            ChangeRow::filter(ChangeRow::fields().id().eq(*id))
                .delete()
                .exec(&mut tx)
                .await?;
            ChangePartRow::filter(ChangePartRow::fields().change_id().eq(*id))
                .delete()
                .exec(&mut tx)
                .await?;
            ChangeDepRow::filter(ChangeDepRow::fields().change_id().eq(*id))
                .delete()
                .exec(&mut tx)
                .await?;
            ChangeDepRow::filter(ChangeDepRow::fields().depends_on().eq(*id))
                .delete()
                .exec(&mut tx)
                .await?;
        }
        tx.commit().await?;
        for id in &dropped {
            self.emit_change(*id, ChangeStatus::Unspecified);
        }
        Ok(dropped.iter().map(ToString::to_string).collect())
    }

    // --- synchronization ------------------------------------------------

    /// Local Changes not yet submitted, oldest first, with payloads.
    pub async fn outbox(&self, limit: usize) -> Result<Vec<Envelope>, StoreError> {
        let limit = if limit == 0 { 50 } else { limit };
        let mut db = self.db.lock().await;
        let rows = ChangeRow::filter(
            ChangeRow::fields()
                .status()
                .eq(status_code(ChangeStatus::Local)),
        )
        .order_by(ChangeRow::fields().seq().asc())
        .limit(limit)
        .exec(&mut *db)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(read_envelope(&mut db, self.id, row).await?);
        }
        Ok(out)
    }

    /// How many Changes are in each of two states: not yet accepted
    /// (local, queued, waiting) and needing the user's decision (rejected,
    /// paused).
    pub async fn pending_counts(&self) -> Result<(usize, usize), StoreError> {
        let mut db = self.db.lock().await;
        let pending = ChangeRow::filter(ChangeRow::fields().status().in_list([
            status_code(ChangeStatus::Local),
            status_code(ChangeStatus::Queued),
            status_code(ChangeStatus::Waiting),
        ]))
        .exec(&mut *db)
        .await?
        .len();
        let rejected = ChangeRow::filter(ChangeRow::fields().status().in_list([
            status_code(ChangeStatus::Rejected),
            status_code(ChangeStatus::Paused),
        ]))
        .exec(&mut *db)
        .await?
        .len();
        Ok((pending, rejected))
    }

    /// Records the authority's answer for a submitted Change.
    pub async fn record_outcome(&self, id: &str, outcome: &Outcome) -> Result<(), StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.db.lock().await;
        let (status, seq, rejection) = match outcome {
            Outcome::AcceptedSeq(seq) => (ChangeStatus::Accepted, Some(*seq), None),
            Outcome::Rejected(r) => (
                ChangeStatus::Rejected,
                None,
                Some(format!("{}: {}", r.reason().as_str_name(), r.message)),
            ),
        };
        let paused = if status == ChangeStatus::Rejected {
            dependents(&mut db, id, &[ChangeStatus::Local, ChangeStatus::Queued]).await?
        } else {
            Vec::new()
        };
        let mut excluded = paused.clone();
        excluded.push(id);
        let touched = if status == ChangeStatus::Rejected {
            objects_of(&mut db, &excluded).await?
        } else {
            Vec::new()
        };
        let mut tx = db.transaction().await?;
        toasty::update!(ChangeRow::filter(ChangeRow::fields().id().eq(id)) {
            status: status_code(status),
            accepted_seq: seq,
            rejection,
        })
        .exec(&mut tx)
        .await?;
        for dep in &paused {
            toasty::update!(ChangeRow::filter(ChangeRow::fields().id().eq(*dep)) {
                status: status_code(ChangeStatus::Paused),
                rejection: Some(format!("depends on rejected change {id}")),
            })
            .exec(&mut tx)
            .await?;
        }
        let mut rebuilt = Vec::with_capacity(touched.len());
        for (object, kind) in touched {
            let live = rebuild_doc(&mut tx, object).await?;
            rebuilt.push((object, kind, live));
        }
        tx.commit().await?;
        self.emit_change(id, status);
        for dep in &paused {
            self.emit_change(*dep, ChangeStatus::Paused);
        }
        self.emit_objects(&rebuilt);
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
    pub async fn log_after(&self, after: i64, limit: usize) -> Result<Vec<Entry>, StoreError> {
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
            out.push(Entry {
                seq: e.seq,
                change: Some(read_envelope(&mut db, self.id, &row).await?),
                accepted_at_ms: e.accepted_at,
            });
        }
        Ok(out)
    }

    /// Authority side: validates a submitted Change by applying its
    /// operations to candidate documents and, if everything applies, installs
    /// it at the next log position. Resubmitting a known id returns the
    /// recorded outcome.
    pub async fn admit(&self, env: &Envelope) -> Result<Outcome, StoreError> {
        let Ok(change_id) = uuid_arg(&env.id) else {
            return Ok(rejected(Reason::Invalid, "change id"));
        };
        let mut db = self.db.lock().await;
        if let Some(existing) = ChangeRow::filter(ChangeRow::fields().id().eq(change_id))
            .first()
            .exec(&mut *db)
            .await?
        {
            return Ok(match (existing.accepted_seq, existing.rejection) {
                (Some(seq), _) => accepted(seq),
                (None, Some(r)) => rejected(Reason::Invalid, r),
                (None, None) => rejected(Reason::Invalid, "change known but undecided"),
            });
        }
        if env.parts.is_empty() {
            return Ok(rejected(Reason::Invalid, "no parts"));
        }

        // Stage every part on a candidate document before touching storage.
        let mut staged: Vec<(ObjectDoc, Object, usize)> = Vec::with_capacity(env.parts.len());
        for part in &env.parts {
            let Ok(object_id) = ObjectId::parse(&part.object_id) else {
                return Ok(rejected(Reason::Invalid, "object id"));
            };
            let (doc, pending) = match load_doc(&mut db, object_id).await {
                Ok((doc, pending)) => {
                    match doc.import(&part.bytes) {
                        Ok(true) => {}
                        Ok(false) => {
                            return Ok(rejected(
                                Reason::MissingPredecessors,
                                "operations build on history the server does not have",
                            ))
                        }
                        Err(e) => return Ok(rejected(Reason::Invalid, e.to_string())),
                    }
                    (doc, pending)
                }
                Err(StoreError::ObjectNotFound(_)) => {
                    match ObjectDoc::from_updates(
                        object_id,
                        std::slice::from_ref(&part.bytes.to_vec()),
                    ) {
                        Ok(doc) => (doc, 0),
                        Err(ModelError::Malformed(_)) => {
                            return Ok(rejected(
                                Reason::MissingPredecessors,
                                "operations build on history the server does not have",
                            ));
                        }
                        Err(e) => return Ok(rejected(Reason::Invalid, e.to_string())),
                    }
                }
                Err(e) => return Err(e),
            };
            let rec = match doc.read() {
                Ok(r) => r,
                Err(e) => return Ok(rejected(Reason::Invalid, e.to_string())),
            };
            if rec.kind != part.kind {
                return Ok(rejected(Reason::Invalid, "kind mismatch"));
            }
            staged.push((doc, rec, pending));
        }

        let seq = last_seq(&mut db).await? + 1;
        let mut tx = db.transaction().await?;
        write_change(&mut tx, env, ChangeStatus::Accepted, Some(seq), "").await?;
        toasty::create!(SpaceLogRow {
            seq,
            change_id,
            accepted_at: now_ms(),
        })
        .exec(&mut tx)
        .await?;
        for ((doc, rec, pending), part) in staged.iter().zip(&env.parts) {
            install_part(&mut tx, doc, rec, part, change_id, *pending).await?;
        }
        tx.commit().await?;
        Ok(accepted(seq))
    }

    /// Replica side: installs an accepted Change delivered from the
    /// authority at log position `seq`. A Change this device produced is
    /// only marked accepted; a foreign one is applied to the documents.
    pub async fn apply_accepted(&self, seq: i64, env: &Envelope) -> Result<(), StoreError> {
        let change_id = uuid_arg(&env.id)?;
        let mut db = self.db.lock().await;
        let mine = ChangeRow::filter(ChangeRow::fields().id().eq(change_id))
            .first()
            .exec(&mut *db)
            .await?;
        let mut installed: Vec<(Uuid, i64, bool)> = Vec::new();
        let mut tx = db.transaction().await?;
        if let Some(row) = mine {
            if row.accepted_seq != Some(seq) {
                toasty::update!(ChangeRow::filter(ChangeRow::fields().id().eq(change_id)) {
                    status: status_code(ChangeStatus::Accepted),
                    accepted_seq: Some(seq),
                })
                .exec(&mut tx)
                .await?;
            }
        } else {
            write_change(&mut tx, env, ChangeStatus::Accepted, Some(seq), "").await?;
            for part in &env.parts {
                let object_id = ObjectId::parse(&part.object_id)?;
                let (doc, pending) = match load_doc_tx(&mut tx, object_id).await {
                    Ok((doc, pending)) => {
                        if !doc.import(&part.bytes)? {
                            return Err(StoreError::Model(ModelError::Malformed(
                                "accepted change with missing predecessors",
                            )));
                        }
                        (doc, pending)
                    }
                    Err(StoreError::ObjectNotFound(_)) => (
                        ObjectDoc::from_updates(
                            object_id,
                            std::slice::from_ref(&part.bytes.to_vec()),
                        )?,
                        0,
                    ),
                    Err(e) => return Err(e),
                };
                let rec = doc.read()?;
                install_part(&mut tx, &doc, &rec, part, change_id, pending).await?;
                installed.push((object_id.uuid(), i64::from(rec.kind), true));
            }
        }
        SpaceLogRow::filter_by_seq(seq)
            .delete()
            .exec(&mut tx)
            .await?;
        toasty::create!(SpaceLogRow {
            seq,
            change_id,
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
        self.emit_change(change_id, ChangeStatus::Accepted);
        self.emit_objects(&installed);
        Ok(())
    }
}

async fn change_row(db: &mut Db, id: Uuid) -> Result<ChangeRow, StoreError> {
    ChangeRow::filter(ChangeRow::fields().id().eq(id))
        .first()
        .exec(&mut *db)
        .await?
        .ok_or_else(|| StoreError::ChangeNotFound(id.to_string()))
}

async fn read_change(db: &mut Db, space: SpaceId, row: &ChangeRow) -> Result<Change, StoreError> {
    let env = read_envelope(db, space, row).await?;
    let deps = ChangeDepRow::filter(ChangeDepRow::fields().change_id().eq(row.id))
        .exec(&mut *db)
        .await?;
    Ok(Change {
        id: env.id,
        space_id: env.space_id,
        device_id: env.device_id,
        created_at_ms: env.created_at_ms,
        status: status_of(row.status)?.into(),
        parts: env
            .parts
            .iter()
            .map(|p| ChangePart {
                object_id: p.object_id.clone(),
                bytes_len: u32::try_from(p.bytes.len()).unwrap_or(u32::MAX),
            })
            .collect(),
        accepted_seq: row.accepted_seq,
        rejection: row.rejection.clone().unwrap_or_default(),
        depends_on: deps.iter().map(|d| d.depends_on.to_string()).collect(),
    })
}

/// Object ids a record refers to: field references, participants and
/// their roles, placed blocks, the Layer target, and applied definitions.
fn references(rec: &Object) -> Vec<Uuid> {
    let mut out: Vec<Uuid> = Vec::new();
    let mut push = |s: &str| {
        if let Ok(id) = Uuid::parse_str(s) {
            out.push(id);
        }
    };
    for v in rec.fields.values() {
        if let Some(value::Value::Ref(r)) = &v.value {
            push(r);
        }
    }
    for p in &rec.participants {
        push(&p.object_id);
        push(&p.role_id);
    }
    for p in &rec.placements {
        push(&p.block_id);
    }
    if let Some(t) = &rec.target_id {
        push(t);
    }
    for t in &rec.types {
        push(t);
    }
    for f in &rec.type_fields {
        push(&f.field_id);
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// The Changes a new write to `rec` builds on: the latest unaccepted Change
/// to the same document, and the creating Change of every Object `rec`
/// refers to while that Change is unaccepted.
async fn dependencies(db: &mut Db, rec: &Object) -> Result<Vec<Uuid>, StoreError> {
    let own = uuid_arg(&rec.id)?;
    let mut deps = Vec::new();
    if let Some(latest) = change_touching(db, own, false).await? {
        if latest.1 != ChangeStatus::Accepted {
            deps.push(latest.0);
        }
    }
    for referenced in references(rec) {
        if referenced == own {
            continue;
        }
        if let Some((id, status)) = change_touching(db, referenced, true).await? {
            if status != ChangeStatus::Accepted {
                deps.push(id);
            }
        }
    }
    deps.sort_unstable();
    deps.dedup();
    Ok(deps)
}

/// The first (creating) or latest Change with a part on `object`.
async fn change_touching(
    db: &mut Db,
    object: Uuid,
    first: bool,
) -> Result<Option<(Uuid, ChangeStatus)>, StoreError> {
    let order = if first { "ASC" } else { "DESC" };
    let rows = toasty::sql::query(format!(
        "SELECT c.id, c.status FROM changes c JOIN change_parts p ON p.change_id = c.id \
         WHERE p.object_id = ?1 ORDER BY c.seq {order} LIMIT 1"
    ))
    .bind(uuid_param(object))
    .exec(&mut *db)
    .await?;
    let Some(Value::Record(rec)) = rows.into_iter().next() else {
        return Ok(None);
    };
    let id = uuid_of(&rec[0], "changes", "id")?;
    let Value::I64(status) = rec[1] else {
        return Err(StoreError::Corrupt {
            table: "changes",
            what: "status",
        });
    };
    Ok(Some((id, status_of(status)?)))
}

async fn write_deps(
    tx: &mut toasty::Transaction<'_>,
    change_id: Uuid,
    deps: &[Uuid],
) -> Result<(), StoreError> {
    for dep in deps {
        toasty::create!(ChangeDepRow {
            change_id,
            depends_on: *dep,
        })
        .exec(&mut *tx)
        .await?;
    }
    Ok(())
}

/// Every Change that transitively depends on `root` and is in one of
/// `statuses`, in dependency order.
async fn dependents(
    db: &mut Db,
    root: Uuid,
    statuses: &[ChangeStatus],
) -> Result<Vec<Uuid>, StoreError> {
    let mut out: Vec<Uuid> = Vec::new();
    let mut frontier = vec![root];
    while let Some(id) = frontier.pop() {
        let edges = ChangeDepRow::filter(ChangeDepRow::fields().depends_on().eq(id))
            .exec(&mut *db)
            .await?;
        for edge in edges {
            if out.contains(&edge.change_id) {
                continue;
            }
            let row = change_row(db, edge.change_id).await?;
            if statuses.contains(&status_of(row.status)?) {
                out.push(edge.change_id);
                frontier.push(edge.change_id);
            }
        }
    }
    Ok(out)
}

/// Objects touched by any of `changes`, with their kind codes.
async fn objects_of(db: &mut Db, changes: &[Uuid]) -> Result<Vec<(Uuid, i64)>, StoreError> {
    let mut out: Vec<(Uuid, i64)> = Vec::new();
    for id in changes {
        let parts = ChangePartRow::filter(ChangePartRow::fields().change_id().eq(*id))
            .exec(&mut *db)
            .await?;
        out.extend(parts.iter().map(|p| (p.object_id, p.kind)));
    }
    out.sort_unstable();
    out.dedup_by_key(|(id, _)| *id);
    Ok(out)
}

/// Rebuilds a document from the parts of every Change that is neither
/// rejected nor paused, in recording order, and rewrites its snapshot,
/// update log, and projections. A document none of them created is
/// removed from the live state; its parts stay with their Changes.
/// Returns whether the document is live afterwards.
async fn rebuild_doc(tx: &mut toasty::Transaction<'_>, object: Uuid) -> Result<bool, StoreError> {
    let rows = toasty::sql::query(
        "SELECT p.bytes FROM change_parts p JOIN changes c ON c.id = p.change_id \
         WHERE p.object_id = ?1 AND c.status NOT IN (?2, ?3) ORDER BY c.seq ASC",
    )
    .bind(uuid_param(object))
    .bind(Value::I64(status_code(ChangeStatus::Rejected)))
    .bind(Value::I64(status_code(ChangeStatus::Paused)))
    .exec(&mut *tx)
    .await?;
    let mut updates: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
    for row in rows {
        let Value::Record(rec) = row else {
            return Err(StoreError::Corrupt {
                table: "change_parts",
                what: "row shape",
            });
        };
        match &rec[0] {
            Value::Bytes(b) => updates.push(b.clone()),
            _ => {
                return Err(StoreError::Corrupt {
                    table: "change_parts",
                    what: "bytes",
                })
            }
        }
    }
    DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(object))
        .delete()
        .exec(&mut *tx)
        .await?;
    DocRow::filter_by_object_id(object)
        .delete()
        .exec(&mut *tx)
        .await?;
    delete_projections(tx, object).await?;
    if updates.is_empty() {
        return Ok(false);
    }
    let id = ObjectId::from_uuid(object);
    let doc = match ObjectDoc::from_updates(id, &updates) {
        Ok(doc) => doc,
        // Nothing left that created the document: it leaves the live state.
        Err(ModelError::Malformed(_)) => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let rec = doc.read()?;
    write_snapshot(tx, &doc, &rec).await?;
    write_projections(tx, &rec).await?;
    Ok(true)
}

async fn delete_projections(tx: &mut toasty::Transaction<'_>, id: Uuid) -> Result<(), StoreError> {
    ObjectRow::filter_by_object_id(id)
        .delete()
        .exec(&mut *tx)
        .await?;
    ObjectTypeRow::filter(ObjectTypeRow::fields().object_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    FieldValueRow::filter(FieldValueRow::fields().object_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    TypeFieldRow::filter(TypeFieldRow::fields().type_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    ParticipantRow::filter(ParticipantRow::fields().relation_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    PlacementRow::filter(PlacementRow::fields().document_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    Ok(())
}

async fn read_envelope(
    db: &mut Db,
    space: SpaceId,
    row: &ChangeRow,
) -> Result<Envelope, StoreError> {
    let parts = ChangePartRow::filter(ChangePartRow::fields().change_id().eq(row.id))
        .exec(&mut *db)
        .await?;
    let mut parts: Vec<Part> = parts
        .into_iter()
        .map(|p| Part {
            object_id: ObjectId::from_uuid(p.object_id).to_string(),
            kind: i32::try_from(p.kind).unwrap_or_default(),
            from_version: p.base_version.into(),
            to_version: p.next_version.into(),
            bytes: p.bytes.into(),
        })
        .collect();
    parts.sort_by(|a, b| a.object_id.cmp(&b.object_id));
    Ok(Envelope {
        id: ObjectId::from_uuid(row.id).to_string(),
        space_id: space.to_string(),
        device_id: row.device_id.clone(),
        created_at_ms: row.created_at,
        parts,
    })
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

async fn write_change(
    tx: &mut toasty::Transaction<'_>,
    env: &Envelope,
    status: ChangeStatus,
    accepted_seq: Option<i64>,
    rejection: &str,
) -> Result<(), StoreError> {
    let change_id = uuid_arg(&env.id)?;
    toasty::create!(ChangeRow {
        id: change_id,
        device_id: env.device_id.clone(),
        created_at: env.created_at_ms,
        status: status_code(status),
        accepted_seq,
        rejection: (!rejection.is_empty()).then(|| rejection.to_owned()),
    })
    .exec(&mut *tx)
    .await?;
    for part in &env.parts {
        toasty::create!(ChangePartRow {
            change_id,
            object_id: uuid_arg(&part.object_id)?,
            kind: i64::from(part.kind),
            base_version: part.from_version.to_vec(),
            next_version: part.to_version.to_vec(),
            bytes: part.bytes.to_vec(),
        })
        .exec(&mut *tx)
        .await?;
    }
    Ok(())
}

async fn write_snapshot(
    tx: &mut toasty::Transaction<'_>,
    doc: &ObjectDoc,
    rec: &Object,
) -> Result<(), StoreError> {
    let id = uuid_arg(&rec.id)?;
    DocRow::filter_by_object_id(id)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(DocRow {
        object_id: id,
        kind: i64::from(rec.kind),
        snapshot: doc.snapshot()?,
        version: doc.version(),
        updated_at: rec.updated_at_ms,
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
}

/// Writes an applied part: either the first snapshot of a new document or
/// one more entry in its update log (compacting when due), plus the
/// projections.
async fn install_part(
    tx: &mut toasty::Transaction<'_>,
    doc: &ObjectDoc,
    rec: &Object,
    part: &Part,
    change_id: Uuid,
    pending: usize,
) -> Result<(), StoreError> {
    let object_id = uuid_arg(&part.object_id)?;
    let is_new = DocRow::filter_by_object_id(object_id)
        .first()
        .exec(&mut *tx)
        .await?
        .is_none();
    if is_new {
        write_snapshot(tx, doc, rec).await?;
    } else {
        toasty::create!(DocUpdateRow {
            object_id,
            change_id,
            bytes: part.bytes.to_vec(),
        })
        .exec(&mut *tx)
        .await?;
        if pending + 1 >= COMPACT_AFTER_UPDATES {
            write_snapshot(tx, doc, rec).await?;
            DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(object_id))
                .delete()
                .exec(&mut *tx)
                .await?;
        }
    }
    write_projections(tx, rec).await
}

async fn write_projections(
    tx: &mut toasty::Transaction<'_>,
    rec: &Object,
) -> Result<(), StoreError> {
    let id = uuid_arg(&rec.id)?;
    ObjectRow::filter_by_object_id(id)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(ObjectRow {
        object_id: id,
        kind: i64::from(rec.kind),
        created_at: rec.created_at_ms,
        updated_at: rec.updated_at_ms,
        deleted_at: rec.deleted_at_ms,
        target_id: rec.target_id.as_deref().map(uuid_arg).transpose()?,
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
    for (field, v) in &rec.fields {
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
        match &v.value {
            Some(value::Value::Bool(b)) => row.v_bool = Some(*b),
            Some(value::Value::Int(i)) => row.v_int = Some(*i),
            Some(value::Value::Double(d)) => row.v_real = Some(*d),
            Some(value::Value::String(s)) => row.v_text = Some(s.clone()),
            Some(value::Value::TimeMs(t)) => row.v_time = Some(*t),
            Some(value::Value::Ref(r)) => row.v_ref = Some(uuid_arg(r)?),
            Some(value::Value::Text(t)) => row.v_text = Some(t.clone()),
            // Lists are not projected yet; they are read from the document.
            Some(value::Value::List(_)) | None => continue,
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

    TypeFieldRow::filter(TypeFieldRow::fields().type_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    for f in &rec.type_fields {
        toasty::create!(TypeFieldRow {
            type_id: id,
            field_id: f.field_id.clone(),
            required: f.required,
        })
        .exec(&mut *tx)
        .await?;
    }

    write_links(tx, id, rec).await
}

/// Projections that point at other Objects: participants and placements.
async fn write_links(
    tx: &mut toasty::Transaction<'_>,
    id: Uuid,
    rec: &Object,
) -> Result<(), StoreError> {
    ParticipantRow::filter(ParticipantRow::fields().relation_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    for p in &rec.participants {
        toasty::create!(ParticipantRow {
            relation_id: id,
            slot: p.slot.clone(),
            object_id: uuid_arg(&p.object_id)?,
            role_id: p.role_id.clone(),
        })
        .exec(&mut *tx)
        .await?;
    }

    PlacementRow::filter(PlacementRow::fields().document_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    for p in &rec.placements {
        toasty::create!(PlacementRow {
            document_id: id,
            placement_id: p.id.clone(),
            parent_id: p.parent_id.clone(),
            index: i64::from(p.index),
            block_id: uuid_arg(&p.block_id)?,
        })
        .exec(&mut *tx)
        .await?;
    }
    Ok(())
}

/// Unused import guard: keep the protocol value type name available for
/// callers that want to build values without reaching into prost modules.
pub type FieldValue = PbValue;
