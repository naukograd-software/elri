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
use elri_proto::elri::v1::definition::{Pin, Schema};
use elri_proto::elri::v1::events::{
    event, ChangeEvent, Event, ObjectEvent, RemoteEvent, SpaceEvent,
};
use elri_proto::elri::v1::object::{value, Object, ObjectKind, Value as PbValue};
use elri_proto::elri::v1::script::Grant;
use elri_proto::elri::v1::space::{Authority, Role, Space};
use elri_proto::elri::v1::sync::{Entry, Envelope, Part};
use loro::VersionVector;
use toasty::stmt::Value;
use toasty::Db;
use toasty_driver_turso::Turso;
use tokio::sync::{broadcast, Mutex, Notify};
use uuid::Uuid;

use crate::blobs::Blobs;
use crate::change::{accepted, rejected, Outcome, Reason};
use crate::device::DeviceId;
use crate::model::{
    field_def_kind, now_ms, publication_of, ModelError, ObjectDoc, ObjectId, SpaceId,
};
use models::{
    BlobRow, ChangeDepRow, ChangePartRow, ChangeRow, DocRow, DocUpdateRow, FieldValueRow, GrantRow,
    GraveyardRow, JournalRow, ObjectRow, ObjectTypeRow, ParticipantRow, PinRow, PlacementRow,
    RemoteRow, SettingRow, SpaceLogRow, SpaceRow, SyncStateRow, TypeFieldRow,
};

mod coordinator;
pub use coordinator::{Edit, Journaled};

/// Actor of a Change the user made directly.
pub const ACTOR_USER: &str = "user";
/// Actor of state installed from the authority.
pub const ACTOR_REMOTE: &str = "remote";

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
    /// No such grant on this device.
    #[error("grant {0} not found")]
    GrantNotFound(String),
    /// The schema epoch moved since the caller looked.
    #[error("schema epoch is {actual}, not {expected}; look again")]
    EpochMoved {
        /// What the caller expected.
        expected: i64,
        /// What it is.
        actual: i64,
    },
    /// Not a publication Object.
    #[error("object {0} is not a publication")]
    NotPublication(ObjectId),
    /// The Space's initial download is not finished; it is read-only.
    #[error("space is downloading: {received} of {target} accepted changes received")]
    Downloading {
        /// Highest sequence installed.
        received: i64,
        /// Sequence the download must reach.
        target: i64,
    },
    /// The Object was purged; its identity cannot be used again.
    #[error("object {0} was purged")]
    Purged(ObjectId),
    /// Purge needs a deleted Object.
    #[error("object {0} is not deleted; delete it first")]
    NotDeleted(ObjectId),
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
    /// Access token; kept in the secret store, never in the database. Empty
    /// when read from the database.
    pub token: String,
    /// User id on the server.
    pub user_id: String,
    /// User name on the server.
    pub user_name: String,
}

/// Opens a database file, creating its schema when the file is new. The
/// flag says whether it was new.
async fn open_db(path: &Path, models: toasty::ModelSet) -> Result<(Db, bool), StoreError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| StoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let fresh = !path.exists();
    let db = Db::builder()
        .models(models)
        .build(Turso::file(path).experimental_index_method(true))
        .await?;
    if fresh {
        db.push_schema().await?;
    }
    restrict_to_owner(path);
    Ok((db, fresh))
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
    /// Visibility gate: held for writing while a coordinated Change is
    /// being installed across Spaces, so readers see it whole or not at
    /// all.
    gate: tokio::sync::RwLock<()>,
    /// Signalled after every coordinated write, for the coordinator's
    /// submission worker.
    dirty: Notify,
    blobs: Blobs,
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
        let (db, _) = open_db(
            &root.join("device.db"),
            toasty::models!(SpaceRow, RemoteRow, JournalRow, GrantRow, SettingRow),
        )
        .await?;
        let blobs = Blobs::new(&root);
        let store = Self {
            root,
            device_id,
            device: Mutex::new(db),
            spaces: Mutex::new(HashMap::new()),
            events: broadcast::channel(EVENT_BUFFER).0,
            gate: tokio::sync::RwLock::new(()),
            dirty: Notify::new(),
            blobs,
        };
        store.recover().await?;
        Ok(store)
    }

    /// Resolves after the next coordinated write (or at once if one
    /// happened since the last call).
    pub async fn wait_dirty(&self) {
        self.dirty.notified().await;
    }

    /// Writes a compacted copy of every database into `dir` (`device.db`
    /// and `spaces/<id>.db`) at one consistent moment: every lock is held
    /// while the engine copies, so no write lands between files. Returns
    /// the device id and a summary of each Space.
    pub async fn backup_into(
        &self,
        dir: &Path,
    ) -> Result<(DeviceId, Vec<crate::backup::SpaceSummary>), StoreError> {
        let listed = self.list_spaces().await?;
        let mut spaces = Vec::with_capacity(listed.len());
        for s in &listed {
            spaces.push(self.space(ObjectId::parse(&s.id)?).await?);
        }
        // The barrier: device db first, then every Space, all held until
        // the last copy is written.
        let mut device = self.device.lock().await;
        let mut guards = Vec::with_capacity(spaces.len());
        for s in &spaces {
            guards.push(s.db.lock().await);
        }
        vacuum_into(&mut device, &dir.join("device.db")).await?;
        let mut summaries = Vec::with_capacity(spaces.len());
        for (guard, info) in guards.iter_mut().zip(&listed) {
            vacuum_into(guard, &dir.join("spaces").join(format!("{}.db", info.id))).await?;
            let cursor = read_cursor(guard).await?;
            let (pending, rejected) = pending_counts(guard).await?;
            summaries.push(crate::backup::SpaceSummary {
                id: info.id.clone(),
                name: info.name.clone(),
                authority: info.authority,
                cursor,
                pending,
                rejected,
            });
        }
        Ok((self.device_id, summaries))
    }

    /// Opens every copied database in `dir` and reads from it, the way a
    /// restore would.
    pub async fn verify_copies(
        dir: &Path,
        files: &[crate::backup::FileEntry],
    ) -> Result<(), StoreError> {
        for f in files {
            // Blobs are checked by hash alone; only databases are opened.
            if f.path.starts_with("blobs/") {
                continue;
            }
            let path = dir.join(&f.path);
            let mut db = Db::builder()
                .models(if f.path == "device.db" {
                    toasty::models!(SpaceRow, RemoteRow)
                } else {
                    toasty::models!(DocRow, ChangeRow, SpaceLogRow)
                })
                .build(Turso::file(&path).experimental_index_method(true))
                .await?;
            let table = if f.path == "device.db" {
                "spaces"
            } else {
                "objects"
            };
            toasty::sql::query(format!("SELECT count(*) FROM {table}"))
                .exec(&mut db)
                .await?;
        }
        Ok(())
    }

    /// Attachment bytes of this device.
    pub fn blobs(&self) -> &Blobs {
        &self.blobs
    }

    /// Purges expired deleted Objects in every Space this store is the
    /// authority for: local Spaces on a device, every Space on a server.
    pub async fn purge_expired(&self, all: bool) -> Result<usize, StoreError> {
        let now = now_ms();
        let mut count = 0;
        for info in self.list_spaces().await? {
            if !all && info.authority() != Authority::Local {
                continue;
            }
            let space = self.space(ObjectId::parse(&info.id)?).await?;
            count += space.purge_expired(now).await?.len();
        }
        Ok(count)
    }

    // --- grants and settings ----------------------------------------------

    /// A device setting; `default` when unset.
    pub async fn setting(&self, key: &str, default: i64) -> Result<i64, StoreError> {
        let mut db = self.device.lock().await;
        Ok(SettingRow::filter_by_key(key.to_owned())
            .first()
            .exec(&mut *db)
            .await?
            .map_or(default, |r| r.value))
    }

    /// Sets a device setting.
    pub async fn set_setting(&self, key: &str, value: i64) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        SettingRow::filter_by_key(key.to_owned())
            .delete()
            .exec(&mut *db)
            .await?;
        toasty::create!(SettingRow {
            key: key.to_owned(),
            value,
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// The live grant for a script version, if any.
    pub async fn grant_for(
        &self,
        script_id: ObjectId,
        version: &str,
    ) -> Result<Option<Grant>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = GrantRow::filter(
            GrantRow::fields()
                .script_id()
                .eq(script_id.uuid())
                .and(GrantRow::fields().version().eq(version.to_owned())),
        )
        .exec(&mut *db)
        .await?;
        Ok(rows
            .iter()
            .filter(|r| r.revoked_at.is_none())
            .map(grant_of)
            .next())
    }

    /// Records consent to run a script version on this device.
    pub async fn grant(
        &self,
        space_id: SpaceId,
        script_id: ObjectId,
        version: &str,
        benign: bool,
    ) -> Result<Grant, StoreError> {
        if let Some(existing) = self.grant_for(script_id, version).await? {
            return Ok(existing);
        }
        let mut db = self.device.lock().await;
        let row = GrantRow {
            id: Uuid::now_v7(),
            space_id: space_id.uuid(),
            script_id: script_id.uuid(),
            version: version.to_owned(),
            benign,
            granted_at: now_ms(),
            revoked_at: None,
        };
        let grant = grant_of(&row);
        toasty::create!(GrantRow {
            id: row.id,
            space_id: row.space_id,
            script_id: row.script_id,
            version: row.version,
            benign: row.benign,
            granted_at: row.granted_at,
            revoked_at: None,
        })
        .exec(&mut *db)
        .await?;
        Ok(grant)
    }

    /// Revokes a grant; the script stops running at once.
    pub async fn revoke_grant(&self, id: &str) -> Result<(), StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        if GrantRow::filter_by_id(id)
            .first()
            .exec(&mut *db)
            .await?
            .is_none()
        {
            return Err(StoreError::GrantNotFound(id.to_string()));
        }
        toasty::update!(GrantRow::filter_by_id(id) {
            revoked_at: Some(now_ms()),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Grants on this device, newest first.
    pub async fn grants(
        &self,
        space: Option<SpaceId>,
        include_revoked: bool,
    ) -> Result<Vec<Grant>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = GrantRow::all().exec(&mut *db).await?;
        let mut out: Vec<Grant> = rows
            .iter()
            .filter(|r| space.is_none_or(|s| r.space_id == s.uuid()))
            .filter(|r| include_revoked || r.revoked_at.is_none())
            .map(grant_of)
            .collect();
        out.sort_by_key(|g| std::cmp::Reverse(g.granted_at_ms));
        Ok(out)
    }

    /// Subscribes to everything that happens in this store from now on.
    pub fn watch(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Announces a change of the server link.
    pub fn emit_remote(&self) {
        emit(&self.events, event::Event::Remote(RemoteEvent {}));
    }

    /// Publishes an event to every watcher.
    pub fn emit(&self, body: event::Event) {
        emit(&self.events, body);
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
        let space = SpaceStore::open(
            &self.space_path(id),
            self.device_id,
            self.events.clone(),
            self.blobs.clone(),
        )
        .await?;
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
            last_seq: 0,
            retention_days: 0,
        })
    }

    /// The storage of one Space, opened on first use.
    pub async fn space(&self, id: SpaceId) -> Result<Arc<SpaceStore>, StoreError> {
        let _visible = self.gate.read().await;
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
            SpaceStore::open(
                &self.space_path(id),
                self.device_id,
                self.events.clone(),
                self.blobs.clone(),
            )
            .await?,
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
                token: String::new(),
                user_id: r.user_id,
                user_name: r.user_name,
            }))
    }

    /// Stores the server configuration, replacing any previous one. The
    /// token is not stored here.
    pub async fn set_remote(&self, cfg: &RemoteConfig) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        RemoteRow::filter_by_key("server".to_owned())
            .delete()
            .exec(&mut *db)
            .await?;
        toasty::create!(RemoteRow {
            key: "server".to_owned(),
            url: cfg.url.clone(),
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

fn grant_of(r: &GrantRow) -> Grant {
    Grant {
        id: r.id.to_string(),
        space_id: ObjectId::from_uuid(r.space_id).to_string(),
        script_id: ObjectId::from_uuid(r.script_id).to_string(),
        version: r.version.clone(),
        benign: r.benign,
        granted_at_ms: r.granted_at,
        revoked_at_ms: r.revoked_at,
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
        last_seq: 0,
        retention_days: 0,
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
    blobs: Blobs,
}

impl SpaceStore {
    async fn open(
        path: &Path,
        device_id: DeviceId,
        events: broadcast::Sender<Event>,
        blobs: Blobs,
    ) -> Result<Self, StoreError> {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or(StoreError::Corrupt {
                table: "spaces",
                what: "file name",
            })?;
        let id = ObjectId::parse(stem)?;
        let (mut db, fresh) = open_db(
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
                GraveyardRow,
                BlobRow,
                PinRow,
                SpaceLogRow,
                SyncStateRow
            ),
        )
        .await?;
        if fresh {
            // Full-text index over projected string and text values; kept
            // in step with the table by the engine.
            toasty::sql::query("CREATE INDEX field_values_fts ON field_values USING fts(v_text)")
                .exec(&mut db)
                .await?;
        }
        Ok(Self {
            id,
            device_id,
            db: Mutex::new(db),
            dirty: Notify::new(),
            events,
            blobs,
        })
    }

    /// This Space's id.
    pub fn id(&self) -> SpaceId {
        self.id
    }

    /// Attachment bytes.
    pub fn blobs(&self) -> &Blobs {
        &self.blobs
    }

    // --- attachment bytes ------------------------------------------------

    /// What this device knows about a blob: `(size, pinned, uploaded)`.
    pub async fn blob(&self, sha256: &str) -> Result<Option<(i64, bool, bool)>, StoreError> {
        let mut db = self.db.lock().await;
        Ok(BlobRow::filter_by_sha256(sha256.to_owned())
            .first()
            .exec(&mut *db)
            .await?
            .map(|b| (b.size, b.pinned, b.uploaded)))
    }

    /// Records that the bytes are here; `uploaded` says whether the
    /// authority has them too.
    pub async fn record_blob(
        &self,
        sha256: &str,
        size: i64,
        uploaded: bool,
    ) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        let existing = BlobRow::filter_by_sha256(sha256.to_owned())
            .first()
            .exec(&mut *db)
            .await?;
        match existing {
            Some(row) => {
                toasty::update!(BlobRow::filter_by_sha256(sha256.to_owned()) {
                    uploaded: row.uploaded || uploaded,
                })
                .exec(&mut *db)
                .await?;
            }
            None => {
                toasty::create!(BlobRow {
                    sha256: sha256.to_owned(),
                    size,
                    pinned: false,
                    uploaded,
                    created_at: now_ms(),
                })
                .exec(&mut *db)
                .await?;
            }
        }
        Ok(())
    }

    /// Marks a blob pinned or not.
    pub async fn set_blob_pinned(&self, sha256: &str, pinned: bool) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        toasty::update!(BlobRow::filter_by_sha256(sha256.to_owned()) { pinned })
            .exec(&mut *db)
            .await?;
        Ok(())
    }

    /// Marks that the authority has the bytes.
    pub async fn set_blob_uploaded(&self, sha256: &str) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        toasty::update!(BlobRow::filter_by_sha256(sha256.to_owned()) { uploaded: true })
            .exec(&mut *db)
            .await?;
        Ok(())
    }

    /// Digests of blobs this device holds, with sizes.
    pub async fn blobs_held(&self) -> Result<Vec<(String, i64)>, StoreError> {
        let mut db = self.db.lock().await;
        Ok(BlobRow::all()
            .exec(&mut *db)
            .await?
            .into_iter()
            .map(|b| (b.sha256, b.size))
            .collect())
    }

    /// Digests named by live attachment Objects of this Space.
    pub async fn attachment_digests(&self) -> Result<Vec<String>, StoreError> {
        let mut db = self.db.lock().await;
        let rows = toasty::sql::query(
            "SELECT DISTINCT f.v_text FROM field_values f JOIN objects o ON o.object_id = f.object_id \
             WHERE f.field_id = ?1 AND o.kind = ?2",
        )
        .bind(Value::String(crate::model::reserved::SHA256.to_owned()))
        .bind(Value::I64(i64::from(ObjectKind::Attachment as i32)))
        .exec(&mut *db)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if let Value::Record(rec) = row {
                if let Value::String(s) = &rec[0] {
                    out.push(s.clone());
                }
            }
        }
        Ok(out)
    }

    fn emit_object(&self, rec: &Object, actor: &str) {
        emit(
            &self.events,
            event::Event::Object(ObjectEvent {
                space_id: self.id.to_string(),
                object_id: rec.id.clone(),
                kind: rec.kind,
                removed: false,
                actor: actor.to_owned(),
            }),
        );
    }

    fn emit_objects(&self, touched: &[(Uuid, i64, bool)], actor: &str) {
        for (object, kind, live) in touched {
            emit(
                &self.events,
                event::Event::Object(ObjectEvent {
                    space_id: self.id.to_string(),
                    object_id: object.to_string(),
                    kind: i32::try_from(*kind).unwrap_or_default(),
                    removed: !live,
                    actor: actor.to_owned(),
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

    fn new_envelope(&self, parts: Vec<Part>, actor: &str) -> Envelope {
        Envelope {
            id: ObjectId::new().to_string(),
            space_id: self.id.to_string(),
            device_id: self.device_id.as_string(),
            created_at_ms: now_ms(),
            parts,
            actor: actor.to_owned(),
        }
    }

    /// Refuses local writes while the initial download is incomplete.
    async fn writable(db: &mut Db) -> Result<(), StoreError> {
        let target = read_state(db, INITIAL_TARGET).await?;
        let cursor = read_cursor(db).await?;
        if cursor < target {
            return Err(StoreError::Downloading {
                received: cursor,
                target,
            });
        }
        Ok(())
    }

    /// Marks the sequence the initial download must reach before the Space
    /// accepts local writes; 0 for a Space that starts empty.
    pub async fn set_initial_target(&self, seq: i64) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        write_state(&mut db, INITIAL_TARGET, seq).await
    }

    /// `(received, target)` while the initial download is incomplete.
    pub async fn downloading(&self) -> Result<Option<(i64, i64)>, StoreError> {
        let mut db = self.db.lock().await;
        let target = read_state(&mut db, INITIAL_TARGET).await?;
        let cursor = read_cursor(&mut db).await?;
        Ok((cursor < target).then_some((cursor, target)))
    }

    /// Stores a freshly created document as one Change with a full snapshot,
    /// attributed to the user.
    pub async fn insert(&self, doc: &ObjectDoc) -> Result<Object, StoreError> {
        self.insert_as(doc, ACTOR_USER).await
    }

    /// [`Self::insert`] attributed to `actor`.
    pub async fn insert_as(&self, doc: &ObjectDoc, actor: &str) -> Result<Object, StoreError> {
        let rec = doc.read()?;
        let empty = VersionVector::default();
        let part = Part {
            object_id: rec.id.clone(),
            kind: rec.kind,
            from_version: empty.encode().into(),
            to_version: doc.version().into(),
            bytes: doc.updates_since(&empty)?.into(),
            space_id: String::new(),
        };
        let env = self.new_envelope(vec![part], actor);
        let mut db = self.db.lock().await;
        Self::writable(&mut db).await?;
        if is_purged(&mut db, doc.id().uuid()).await? {
            return Err(StoreError::Purged(doc.id()));
        }
        let deps = dependencies(&mut db, &rec).await?;
        let mut tx = db.transaction().await?;
        write_change(&mut tx, &env, ChangeStatus::Local, None, "").await?;
        write_deps(&mut tx, uuid_arg(&env.id)?, &deps).await?;
        write_snapshot(&mut tx, doc, &rec).await?;
        write_projections(&mut tx, &rec).await?;
        tx.commit().await?;
        self.dirty.notify_one();
        self.emit_change(uuid_arg(&env.id)?, ChangeStatus::Local);
        self.emit_object(&rec, actor);
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
        self.update_as(id, edit, ACTOR_USER).await
    }

    /// [`Self::update`] attributed to `actor`.
    pub async fn update_as<F>(
        &self,
        id: ObjectId,
        edit: F,
        actor: &str,
    ) -> Result<Object, StoreError>
    where
        F: FnOnce(&ObjectDoc) -> Result<(), ModelError>,
    {
        let mut db = self.db.lock().await;
        Self::writable(&mut db).await?;
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
            space_id: String::new(),
        };
        let env = self.new_envelope(vec![part], actor);
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
        self.emit_object(&rec, actor);
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
        // A pinned publication fixes the kind, whatever the live definition says.
        if let Some(pin) = PinRow::filter_by_definition_id(id.uuid())
            .first()
            .exec(&mut *db)
            .await?
        {
            if let Ok((doc, _)) = load_doc(&mut db, ObjectId::from_uuid(pin.publication_id)).await {
                if let Some(p) = publication_of(&doc.read()?) {
                    let snap: serde_json::Value =
                        serde_json::from_str(&p.snapshot_json).unwrap_or_default();
                    if let Some(code) = snap.get("kind").and_then(serde_json::Value::as_i64) {
                        if let Some(kind) = i32::try_from(code)
                            .ok()
                            .and_then(|c| FieldKind::try_from(c).ok())
                            .filter(|k| *k != FieldKind::Unspecified)
                        {
                            return Ok(Some(kind));
                        }
                    }
                }
            }
        }
        Ok(field_def_kind(&load_doc(&mut db, id).await?.0.read()?))
    }

    // --- retention ------------------------------------------------------

    /// Days a deleted Object stays recoverable; 0 = until an explicit purge.
    pub async fn retention_days(&self) -> Result<i64, StoreError> {
        let mut db = self.db.lock().await;
        Ok(
            match SyncStateRow::filter_by_key(RETENTION_DAYS.to_owned())
                .first()
                .exec(&mut *db)
                .await?
            {
                Some(r) => r.value,
                None => DEFAULT_RETENTION_DAYS,
            },
        )
    }

    /// Sets the retention.
    pub async fn set_retention_days(&self, days: i64) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        write_state(&mut db, RETENTION_DAYS, days.max(0)).await
    }

    /// Authority side: purges every Object deleted longer ago than the
    /// retention. Returns what was purged.
    pub async fn purge_expired(&self, now: i64) -> Result<Vec<ObjectId>, StoreError> {
        let days = self.retention_days().await?;
        if days <= 0 {
            return Ok(Vec::new());
        }
        let cutoff = now - days * 24 * 60 * 60 * 1000;
        let expired: Vec<ObjectId> = {
            let mut db = self.db.lock().await;
            ObjectRow::all()
                .exec(&mut *db)
                .await?
                .into_iter()
                .filter(|r| r.deleted_at.is_some_and(|d| d < cutoff))
                .map(|r| ObjectId::from_uuid(r.object_id))
                .collect()
        };
        for id in &expired {
            self.purge_object(*id).await?;
        }
        Ok(expired)
    }

    // --- schema pins ----------------------------------------------------

    /// The Space's pins under their epoch.
    pub async fn schema(&self) -> Result<Schema, StoreError> {
        let mut db = self.db.lock().await;
        let epoch = read_state(&mut db, SCHEMA_EPOCH).await?;
        let rows = PinRow::all().exec(&mut *db).await?;
        let mut pins: Vec<Pin> = rows
            .iter()
            .map(|r| Pin {
                definition_id: ObjectId::from_uuid(r.definition_id).to_string(),
                publication_id: ObjectId::from_uuid(r.publication_id).to_string(),
                version: i32::try_from(r.version).unwrap_or(i32::MAX),
                epoch: r.epoch,
            })
            .collect();
        pins.sort_by(|a, b| a.definition_id.cmp(&b.definition_id));
        Ok(Schema { epoch, pins })
    }

    /// Authority side: pins a publication against the expected epoch and
    /// records the pin in the Space log.
    pub async fn pin_definition(
        &self,
        publication: ObjectId,
        expected_epoch: i64,
    ) -> Result<Pin, StoreError> {
        let mut db = self.db.lock().await;
        let actual = read_state(&mut db, SCHEMA_EPOCH).await?;
        if actual != expected_epoch {
            return Err(StoreError::EpochMoved {
                expected: expected_epoch,
                actual,
            });
        }
        let (doc, _) = load_doc(&mut db, publication).await?;
        let Some(p) = publication_of(&doc.read()?) else {
            return Err(StoreError::NotPublication(publication));
        };
        let pin = Pin {
            definition_id: p.definition_id.clone(),
            publication_id: p.id.clone(),
            version: p.version,
            epoch: actual + 1,
        };
        let seq = last_seq(&mut db).await? + 1;
        let mut tx = db.transaction().await?;
        write_pin(&mut tx, &pin).await?;
        write_log_pin(&mut tx, seq, &pin).await?;
        write_state_tx(&mut tx, SCHEMA_EPOCH, pin.epoch).await?;
        tx.commit().await?;
        Ok(pin)
    }

    /// Replica side: installs a pin delivered from the authority.
    pub async fn apply_pin(&self, seq: i64, pin: &Pin) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        let mut tx = db.transaction().await?;
        write_pin(&mut tx, pin).await?;
        write_log_pin(&mut tx, seq, pin).await?;
        write_state_tx(&mut tx, SCHEMA_EPOCH, pin.epoch).await?;
        write_state_tx(&mut tx, CURSOR, seq).await?;
        tx.commit().await?;
        Ok(())
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

    /// Full-text search over string and text fields of live Objects, best
    /// first. Each Object appears once, with its best score and the fields
    /// that matched.
    pub async fn search(
        &self,
        query: &str,
        kind: Option<ObjectKind>,
        limit: usize,
    ) -> Result<Vec<(Object, f64, Vec<String>)>, StoreError> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        let limit = if limit == 0 { 50 } else { limit };
        let mut db = self.db.lock().await;
        // Several fields of one Object may match; fetch more rows than
        // Objects wanted and fold them.
        let rows = toasty::sql::query(
            "SELECT fts_score(v_text, ?1) AS score, object_id, field_id FROM field_values \
             WHERE fts_match(v_text, ?1) ORDER BY score DESC LIMIT ?2",
        )
        .bind(Value::String(query.to_owned()))
        .bind(Value::I64(i64::try_from(limit * 8).unwrap_or(i64::MAX)))
        .exec(&mut *db)
        .await?;
        let mut order: Vec<Uuid> = Vec::new();
        let mut best: HashMap<Uuid, (f64, Vec<String>)> = HashMap::new();
        for row in rows {
            let Value::Record(rec) = row else {
                return Err(StoreError::Corrupt {
                    table: "field_values",
                    what: "row shape",
                });
            };
            let score = match &rec[0] {
                Value::F64(f) => *f,
                _ => 0.0,
            };
            let object = uuid_of(&rec[1], "field_values", "object_id")?;
            let field = match &rec[2] {
                Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let entry = best.entry(object).or_insert_with(|| {
                order.push(object);
                (score, Vec::new())
            });
            entry.0 = entry.0.max(score);
            entry.1.push(field);
        }
        let mut out = Vec::with_capacity(order.len().min(limit));
        for id in order {
            let (doc, _) = load_doc(&mut db, ObjectId::from_uuid(id)).await?;
            let rec = doc.read()?;
            if rec.deleted_at_ms.is_some() || kind.is_some_and(|k| rec.kind() != k) {
                continue;
            }
            let (score, mut fields) = best.remove(&id).unwrap_or_default();
            fields.sort();
            fields.dedup();
            out.push((rec, score, fields));
            if out.len() == limit {
                break;
            }
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
        self.emit_objects(&rebuilt, ACTOR_USER);
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
                .eq(status_code(ChangeStatus::Local))
                .and(ChangeRow::fields().coordinated().eq(false)),
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
        pending_counts(&mut db).await
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
        self.emit_objects(&rebuilt, ACTOR_REMOTE);
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
            if let Some(object) = e.purged_object_id {
                out.push(Entry {
                    seq: e.seq,
                    change: None,
                    accepted_at_ms: e.accepted_at,
                    purged_object_id: object.to_string(),
                    pin: None,
                });
                continue;
            }
            if let Some(json) = &e.pin_json {
                out.push(Entry {
                    seq: e.seq,
                    change: None,
                    accepted_at_ms: e.accepted_at,
                    purged_object_id: String::new(),
                    pin: Some(pin_from_json(json)?),
                });
                continue;
            }
            let row = match e.change_id {
                Some(change_id) => {
                    ChangeRow::filter(ChangeRow::fields().id().eq(change_id))
                        .first()
                        .exec(&mut *db)
                        .await?
                }
                None => None,
            };
            let Some(row) = row else {
                return Err(StoreError::Corrupt {
                    table: "space_log",
                    what: "change missing",
                });
            };
            out.push(Entry {
                seq: e.seq,
                change: Some(read_envelope(&mut db, self.id, &row).await?),
                accepted_at_ms: e.accepted_at,
                purged_object_id: String::new(),
                pin: None,
            });
        }
        Ok(out)
    }

    /// Authority side: validates a submitted Change by applying its
    /// operations to candidate documents and, if everything applies, installs
    /// it at the next log position. Resubmitting a known id returns the
    /// recorded outcome.
    pub async fn admit(&self, env: &Envelope) -> Result<Outcome, StoreError> {
        self.admit_with(env, &|_| Ok(())).await
    }

    /// [`Self::admit`] with an extra check of every staged Object; a
    /// message from `check` rejects the whole Change as invalid.
    pub async fn admit_with(
        &self,
        env: &Envelope,
        check: &(dyn Fn(&Object) -> Result<(), String> + Sync),
    ) -> Result<Outcome, StoreError> {
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
            if is_purged(&mut db, object_id.uuid()).await? {
                return Ok(rejected(
                    Reason::Purged,
                    format!("object {object_id} was purged"),
                ));
            }
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
            if let Err(msg) = check(&rec) {
                return Ok(rejected(Reason::Invalid, msg));
            }
            staged.push((doc, rec, pending));
        }

        let seq = last_seq(&mut db).await? + 1;
        let mut tx = db.transaction().await?;
        write_change(&mut tx, env, ChangeStatus::Accepted, Some(seq), "").await?;
        toasty::create!(SpaceLogRow {
            seq,
            change_id: Some(change_id),
            accepted_at: now_ms(),
            purged_object_id: None,
            pin_json: None,
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
                // An erased payload means the authority purged the object
                // after accepting this Change; the purge entry follows in
                // the log.
                if part.bytes.is_empty() || is_purged_tx(&mut tx, object_id.uuid()).await? {
                    continue;
                }
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
        write_log(&mut tx, seq, Some(change_id), None).await?;
        write_state_tx(&mut tx, CURSOR, seq).await?;
        tx.commit().await?;
        self.emit_change(change_id, ChangeStatus::Accepted);
        self.emit_objects(&installed, ACTOR_REMOTE);
        Ok(())
    }

    /// Authority side: erases a deleted Object for good and records the
    /// purge in the Space log. Purging twice is a no-op.
    pub async fn purge_object(&self, id: ObjectId) -> Result<i64, StoreError> {
        let mut db = self.db.lock().await;
        if let Some(seq) = purged_seq(&mut db, id.uuid()).await? {
            return Ok(seq);
        }
        let Some(row) = ObjectRow::filter_by_object_id(id.uuid())
            .first()
            .exec(&mut *db)
            .await?
        else {
            return Err(StoreError::ObjectNotFound(id));
        };
        if row.deleted_at.is_none() {
            return Err(StoreError::NotDeleted(id));
        }
        let seq = last_seq(&mut db).await? + 1;
        let mut tx = db.transaction().await?;
        erase(&mut tx, id.uuid(), seq).await?;
        write_log(&mut tx, seq, None, Some(id.uuid())).await?;
        tx.commit().await?;
        self.release_blobs();
        self.emit_objects(&[(id.uuid(), row.kind, false)], ACTOR_USER);
        Ok(seq)
    }

    /// Replica side: installs a purge delivered from the authority at log
    /// position `seq`.
    pub async fn apply_purge(&self, seq: i64, id: ObjectId) -> Result<(), StoreError> {
        let mut db = self.db.lock().await;
        let kind = ObjectRow::filter_by_object_id(id.uuid())
            .first()
            .exec(&mut *db)
            .await?
            .map(|r| r.kind);
        let mut tx = db.transaction().await?;
        erase(&mut tx, id.uuid(), seq).await?;
        write_log(&mut tx, seq, None, Some(id.uuid())).await?;
        write_state_tx(&mut tx, CURSOR, seq).await?;
        tx.commit().await?;
        self.release_blobs();
        if let Some(kind) = kind {
            self.emit_objects(&[(id.uuid(), kind, false)], ACTOR_REMOTE);
        }
        Ok(())
    }

    /// Deletes the files of digests an erasure released.
    fn release_blobs(&self) {
        for sha in take_released() {
            if let Err(e) = self.blobs.remove(self.id, &sha) {
                tracing::warn!(error = %e, "could not remove released attachment bytes");
            }
        }
    }

    /// Whether an Object was purged.
    pub async fn is_purged(&self, id: ObjectId) -> Result<bool, StoreError> {
        let mut db = self.db.lock().await;
        is_purged(&mut db, id.uuid()).await
    }
}

async fn is_purged(db: &mut Db, id: Uuid) -> Result<bool, StoreError> {
    Ok(purged_seq(db, id).await?.is_some())
}

async fn purged_seq(db: &mut Db, id: Uuid) -> Result<Option<i64>, StoreError> {
    Ok(GraveyardRow::filter_by_object_id(id)
        .first()
        .exec(&mut *db)
        .await?
        .map(|g| g.seq))
}

async fn is_purged_tx(tx: &mut toasty::Transaction<'_>, id: Uuid) -> Result<bool, StoreError> {
    Ok(GraveyardRow::filter_by_object_id(id)
        .first()
        .exec(&mut *tx)
        .await?
        .is_some())
}

/// Removes everything managed for an Object except the record that it
/// existed: document, update log, projections, and the payload of every
/// Change part that touched it. The parts themselves stay, so the Changes'
/// other objects keep their history.
async fn erase(tx: &mut toasty::Transaction<'_>, id: Uuid, seq: i64) -> Result<(), StoreError> {
    DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(id))
        .delete()
        .exec(&mut *tx)
        .await?;
    DocRow::filter_by_object_id(id)
        .delete()
        .exec(&mut *tx)
        .await?;
    let released = attachment_digest_of(tx, id).await?;
    delete_projections(tx, id).await?;
    if let Some(sha) = released {
        if !digest_referenced(tx, &sha).await? {
            BlobRow::filter_by_sha256(sha.clone())
                .delete()
                .exec(&mut *tx)
                .await?;
            // The file goes after the transaction, from the caller.
            RELEASED.with(|r| r.borrow_mut().push(sha));
        }
    }
    toasty::update!(ChangePartRow::filter(ChangePartRow::fields().object_id().eq(id)) {
        bytes: Vec::new(),
    })
    .exec(&mut *tx)
    .await?;
    GraveyardRow::filter_by_object_id(id)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(GraveyardRow {
        object_id: id,
        purged_at: now_ms(),
        seq,
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
}

thread_local! {
    /// Digests whose files must go once the erasing transaction commits.
    static RELEASED: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Takes the digests released by erasures since the last call.
fn take_released() -> Vec<String> {
    RELEASED.with(|r| std::mem::take(&mut *r.borrow_mut()))
}

/// The digest an attachment Object names, from its projection.
async fn attachment_digest_of(
    tx: &mut toasty::Transaction<'_>,
    object: Uuid,
) -> Result<Option<String>, StoreError> {
    let row = FieldValueRow::filter(
        FieldValueRow::fields().object_id().eq(object).and(
            FieldValueRow::fields()
                .field_id()
                .eq(crate::model::reserved::SHA256.to_owned()),
        ),
    )
    .first()
    .exec(&mut *tx)
    .await?;
    Ok(row.and_then(|r| r.v_text))
}

/// Whether any attachment Object of the Space still names the digest.
async fn digest_referenced(
    tx: &mut toasty::Transaction<'_>,
    sha256: &str,
) -> Result<bool, StoreError> {
    let rows = FieldValueRow::filter(
        FieldValueRow::fields()
            .field_id()
            .eq(crate::model::reserved::SHA256.to_owned())
            .and(FieldValueRow::fields().v_text().eq(Some(sha256.to_owned()))),
    )
    .exec(&mut *tx)
    .await?;
    Ok(!rows.is_empty())
}

async fn write_log(
    tx: &mut toasty::Transaction<'_>,
    seq: i64,
    change_id: Option<Uuid>,
    purged_object_id: Option<Uuid>,
) -> Result<(), StoreError> {
    SpaceLogRow::filter_by_seq(seq)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(SpaceLogRow {
        seq,
        change_id,
        accepted_at: now_ms(),
        purged_object_id,
        pin_json: None,
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
}

async fn write_state_tx(
    tx: &mut toasty::Transaction<'_>,
    key: &str,
    value: i64,
) -> Result<(), StoreError> {
    SyncStateRow::filter_by_key(key.to_owned())
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(SyncStateRow {
        key: key.to_owned(),
        value,
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
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
        actor: row.actor.clone(),
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
    if is_purged_tx(tx, object).await? {
        return Ok(false);
    }
    let rows = toasty::sql::query(
        "SELECT p.bytes FROM change_parts p JOIN changes c ON c.id = p.change_id \
         WHERE p.object_id = ?1 AND c.status NOT IN (?2, ?3) AND length(p.bytes) > 0 \
         ORDER BY c.seq ASC",
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
            space_id: String::new(),
        })
        .collect();
    parts.sort_by(|a, b| a.object_id.cmp(&b.object_id));
    Ok(Envelope {
        id: ObjectId::from_uuid(row.id).to_string(),
        space_id: space.to_string(),
        device_id: row.device_id.clone(),
        created_at_ms: row.created_at,
        parts,
        actor: row.actor.clone(),
    })
}

/// Sync-state key: highest Space-log sequence installed locally.
const CURSOR: &str = "cursor";
/// Sync-state key: sequence the initial download must reach.
const INITIAL_TARGET: &str = "initial_target";
/// Sync-state key: the schema epoch, moved by every pin.
const SCHEMA_EPOCH: &str = "schema_epoch";
/// Sync-state key: retention of deleted Objects in days; absent = default.
const RETENTION_DAYS: &str = "retention_days";
/// Retention of deleted Objects when none was set.
pub const DEFAULT_RETENTION_DAYS: i64 = 90;

async fn write_pin(tx: &mut toasty::Transaction<'_>, pin: &Pin) -> Result<(), StoreError> {
    let definition = uuid_arg(&pin.definition_id)?;
    PinRow::filter_by_definition_id(definition)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(PinRow {
        definition_id: definition,
        publication_id: uuid_arg(&pin.publication_id)?,
        version: i64::from(pin.version),
        epoch: pin.epoch,
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
}

async fn write_log_pin(
    tx: &mut toasty::Transaction<'_>,
    seq: i64,
    pin: &Pin,
) -> Result<(), StoreError> {
    let json = serde_json::json!({
        "definition_id": pin.definition_id,
        "publication_id": pin.publication_id,
        "version": pin.version,
        "epoch": pin.epoch,
    })
    .to_string();
    SpaceLogRow::filter_by_seq(seq)
        .delete()
        .exec(&mut *tx)
        .await?;
    toasty::create!(SpaceLogRow {
        seq,
        change_id: None,
        accepted_at: now_ms(),
        purged_object_id: None,
        pin_json: Some(json),
    })
    .exec(&mut *tx)
    .await?;
    Ok(())
}

fn pin_from_json(s: &str) -> Result<Pin, StoreError> {
    let v: serde_json::Value = serde_json::from_str(s).map_err(|_| StoreError::Corrupt {
        table: "space_log",
        what: "pin",
    })?;
    let text = |k: &str| {
        v.get(k)
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    Ok(Pin {
        definition_id: text("definition_id"),
        publication_id: text("publication_id"),
        version: i32::try_from(
            v.get("version")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
        )
        .unwrap_or(0),
        epoch: v
            .get("epoch")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
    })
}

async fn read_state(db: &mut Db, key: &str) -> Result<i64, StoreError> {
    Ok(SyncStateRow::filter_by_key(key.to_owned())
        .first()
        .exec(&mut *db)
        .await?
        .map_or(0, |r| r.value))
}

async fn write_state(db: &mut Db, key: &str, value: i64) -> Result<(), StoreError> {
    SyncStateRow::filter_by_key(key.to_owned())
        .delete()
        .exec(&mut *db)
        .await?;
    toasty::create!(SyncStateRow {
        key: key.to_owned(),
        value,
    })
    .exec(&mut *db)
    .await?;
    Ok(())
}

async fn read_cursor(db: &mut Db) -> Result<i64, StoreError> {
    read_state(db, CURSOR).await
}

async fn pending_counts(db: &mut Db) -> Result<(usize, usize), StoreError> {
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

/// Has the engine write a compacted, consistent copy of the open database.
async fn vacuum_into(db: &mut Db, path: &Path) -> Result<(), StoreError> {
    let quoted = path.to_string_lossy().replace('\'', "''");
    toasty::sql::query(format!("VACUUM INTO '{quoted}'"))
        .exec(&mut *db)
        .await?;
    restrict_to_owner(path);
    Ok(())
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
    write_change_flagged(tx, env, status, accepted_seq, rejection, false).await
}

async fn write_change_flagged(
    tx: &mut toasty::Transaction<'_>,
    env: &Envelope,
    status: ChangeStatus,
    accepted_seq: Option<i64>,
    rejection: &str,
    coordinated: bool,
) -> Result<(), StoreError> {
    let change_id = uuid_arg(&env.id)?;
    toasty::create!(ChangeRow {
        id: change_id,
        device_id: env.device_id.clone(),
        created_at: env.created_at_ms,
        status: status_code(status),
        accepted_seq,
        rejection: (!rejection.is_empty()).then(|| rejection.to_owned()),
        coordinated,
        actor: env.actor.clone(),
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
            // Lists and maps are not projected; they are read from the document.
            Some(value::Value::List(_) | value::Value::Map(_)) | None => continue,
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
