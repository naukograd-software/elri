//! The commit coordinator: one Change across several documents, possibly
//! across Spaces, that lands whole or not at all.
//!
//! Every Space file has its own transactions and nothing spans them, so
//! the coordinator is a protocol on top: stage every part without touching
//! live state, write the whole Change to the journal in the device
//! database, install the parts Space by Space, then mark the journal
//! entry applied. A crash between the journal write and the mark is
//! finished at the next open; readers wait at the visibility gate while
//! parts are being installed. A Change that stays inside one Space skips
//! the journal: one transaction is already whole.

use std::collections::BTreeMap;
use std::sync::Arc;

use elri_proto::elri::v1::change::ChangeStatus;
use elri_proto::elri::v1::object::Object;
use elri_proto::elri::v1::sync::{Envelope, Part};
use loro::VersionVector;
use prost::Message as _;
use toasty::Db;
use tokio::sync::MutexGuard;
use uuid::Uuid;

use super::{
    dependencies, is_purged, load_doc, now_ms, status_code, uuid_arg, write_change_flagged,
    write_deps, write_log, write_projections, write_snapshot, ChangeRow, DocUpdateRow, JournalRow,
    ModelError, ObjectDoc, ObjectId, Outcome, Reason, SpaceId, SpaceStore, Store, StoreError,
    COMPACT_AFTER_UPDATES,
};
use crate::change::{accepted, rejected};
use crate::sync::{envelope_for, envelope_spaces};

/// Journal entry states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JournalState {
    /// Decided and durable; parts may still be missing from Spaces.
    Committed = 1,
    /// Every part is installed.
    Applied = 2,
}

/// An edit of an existing document, applied on a candidate.
pub type EditFn = Box<dyn FnOnce(&ObjectDoc) -> Result<(), ModelError> + Send>;

/// One write of a coordinated Change.
pub enum Edit {
    /// A new document.
    Create(ObjectDoc),
    /// An edit of an existing document.
    Update(ObjectId, EditFn),
}

/// A coordinated Change as the journal knows it.
#[derive(Debug, Clone)]
pub struct Journaled {
    /// The whole envelope, every Space's parts included.
    pub envelope: Envelope,
    /// Status of the whole Change.
    pub status: ChangeStatus,
}

/// A part staged on a candidate document, not yet installed.
struct StagedDoc {
    doc: ObjectDoc,
    rec: Object,
    pending: usize,
    is_new: bool,
    part: Part,
    deps: Vec<Uuid>,
}

impl Store {
    /// Applies `edits` as one Change. Edits of one Space go in one
    /// transaction; edits across Spaces go through the journal and the
    /// visibility gate. Returns the Change id and the written Objects in
    /// edit order.
    pub async fn commit(
        &self,
        edits: Vec<(SpaceId, Edit)>,
    ) -> Result<(String, Vec<Object>), StoreError> {
        self.commit_as(edits, super::ACTOR_USER).await
    }

    /// [`Self::commit`] attributed to `actor`.
    pub async fn commit_as(
        &self,
        edits: Vec<(SpaceId, Edit)>,
        actor: &str,
    ) -> Result<(String, Vec<Object>), StoreError> {
        if edits.is_empty() {
            return Err(StoreError::Model(ModelError::Malformed("no writes")));
        }
        // Spaces in id order: the lock order every coordinated writer uses.
        let mut by_space: BTreeMap<SpaceId, Vec<usize>> = BTreeMap::new();
        for (i, (space, _)) in edits.iter().enumerate() {
            by_space.entry(*space).or_default().push(i);
        }
        let mut stores: Vec<(SpaceId, Arc<SpaceStore>)> = Vec::with_capacity(by_space.len());
        for space in by_space.keys() {
            stores.push((*space, self.space(*space).await?));
        }
        let multi = stores.len() > 1;
        let env_id = ObjectId::new();
        let primary = stores[0].0;

        let _visible = if multi {
            Some(self.gate.write().await)
        } else {
            None
        };
        let mut guards: Vec<MutexGuard<'_, Db>> = Vec::with_capacity(stores.len());
        for (_, s) in &stores {
            guards.push(s.db.lock().await);
        }

        // Stage everything first; nothing live changes until the decision.
        let mut staged: Vec<Vec<StagedDoc>> = Vec::with_capacity(stores.len());
        let mut edits: Vec<Option<Edit>> = edits.into_iter().map(|(_, e)| Some(e)).collect();
        let mut written: Vec<Option<Object>> = vec![None; edits.len()];
        for ((space, _), guard) in stores.iter().zip(guards.iter_mut()) {
            SpaceStore::writable(guard).await?;
            let mut docs = Vec::new();
            for &i in &by_space[space] {
                let Some(edit) = edits[i].take() else {
                    continue;
                };
                let d = stage(guard, *space, primary, edit).await?;
                written[i] = Some(d.rec.clone());
                docs.push(d);
            }
            let recs: Vec<Object> = docs.iter().map(|d| d.rec.clone()).collect();
            super::check_strict(guard, &recs).await?;
            staged.push(docs);
        }
        let env = Envelope {
            id: env_id.to_string(),
            space_id: primary.to_string(),
            device_id: self.device_id.as_string(),
            created_at_ms: now_ms(),
            parts: staged
                .iter()
                .flat_map(|docs| docs.iter().map(|d| d.part.clone()))
                .collect(),
            actor: actor.to_owned(),
        };

        if multi {
            self.journal_write(&env, JournalState::Committed, ChangeStatus::Local)
                .await?;
        }
        for (((space, _), guard), docs) in stores.iter().zip(guards.iter_mut()).zip(&staged) {
            install(guard, &envelope_for(&env, *space), docs, multi).await?;
        }
        if multi {
            self.journal_mark(env_id.uuid(), JournalState::Applied)
                .await?;
        }
        drop(guards);
        for ((_, s), docs) in stores.iter().zip(&staged) {
            s.dirty.notify_one();
            s.emit_change(env_id.uuid(), ChangeStatus::Local);
            for d in docs {
                s.emit_object(&d.rec, actor);
            }
        }
        if multi {
            self.dirty.notify_one();
        }
        Ok((env_id.to_string(), written.into_iter().flatten().collect()))
    }

    /// Finishes journal entries left committed but not applied by an
    /// interrupted run: every part is installed again, idempotently.
    pub(super) async fn recover(&self) -> Result<(), StoreError> {
        let rows = {
            let mut db = self.device.lock().await;
            JournalRow::all().exec(&mut *db).await?
        };
        for row in rows {
            if row.state != JournalState::Committed as i64 {
                continue;
            }
            let env =
                Envelope::decode(row.envelope.as_slice()).map_err(|_| StoreError::Corrupt {
                    table: "journal",
                    what: "envelope",
                })?;
            let status = super::status_of(row.status)?;
            tracing::warn!(change = %env.id, "finishing an interrupted coordinated change");
            for space in envelope_spaces(&env)? {
                let Ok(store) = self.space(space).await else {
                    // A Space this device no longer has; nothing to finish.
                    continue;
                };
                let mut db = store.db.lock().await;
                if ChangeRow::filter(ChangeRow::fields().id().eq(row.change_id))
                    .first()
                    .exec(&mut *db)
                    .await?
                    .is_some()
                {
                    continue;
                }
                let sub = envelope_for(&env, space);
                let docs = restage_from_bytes(&mut db, &sub).await?;
                install_with_status(&mut db, &sub, &docs, true, status, None).await?;
            }
            self.journal_mark(row.change_id, JournalState::Applied)
                .await?;
        }
        Ok(())
    }

    /// Coordinated Changes not yet submitted, oldest first.
    pub async fn coordinator_outbox(&self) -> Result<Vec<Envelope>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = JournalRow::filter(
            JournalRow::fields()
                .status()
                .eq(status_code(ChangeStatus::Local))
                .and(
                    JournalRow::fields()
                        .state()
                        .eq(JournalState::Applied as i64),
                ),
        )
        .order_by(JournalRow::fields().created_at().asc())
        .exec(&mut *db)
        .await?;
        rows.iter()
            .map(|r| {
                Envelope::decode(r.envelope.as_slice()).map_err(|_| StoreError::Corrupt {
                    table: "journal",
                    what: "envelope",
                })
            })
            .collect()
    }

    /// A coordinated Change by id, if it is one.
    pub async fn journaled(&self, id: &str) -> Result<Option<Journaled>, StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        let Some(row) = JournalRow::filter_by_change_id(id)
            .first()
            .exec(&mut *db)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Journaled {
            envelope: Envelope::decode(row.envelope.as_slice()).map_err(|_| {
                StoreError::Corrupt {
                    table: "journal",
                    what: "envelope",
                }
            })?,
            status: super::status_of(row.status)?,
        }))
    }

    /// Records the authority's answer for a coordinated Change in every
    /// Space it touched and in the journal.
    pub async fn record_coordinated_outcome(
        &self,
        env: &Envelope,
        outcome: &Outcome,
    ) -> Result<(), StoreError> {
        for space in envelope_spaces(env)? {
            if let Ok(store) = self.space(space).await {
                store.record_outcome(&env.id, outcome).await?;
            }
        }
        let status = match outcome {
            Outcome::AcceptedSeq(_) => ChangeStatus::Accepted,
            Outcome::Rejected(_) => ChangeStatus::Rejected,
        };
        self.journal_status(uuid_arg(&env.id)?, status).await
    }

    /// Retries a rejected Change: in its Space, and, if it was
    /// coordinated, in every Space it touched and in the journal, so the
    /// coordinator submits it again.
    pub async fn retry_change(
        &self,
        space: SpaceId,
        id: &str,
    ) -> Result<crate::change::Change, StoreError> {
        let Some(j) = self.journaled(id).await? else {
            return self.space(space).await?.retry_change(id).await;
        };
        let mut first = None;
        for s in envelope_spaces(&j.envelope)? {
            let change = self.space(s).await?.retry_change(id).await?;
            if s == space {
                first = Some(change);
            }
        }
        self.journal_status(uuid_arg(id)?, ChangeStatus::Local)
            .await?;
        self.dirty.notify_one();
        first.ok_or(StoreError::ChangeNotFound(id.to_owned()))
    }

    /// Discards a rejected or paused Change everywhere it was recorded.
    pub async fn discard_change(
        &self,
        space: SpaceId,
        id: &str,
    ) -> Result<Vec<String>, StoreError> {
        let Some(j) = self.journaled(id).await? else {
            return self.space(space).await?.discard_change(id).await;
        };
        let mut dropped = Vec::new();
        for s in envelope_spaces(&j.envelope)? {
            dropped.extend(self.space(s).await?.discard_change(id).await?);
        }
        dropped.sort();
        dropped.dedup();
        let mut db = self.device.lock().await;
        JournalRow::filter_by_change_id(uuid_arg(id)?)
            .delete()
            .exec(&mut *db)
            .await?;
        Ok(dropped)
    }

    /// Authority side: admits a Change spanning Spaces as a whole. Every
    /// part is staged first; one failing part rejects the Change and
    /// nothing is written. Returns the sequence assigned in each Space.
    pub async fn admit_coordinated(
        &self,
        env: &Envelope,
        check: &(dyn Fn(&Object) -> Result<(), String> + Sync),
    ) -> Result<Result<Vec<(SpaceId, i64)>, Outcome>, StoreError> {
        let spaces = envelope_spaces(env)?;
        let change_id = uuid_arg(&env.id)?;
        let mut stores = Vec::with_capacity(spaces.len());
        for s in &spaces {
            stores.push(self.space(*s).await?);
        }
        let _visible = self.gate.write().await;
        let mut guards: Vec<MutexGuard<'_, Db>> = Vec::with_capacity(stores.len());
        for s in &stores {
            guards.push(s.db.lock().await);
        }
        // Replay of a known Change returns the recorded outcome.
        if let Some(existing) = ChangeRow::filter(ChangeRow::fields().id().eq(change_id))
            .first()
            .exec(&mut *guards[0])
            .await?
        {
            return Ok(Err(match (existing.accepted_seq, existing.rejection) {
                (Some(seq), _) => accepted(seq),
                (None, Some(r)) => rejected(Reason::Invalid, r),
                (None, None) => rejected(Reason::Invalid, "change known but undecided"),
            }));
        }
        let mut staged = Vec::with_capacity(spaces.len());
        for (space, guard) in spaces.iter().zip(guards.iter_mut()) {
            let sub = envelope_for(env, *space);
            if sub.parts.is_empty() {
                return Ok(Err(rejected(Reason::Invalid, "a space without parts")));
            }
            match stage_admit(guard, &sub).await? {
                Ok(docs) => {
                    for d in &docs {
                        if let Err(msg) = check(&d.rec) {
                            return Ok(Err(rejected(Reason::Invalid, msg)));
                        }
                    }
                    let recs: Vec<Object> = docs.iter().map(|d| d.rec.clone()).collect();
                    if let Some(outcome) = super::strict_rejection(guard, &recs).await? {
                        return Ok(Err(outcome));
                    }
                    staged.push(docs);
                }
                Err(outcome) => return Ok(Err(outcome)),
            }
        }
        let mut seqs = Vec::with_capacity(spaces.len());
        for guard in &mut guards {
            seqs.push(super::last_seq(guard).await? + 1);
        }
        self.journal_write(env, JournalState::Committed, ChangeStatus::Accepted)
            .await?;
        for (((space, guard), docs), seq) in
            spaces.iter().zip(guards.iter_mut()).zip(&staged).zip(&seqs)
        {
            let sub = envelope_for(env, *space);
            install_with_status(guard, &sub, docs, true, ChangeStatus::Accepted, Some(*seq))
                .await?;
        }
        self.journal_mark(change_id, JournalState::Applied).await?;
        drop(guards);
        for (s, docs) in stores.iter().zip(&staged) {
            s.emit_change(change_id, ChangeStatus::Accepted);
            for d in docs {
                s.emit_object(&d.rec, super::ACTOR_REMOTE);
            }
        }
        Ok(Ok(spaces.into_iter().zip(seqs).collect()))
    }

    async fn journal_write(
        &self,
        env: &Envelope,
        state: JournalState,
        status: ChangeStatus,
    ) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        toasty::create!(JournalRow {
            change_id: uuid_arg(&env.id)?,
            envelope: env.encode_to_vec(),
            state: state as i64,
            status: status_code(status),
            created_at: now_ms(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    async fn journal_mark(&self, id: Uuid, state: JournalState) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        toasty::update!(JournalRow::filter_by_change_id(id) {
            state: state as i64,
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    async fn journal_status(&self, id: Uuid, status: ChangeStatus) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        toasty::update!(JournalRow::filter_by_change_id(id) {
            status: status_code(status),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }
}

/// Applies one edit to a candidate document and describes the part.
async fn stage(
    db: &mut Db,
    space: SpaceId,
    primary: SpaceId,
    edit: Edit,
) -> Result<StagedDoc, StoreError> {
    let space_id = if space == primary {
        String::new()
    } else {
        space.to_string()
    };
    match edit {
        Edit::Create(doc) => {
            if is_purged(db, doc.id().uuid()).await? {
                return Err(StoreError::Purged(doc.id()));
            }
            let rec = doc.read()?;
            let empty = VersionVector::default();
            let part = Part {
                object_id: rec.id.clone(),
                kind: rec.kind,
                from_version: empty.encode().into(),
                to_version: doc.version().into(),
                bytes: doc.updates_since(&empty)?.into(),
                space_id,
            };
            let deps = dependencies(db, &rec).await?;
            Ok(StagedDoc {
                doc,
                rec,
                pending: 0,
                is_new: true,
                part,
                deps,
            })
        }
        Edit::Update(id, f) => {
            let (doc, pending) = load_doc(db, id).await?;
            let before = doc.version_vector();
            f(&doc)?;
            doc.touch(now_ms())?;
            let rec = doc.read()?;
            let part = Part {
                object_id: id.to_string(),
                kind: rec.kind,
                from_version: before.encode().into(),
                to_version: doc.version().into(),
                bytes: doc.updates_since(&before)?.into(),
                space_id,
            };
            let deps = dependencies(db, &rec).await?;
            Ok(StagedDoc {
                doc,
                rec,
                pending,
                is_new: false,
                part,
                deps,
            })
        }
    }
}

/// Rebuilds staged documents from an envelope's bytes: for recovery and
/// for admission, where the candidate is the stored document plus the
/// submitted operations.
async fn restage_from_bytes(db: &mut Db, env: &Envelope) -> Result<Vec<StagedDoc>, StoreError> {
    let mut out = Vec::with_capacity(env.parts.len());
    for part in &env.parts {
        let object_id = ObjectId::parse(&part.object_id)?;
        let (doc, pending, is_new) = match load_doc(db, object_id).await {
            Ok((doc, pending)) => {
                if !doc.import(&part.bytes)? {
                    return Err(StoreError::Model(ModelError::Malformed(
                        "coordinated change with missing predecessors",
                    )));
                }
                (doc, pending, false)
            }
            Err(StoreError::ObjectNotFound(_)) => (
                ObjectDoc::from_updates(object_id, std::slice::from_ref(&part.bytes.to_vec()))?,
                0,
                true,
            ),
            Err(e) => return Err(e),
        };
        let rec = doc.read()?;
        out.push(StagedDoc {
            doc,
            rec,
            pending,
            is_new,
            part: part.clone(),
            deps: Vec::new(),
        });
    }
    Ok(out)
}

/// Admission staging: like [`restage_from_bytes`] but every failure is a
/// rejection, not an error.
async fn stage_admit(
    db: &mut Db,
    env: &Envelope,
) -> Result<Result<Vec<StagedDoc>, Outcome>, StoreError> {
    for part in &env.parts {
        let Ok(object_id) = ObjectId::parse(&part.object_id) else {
            return Ok(Err(rejected(Reason::Invalid, "object id")));
        };
        if is_purged(db, object_id.uuid()).await? {
            return Ok(Err(rejected(
                Reason::Purged,
                format!("object {object_id} was purged"),
            )));
        }
    }
    let docs = match restage_from_bytes(db, env).await {
        Ok(docs) => docs,
        Err(StoreError::Model(ModelError::Malformed(_))) => {
            return Ok(Err(rejected(
                Reason::MissingPredecessors,
                "operations build on history the server does not have",
            )))
        }
        Err(StoreError::Model(e)) => return Ok(Err(rejected(Reason::Invalid, e.to_string()))),
        Err(e) => return Err(e),
    };
    for (d, part) in docs.iter().zip(&env.parts) {
        if d.rec.kind != part.kind {
            return Ok(Err(rejected(Reason::Invalid, "kind mismatch")));
        }
        // The document's own Space claim must agree with where the part is
        // routed: authorization was checked against the route.
        let route = crate::sync::part_space(env, part)
            .ok()
            .map(|s| s.to_string());
        if route.as_deref() != Some(d.rec.space_id.as_str()) {
            return Ok(Err(rejected(Reason::Invalid, "space mismatch")));
        }
    }
    Ok(Ok(docs))
}

/// Installs staged parts of one Space as a local Change.
async fn install(
    db: &mut Db,
    env: &Envelope,
    docs: &[StagedDoc],
    coordinated: bool,
) -> Result<(), StoreError> {
    install_with_status(db, env, docs, coordinated, ChangeStatus::Local, None).await
}

async fn install_with_status(
    db: &mut Db,
    env: &Envelope,
    docs: &[StagedDoc],
    coordinated: bool,
    status: ChangeStatus,
    accepted_seq: Option<i64>,
) -> Result<(), StoreError> {
    let change_id = uuid_arg(&env.id)?;
    let mut tx = db.transaction().await?;
    write_change_flagged(&mut tx, env, status, accepted_seq, "", coordinated).await?;
    for d in docs {
        write_deps(&mut tx, change_id, &d.deps).await?;
        if d.is_new {
            write_snapshot(&mut tx, &d.doc, &d.rec).await?;
        } else {
            toasty::create!(DocUpdateRow {
                object_id: uuid_arg(&d.rec.id)?,
                change_id,
                bytes: d.part.bytes.to_vec(),
            })
            .exec(&mut tx)
            .await?;
            if d.pending + 1 >= COMPACT_AFTER_UPDATES {
                write_snapshot(&mut tx, &d.doc, &d.rec).await?;
                DocUpdateRow::filter(DocUpdateRow::fields().object_id().eq(uuid_arg(&d.rec.id)?))
                    .delete()
                    .exec(&mut tx)
                    .await?;
            }
        }
        write_projections(&mut tx, &d.rec).await?;
    }
    if let Some(seq) = accepted_seq {
        write_log(&mut tx, seq, Some(change_id), None).await?;
    }
    tx.commit().await?;
    Ok(())
}
