//! The core API as JSON operations, for hosts that speak bytes rather than
//! Lua tables: WASM and native extensions. The same checks apply as for
//! scripts: reads and writes are bounded by a manifest relative to the
//! Space of the call, reads are recorded as labels, writes are attributed.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use elri_proto::elri::v1::events::{event, NoticeEvent};
use elri_proto::elri::v1::object::{value, Object, ObjectKind, Participant, Value, ValueList};
use elri_proto::elri::v1::script::Manifest;
use serde_json::{json, Value as Json};

use crate::model::{now_ms, ObjectDoc, ObjectId, SpaceId};
use crate::store::{ObjectFilter, Store};

/// What one invocation may touch and what it has touched.
pub struct Session {
    /// The store.
    pub store: Arc<Store>,
    /// The Space of the call.
    pub own: SpaceId,
    /// What the code declared.
    pub manifest: Manifest,
    /// Who the writes are attributed to.
    pub actor: String,
    /// Spaces read so far.
    pub labels: BTreeSet<String>,
    /// Lines logged.
    pub logs: Vec<String>,
    /// Host calls made.
    pub calls: u32,
    /// Calls allowed.
    pub call_limit: u32,
}

impl Session {
    fn space_of(&self, req: &Json) -> Result<SpaceId, String> {
        match req.get("space").and_then(Json::as_str) {
            None => Ok(self.own),
            Some(s) => ObjectId::parse(s).map_err(|e| e.to_string()),
        }
    }

    fn may_read(&mut self, space: SpaceId) -> Result<(), String> {
        if space != self.own && !self.manifest.reads.contains(&space.to_string()) {
            return Err(format!("capability denied: read space {space}"));
        }
        self.labels.insert(space.to_string());
        Ok(())
    }

    fn may_write(&self, space: SpaceId) -> Result<(), String> {
        if space != self.own && !self.manifest.writes.contains(&space.to_string()) {
            return Err(format!("capability denied: write space {space}"));
        }
        Ok(())
    }

    /// A hypothesis is revised only by the agent that proposed it.
    async fn may_revise(&self, space: SpaceId, id: ObjectId) -> Result<(), String> {
        let current = self
            .store
            .space(space)
            .await
            .map_err(|e| e.to_string())?
            .get(id)
            .await
            .map_err(|e| e.to_string())?;
        crate::hypothesis::may_revise(&current, &self.actor)
    }

    /// Every Space read so far must allow copying into `space`.
    async fn may_copy_to(&self, space: SpaceId) -> Result<(), String> {
        crate::policy::check_labels(
            &self.store,
            &self.labels,
            elri_proto::elri::v1::space::TransferOp::Copy,
            &space.to_string(),
        )
        .await
    }

    /// Runs one operation. The request is `{"op": ..., ...}`; the result
    /// is `{"ok": ...}` or `{"error": "..."}`.
    pub async fn call(&mut self, req: &Json) -> Json {
        self.calls += 1;
        if self.calls > self.call_limit {
            return json!({ "error": "limit exceeded: host calls" });
        }
        match self.dispatch(req).await {
            Ok(v) => json!({ "ok": v }),
            Err(e) => json!({ "error": e }),
        }
    }

    async fn dispatch(&mut self, req: &Json) -> Result<Json, String> {
        let op = req.get("op").and_then(Json::as_str).unwrap_or_default();
        let id = |req: &Json| -> Result<ObjectId, String> {
            req.get("id")
                .and_then(Json::as_str)
                .ok_or_else(|| "id is required".to_owned())
                .and_then(|s| ObjectId::parse(s).map_err(|e| e.to_string()))
        };
        match op {
            "log" => {
                let line = req
                    .get("text")
                    .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned))
                    .unwrap_or_default();
                self.logs.push(line);
                Ok(Json::Null)
            }
            "notify" => {
                let text = req
                    .get("text")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_owned();
                self.store.emit(event::Event::Notice(NoticeEvent {
                    space_id: self.own.to_string(),
                    source: self.actor.clone(),
                    text,
                }));
                Ok(Json::Null)
            }
            "get" => {
                let space = self.space_of(req)?;
                self.may_read(space)?;
                let o = self
                    .store
                    .space(space)
                    .await
                    .map_err(|e| e.to_string())?
                    .get(id(req)?)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(object_to_json(&o))
            }
            "list" => {
                let space = self.space_of(req)?;
                self.may_read(space)?;
                let filter = filter_from_json(req.get("filter").unwrap_or(&Json::Null))?;
                let objects = self
                    .store
                    .space(space)
                    .await
                    .map_err(|e| e.to_string())?
                    .list(&filter)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Json::Array(objects.iter().map(object_to_json).collect()))
            }
            "search" => {
                let space = self.space_of(req)?;
                self.may_read(space)?;
                let query = req.get("query").and_then(Json::as_str).unwrap_or_default();
                let hits = self
                    .store
                    .space(space)
                    .await
                    .map_err(|e| e.to_string())?
                    .search(query, None, 50)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Json::Array(
                    hits.iter()
                        .map(|(o, score, fields)| {
                            json!({ "object": object_to_json(o), "score": score, "fields": fields })
                        })
                        .collect(),
                ))
            }
            "create" | "update" | "delete" => self.write(op, req).await,
            other => Err(format!("unknown op {other}")),
        }
    }

    async fn write(&mut self, op: &str, req: &Json) -> Result<Json, String> {
        let id = |req: &Json| -> Result<ObjectId, String> {
            req.get("id")
                .and_then(Json::as_str)
                .ok_or_else(|| "id is required".to_owned())
                .and_then(|s| ObjectId::parse(s).map_err(|e| e.to_string()))
        };
        match op {
            "create" => {
                let space = self.space_of(req)?;
                self.may_write(space)?;
                self.may_copy_to(space).await?;
                let doc = doc_from_json(req, space, &self.actor)?;
                let rec = self
                    .store
                    .space(space)
                    .await
                    .map_err(|e| e.to_string())?
                    .insert_as(&doc, &self.actor)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(object_to_json(&rec))
            }
            "update" => {
                let space = self.space_of(req)?;
                self.may_write(space)?;
                self.may_copy_to(space).await?;
                self.may_revise(space, id(req)?).await?;
                let set = fields_from_json(req.get("fields"))?;
                let remove: Vec<String> = strings(req.get("remove_fields"));
                let add_types: Vec<String> = strings(req.get("add_types"));
                let remove_types: Vec<String> = strings(req.get("remove_types"));
                let rec = self
                    .store
                    .space(space)
                    .await
                    .map_err(|e| e.to_string())?
                    .update_as(
                        id(req)?,
                        move |doc| {
                            for (k, v) in &set {
                                doc.set_field(k, v)?;
                            }
                            for k in &remove {
                                doc.remove_field(k)?;
                            }
                            for t in &add_types {
                                doc.add_type(t)?;
                            }
                            for t in &remove_types {
                                doc.remove_type(t)?;
                            }
                            Ok(())
                        },
                        &self.actor,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(object_to_json(&rec))
            }
            "delete" => {
                let space = self.space_of(req)?;
                self.may_write(space)?;
                self.may_revise(space, id(req)?).await?;
                self.store
                    .space(space)
                    .await
                    .map_err(|e| e.to_string())?
                    .update_as(id(req)?, |d| d.mark_deleted(now_ms()), &self.actor)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Json::Bool(true))
            }
            other => Err(format!("not a write: {other}")),
        }
    }
}

fn strings(v: Option<&Json>) -> Vec<String> {
    v.and_then(Json::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Json::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// A field value as JSON: scalars as themselves, references as
/// `{"ref": id}`, instants as `{"time": ms}`, text as `{"text": s}`, rich
/// text as `{"text": s, "marks": [...]}`, lists as arrays, maps as objects.
pub fn value_to_json(v: &Value) -> Json {
    match &v.value {
        Some(value::Value::Bool(b)) => json!(b),
        Some(value::Value::Int(i)) => json!(i),
        Some(value::Value::Double(d)) => json!(d),
        Some(value::Value::String(s)) => json!(s),
        Some(value::Value::TimeMs(t)) => json!({ "time": t }),
        Some(value::Value::Ref(r)) => json!({ "ref": r }),
        Some(value::Value::Text(t)) => json!({ "text": t }),
        Some(value::Value::Rich(r)) => json!({
            "text": r.text,
            "marks": r.marks.iter().map(|m| json!({
                "start": m.start, "end": m.end, "key": m.key,
                "value": serde_json::from_str::<Json>(&m.value_json).unwrap_or(Json::Bool(true)),
            })).collect::<Vec<_>>(),
        }),
        Some(value::Value::List(l)) => Json::Array(l.items.iter().map(value_to_json).collect()),
        Some(value::Value::Map(m)) => Json::Object(
            m.entries
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
        None => Json::Null,
    }
}

/// The inverse of [`value_to_json`]; `None` for null.
pub fn value_from_json(v: &Json) -> Result<Option<Value>, String> {
    let inner = match v {
        Json::Null => return Ok(None),
        Json::Bool(b) => value::Value::Bool(*b),
        Json::Number(n) => match n.as_i64() {
            Some(i) => value::Value::Int(i),
            None => value::Value::Double(n.as_f64().unwrap_or(0.0)),
        },
        Json::String(s) => value::Value::String(s.clone()),
        Json::Array(items) => value::Value::List(ValueList {
            items: items
                .iter()
                .map(value_from_json)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect(),
        }),
        Json::Object(o) => {
            if let Some(r) = o.get("ref").and_then(Json::as_str) {
                value::Value::Ref(r.to_owned())
            } else if let Some(t) = o.get("time").and_then(Json::as_i64) {
                value::Value::TimeMs(t)
            } else if let Some(t) = o.get("text").and_then(Json::as_str) {
                match o.get("marks").and_then(Json::as_array) {
                    Some(marks) => value::Value::Rich(elri_proto::elri::v1::object::RichText {
                        text: t.to_owned(),
                        marks: marks
                            .iter()
                            .map(|m| elri_proto::elri::v1::object::Mark {
                                start: i32::try_from(
                                    m.get("start").and_then(Json::as_i64).unwrap_or(0),
                                )
                                .unwrap_or(0),
                                end: i32::try_from(
                                    m.get("end").and_then(Json::as_i64).unwrap_or(0),
                                )
                                .unwrap_or(0),
                                key: m
                                    .get("key")
                                    .and_then(Json::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                                value_json: m.get("value").map_or_else(
                                    || "true".to_owned(),
                                    std::string::ToString::to_string,
                                ),
                            })
                            .collect(),
                    }),
                    None => value::Value::Text(t.to_owned()),
                }
            } else {
                let mut entries = HashMap::new();
                for (k, v) in o {
                    if let Some(v) = value_from_json(v)? {
                        entries.insert(k.clone(), v);
                    }
                }
                value::Value::Map(elri_proto::elri::v1::object::ValueMap { entries })
            }
        }
    };
    Ok(Some(Value { value: Some(inner) }))
}

/// An Object as JSON, the shape scripts and extensions see.
pub fn object_to_json(o: &Object) -> Json {
    let mut out = json!({
        "id": o.id,
        "space": o.space_id,
        "kind": o.kind().as_str_name().to_lowercase().trim_start_matches("object_kind_"),
        "types": o.types,
        "fields": o.fields.iter().map(|(k, v)| (k.clone(), value_to_json(v))).collect::<serde_json::Map<_, _>>(),
        "created_at": o.created_at_ms,
        "updated_at": o.updated_at_ms,
    });
    if let Some(d) = o.deleted_at_ms {
        out["deleted_at"] = json!(d);
    }
    if let Some(t) = &o.target_id {
        out["target"] = json!(t);
    }
    if !o.participants.is_empty() {
        out["participants"] = Json::Object(
            o.participants
                .iter()
                .map(|p| {
                    (
                        p.slot.clone(),
                        json!({ "object": p.object_id, "role": p.role_id }),
                    )
                })
                .collect(),
        );
    }
    if !o.placements.is_empty() {
        out["placements"] = Json::Array(
            o.placements
                .iter()
                .map(|p| json!({ "id": p.id, "parent": p.parent_id, "index": p.index, "block": p.block_id }))
                .collect(),
        );
    }
    out
}

fn kind_from_json(s: Option<&str>) -> Result<ObjectKind, String> {
    Ok(match s.map(str::to_ascii_lowercase).as_deref() {
        None | Some("entity") => ObjectKind::Entity,
        Some("relation") => ObjectKind::Relation,
        Some("layer") => ObjectKind::Layer,
        Some(other) => return Err(format!("kind {other} cannot be created here")),
    })
}

/// A list filter from `{"kind", "type", "limit", "include_deleted",
/// "participant", "target", "placed_block"}`.
pub fn filter_from_json(f: &Json) -> Result<ObjectFilter, String> {
    let kind = match f.get("kind").and_then(Json::as_str) {
        None => None,
        Some(k) => Some(match k.to_ascii_lowercase().as_str() {
            "entity" => ObjectKind::Entity,
            "relation" => ObjectKind::Relation,
            "layer" => ObjectKind::Layer,
            "attachment" => ObjectKind::Attachment,
            "script" => ObjectKind::Script,
            "rule" => ObjectKind::Rule,
            "interface" => ObjectKind::Interface,
            other => return Err(format!("unknown kind {other}")),
        }),
    };
    let parse = |key: &str| -> Result<Option<ObjectId>, String> {
        match f.get(key).and_then(Json::as_str) {
            None => Ok(None),
            Some(s) => ObjectId::parse(s).map(Some).map_err(|e| e.to_string()),
        }
    };
    Ok(ObjectFilter {
        kind,
        type_id: f.get("type").and_then(Json::as_str).map(str::to_owned),
        include_deleted: f
            .get("include_deleted")
            .and_then(Json::as_bool)
            .unwrap_or(false),
        limit: usize::try_from(f.get("limit").and_then(Json::as_i64).unwrap_or(100)).unwrap_or(100),
        participant: parse("participant")?,
        target: parse("target")?,
        placed_block: parse("placed_block")?,
    })
}

fn fields_from_json(v: Option<&Json>) -> Result<HashMap<String, Value>, String> {
    let mut out = HashMap::new();
    if let Some(Json::Object(o)) = v {
        for (k, v) in o {
            if crate::model::is_reserved_field(k) {
                return Err(format!("field {k}: reserved ids are written by the core"));
            }
            if let Some(v) = value_from_json(v)? {
                out.insert(k.clone(), v);
            }
        }
    }
    Ok(out)
}

/// A proposal from `{"model", "sources", "basis"}`.
pub fn proposal_from_json(v: &Json) -> Result<crate::hypothesis::Proposal, String> {
    let p = crate::hypothesis::Proposal {
        model: v
            .get("model")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned(),
        sources: strings(v.get("sources")),
        basis: v
            .get("basis")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned(),
    };
    crate::hypothesis::check(&p)?;
    Ok(p)
}

/// A new document from `{"kind", "types", "fields", "target", "participants",
/// "hypothesis"}`; a hypothesis is a Layer proposed by `actor`.
pub fn doc_from_json(spec: &Json, space: SpaceId, actor: &str) -> Result<ObjectDoc, String> {
    let kind = kind_from_json(spec.get("kind").and_then(Json::as_str))?;
    let proposal = spec.get("hypothesis").map(proposal_from_json).transpose()?;
    if proposal.is_some() && kind != ObjectKind::Layer {
        return Err("a hypothesis is a layer".to_owned());
    }
    let now = now_ms();
    let doc = match (kind, spec.get("target").and_then(Json::as_str)) {
        (ObjectKind::Layer, Some(target)) => ObjectDoc::create_layer(
            ObjectId::new(),
            space,
            ObjectId::parse(target).map_err(|e| e.to_string())?,
            now,
        ),
        (ObjectKind::Layer, None) => return Err("a layer needs a target".to_owned()),
        (kind, _) => ObjectDoc::create(ObjectId::new(), space, kind, now),
    }
    .map_err(|e| e.to_string())?;
    for t in strings(spec.get("types")) {
        doc.add_type(&t).map_err(|e| e.to_string())?;
    }
    for (k, v) in fields_from_json(spec.get("fields"))? {
        doc.set_field(&k, &v).map_err(|e| e.to_string())?;
    }
    if let Some(Json::Object(ps)) = spec.get("participants") {
        for (slot, p) in ps {
            doc.set_participant(&Participant {
                slot: slot.clone(),
                object_id: p
                    .get("object")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                role_id: p
                    .get("role")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            })
            .map_err(|e| e.to_string())?;
        }
    }
    if let Some(p) = &proposal {
        crate::hypothesis::write(&doc, actor, p).map_err(|e| e.to_string())?;
    }
    doc.touch(now).map_err(|e| e.to_string())?;
    Ok(doc)
}
