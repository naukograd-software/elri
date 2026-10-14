//! `ContextService`: Contexts are Objects holding a selection; resolving
//! one reads the Spaces it names through the ordinary filters.

use std::collections::BTreeSet;
use std::sync::Arc;

use elri_proto::elri::v1::context::context_service_ws::{
    register_context_service_arc, ContextServiceHandler,
};
use elri_proto::elri::v1::context::{
    Context, CreateContextRequest, CreateContextResponse, HideInContextRequest,
    HideInContextResponse, ListContextsRequest, ListContextsResponse, ResolveContextRequest,
    ResolveContextResponse, Selection, UpdateContextRequest, UpdateContextResponse,
};
use elri_proto::elri::v1::object::{Object, ObjectKind};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::model::{now_ms, reserved, string_field, string_value, ObjectDoc, ObjectId, SpaceId};
use crate::store::{ObjectFilter, StoreError};

/// Reserved field: the selection as JSON.
pub const SELECTION: &str = "$selection";

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_context_service_arc(server, services);
}

fn selection_json(s: &Selection) -> String {
    serde_json::json!({
        "spaces": s.space_ids,
        "kinds": s.kinds,
        "types": s.type_ids,
        "layers": s.layer_ids,
        "views": s.view_ids,
        "hidden": s.hidden_ids,
    })
    .to_string()
}

fn selection_of(s: &str) -> Selection {
    let v: serde_json::Value = serde_json::from_str(s).unwrap_or_default();
    let strings = |key: &str| -> Vec<String> {
        v.get(key)
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    };
    Selection {
        space_ids: strings("spaces"),
        kinds: v
            .get("kinds")
            .and_then(serde_json::Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(serde_json::Value::as_i64)
                    .filter_map(|k| i32::try_from(k).ok())
                    .collect()
            })
            .unwrap_or_default(),
        type_ids: strings("types"),
        layer_ids: strings("layers"),
        view_ids: strings("views"),
        hidden_ids: strings("hidden"),
    }
}

fn context_of(o: &Object) -> Context {
    Context {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        selection: Some(selection_of(string_field(o, SELECTION).unwrap_or("{}"))),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

/// Bounds on a selection, so resolving one is a handful of queries.
const MAX_SPACES: usize = 32;
const MAX_TYPES: usize = 32;
const MAX_HIDDEN: usize = 10_000;

fn check_selection(s: &Selection) -> Result<(), Status> {
    if s.space_ids.len() > MAX_SPACES {
        return Err(invalid(format!("selection: at most {MAX_SPACES} spaces")));
    }
    if s.type_ids.len() > MAX_TYPES {
        return Err(invalid(format!("selection: at most {MAX_TYPES} types")));
    }
    if s.hidden_ids.len() > MAX_HIDDEN {
        return Err(invalid(format!(
            "selection: at most {MAX_HIDDEN} hidden objects"
        )));
    }
    for id in s
        .space_ids
        .iter()
        .chain(&s.layer_ids)
        .chain(&s.view_ids)
        .chain(&s.hidden_ids)
    {
        parse_id(id, "selection")?;
    }
    for k in &s.kinds {
        ObjectKind::try_from(*k)
            .ok()
            .filter(|k| *k != ObjectKind::Unspecified)
            .ok_or_else(|| invalid("selection: unknown kind"))?;
    }
    Ok(())
}

impl Services {
    async fn context(&self, space_id: SpaceId, id: ObjectId) -> Result<Context, Status> {
        let o = self.store.space(space_id).await?.get(id).await?;
        if o.kind() != ObjectKind::Context {
            return Err(Status::new(Code::NotFound, "not a context"));
        }
        Ok(context_of(&o))
    }
}

impl ContextServiceHandler for Services {
    async fn create_context(
        &self,
        req: Request<CreateContextRequest>,
    ) -> Result<Response<CreateContextResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let name = m.name.trim();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        let selection = m.selection.clone().unwrap_or_default();
        check_selection(&selection)?;
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Context, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        doc.set_field(SELECTION, &string_value(selection_json(&selection)))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space_id).await?.insert(&doc).await?;
        Ok(Response::new(CreateContextResponse {
            context: Some(context_of(&rec)),
        }))
    }

    async fn update_context(
        &self,
        req: Request<UpdateContextRequest>,
    ) -> Result<Response<UpdateContextResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        self.context(space_id, id).await?;
        let name = m.name.as_deref().map(str::trim).map(str::to_owned);
        if name.as_deref() == Some("") {
            return Err(invalid("name must not be empty"));
        }
        if let Some(s) = &m.selection {
            check_selection(s)?;
        }
        let selection = m.selection.clone();
        let rec = self
            .store
            .space(space_id)
            .await?
            .update(id, move |doc| {
                if let Some(n) = name {
                    doc.set_field(reserved::NAME, &string_value(n))?;
                }
                if let Some(s) = selection {
                    doc.set_field(SELECTION, &string_value(selection_json(&s)))?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdateContextResponse {
            context: Some(context_of(&rec)),
        }))
    }

    async fn list_contexts(
        &self,
        req: Request<ListContextsRequest>,
    ) -> Result<Response<ListContextsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let objects = self
            .store
            .space(space_id)
            .await?
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Context),
                include_deleted: req.msg.include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        Ok(Response::new(ListContextsResponse {
            contexts: objects.iter().map(context_of).collect(),
        }))
    }

    async fn resolve_context(
        &self,
        req: Request<ResolveContextRequest>,
    ) -> Result<Response<ResolveContextResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let ctx = self.context(space_id, id).await?;
        let sel = ctx.selection.unwrap_or_default();
        let limit = usize::try_from(req.msg.limit).unwrap_or(0);
        let limit = if limit == 0 { 200 } else { limit.min(2000) };
        let spaces: Vec<SpaceId> = if sel.space_ids.is_empty() {
            vec![space_id]
        } else {
            sel.space_ids
                .iter()
                .map(|s| parse_id(s, "selection"))
                .collect::<Result<_, _>>()?
        };
        let kinds: Vec<ObjectKind> = if sel.kinds.is_empty() {
            vec![ObjectKind::Entity]
        } else {
            sel.kinds
                .iter()
                .filter_map(|k| ObjectKind::try_from(*k).ok())
                .collect()
        };
        let hidden: BTreeSet<&str> = sel.hidden_ids.iter().map(String::as_str).collect();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut objects: Vec<Object> = Vec::new();
        let mut missing = Vec::new();
        // Kinds and types are deduplicated so a selection cannot multiply
        // the queries; the loops stop once the limit is reached.
        let kinds: BTreeSet<i32> = kinds.iter().map(|k| *k as i32).collect();
        let kinds: Vec<ObjectKind> = kinds
            .into_iter()
            .filter_map(|k| ObjectKind::try_from(k).ok())
            .collect();
        let type_ids: BTreeSet<String> = sel.type_ids.iter().cloned().collect();
        'spaces: for s in spaces {
            let Ok(store) = self.store.space(s).await else {
                missing.push(s.to_string());
                continue;
            };
            for kind in &kinds {
                // One query per type when types are named, one otherwise.
                let type_filters: Vec<Option<String>> = if type_ids.is_empty() {
                    vec![None]
                } else {
                    type_ids.iter().cloned().map(Some).collect()
                };
                for type_id in type_filters {
                    if objects.len() >= limit {
                        break 'spaces;
                    }
                    let found = store
                        .list(&ObjectFilter {
                            kind: Some(*kind),
                            type_id,
                            limit: limit - objects.len(),
                            ..ObjectFilter::default()
                        })
                        .await?;
                    for o in found {
                        if hidden.contains(o.id.as_str()) || !seen.insert(o.id.clone()) {
                            continue;
                        }
                        objects.push(o);
                    }
                }
            }
        }
        objects.sort_by_key(|o| std::cmp::Reverse(o.created_at_ms));
        objects.truncate(limit);
        Ok(Response::new(ResolveContextResponse {
            objects,
            missing_space_ids: missing,
        }))
    }

    async fn hide_in_context(
        &self,
        req: Request<HideInContextRequest>,
    ) -> Result<Response<HideInContextResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        parse_id(&m.object_id, "object_id")?;
        let ctx = self.context(space_id, id).await?;
        let mut sel = ctx.selection.unwrap_or_default();
        sel.hidden_ids.retain(|h| h != &m.object_id);
        if m.hidden {
            sel.hidden_ids.push(m.object_id.clone());
        }
        let rec = self
            .store
            .space(space_id)
            .await?
            .update(id, move |doc| {
                doc.set_field(SELECTION, &string_value(selection_json(&sel)))
            })
            .await?;
        Ok(Response::new(HideInContextResponse {
            context: Some(context_of(&rec)),
        }))
    }
}
