//! `SearchService`: words through the full-text index, meaning through the
//! local embedding model, or both fused by rank. Every answer says how
//! complete it is.

use std::collections::HashMap;
use std::sync::Arc;

use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::search::search_service_ws::{
    register_search_service_arc, SearchServiceHandler,
};
use elri_proto::elri::v1::search::{
    GetSemanticStatusRequest, GetSemanticStatusResponse, Hit, PrepareSemanticRequest,
    PrepareSemanticResponse, SearchMode, SearchRequest, SearchResponse, SemanticState,
    SemanticStatus, Watermark,
};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::model::ObjectId;
use crate::store::StoreError;

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_search_service_arc(server, services);
}

/// Rank constant of reciprocal rank fusion.
const RRF_K: f64 = 60.0;

impl Services {
    async fn semantic_status(&self) -> Result<SemanticStatus, Status> {
        let mut status = self.semantic.status().await;
        let mut pending = 0;
        for info in self.store.list_spaces().await? {
            if let Ok(id) = ObjectId::parse(&info.id) {
                if let Ok(space) = self.store.space(id).await {
                    pending += space.semantic_pending().await?;
                }
            }
        }
        status.pending = pending;
        Ok(status)
    }
}

fn lexical_hits(found: Vec<(Object, f64, Vec<String>)>) -> Vec<Hit> {
    found
        .into_iter()
        .map(|(object, score, fields)| Hit {
            object: Some(object),
            score,
            fields,
            reasons: vec!["words".to_owned()],
            similarity: 0.0,
        })
        .collect()
}

fn semantic_hits(found: Vec<(Object, f32)>) -> Vec<Hit> {
    found
        .into_iter()
        .map(|(object, sim)| Hit {
            object: Some(object),
            score: f64::from(sim),
            fields: Vec::new(),
            reasons: vec!["meaning".to_owned()],
            similarity: f64::from(sim),
        })
        .collect()
}

/// Fuses two rankings: each list contributes 1/(k + rank) per Object.
fn fuse(lexical: Vec<Hit>, semantic: Vec<Hit>, limit: usize) -> Vec<Hit> {
    let mut order: Vec<String> = Vec::new();
    let mut merged: HashMap<String, Hit> = HashMap::new();
    for list in [lexical, semantic] {
        for (rank, hit) in list.into_iter().enumerate() {
            let id = hit
                .object
                .as_ref()
                .map(|o| o.id.clone())
                .unwrap_or_default();
            #[allow(clippy::cast_precision_loss)]
            let add = 1.0 / (RRF_K + rank as f64 + 1.0);
            if let Some(m) = merged.get_mut(&id) {
                m.score += add;
                m.fields.extend(hit.fields);
                m.reasons.extend(hit.reasons);
                m.similarity = m.similarity.max(hit.similarity);
            } else {
                order.push(id.clone());
                merged.insert(id, Hit { score: add, ..hit });
            }
        }
    }
    let mut out: Vec<Hit> = order
        .into_iter()
        .filter_map(|id| merged.remove(&id))
        .collect();
    out.sort_by(|a, b| b.score.total_cmp(&a.score));
    out.truncate(limit);
    out
}

impl SearchServiceHandler for Services {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        if m.query.trim().is_empty() {
            return Err(invalid("query is required"));
        }
        let kind = ObjectKind::try_from(m.kind)
            .ok()
            .filter(|k| *k != ObjectKind::Unspecified);
        let limit = usize::try_from(m.limit).unwrap_or(0).min(500);
        let limit = if limit == 0 { 50 } else { limit };
        let mode = match m.mode() {
            SearchMode::Unspecified | SearchMode::Lexical => SearchMode::Lexical,
            other => other,
        };
        if mode != SearchMode::Lexical && !m.mapping_id.is_empty() {
            return Err(invalid("a mapping narrows lexical search only"));
        }
        let space = self.store.space(space_id).await?;
        let semantic = self.semantic_status().await?;
        let ready = semantic.state() == SemanticState::Ready;
        if mode == SearchMode::Semantic && !ready {
            return Err(Status::new(
                Code::FailedPrecondition,
                format!(
                    "the semantic index is not ready ({})",
                    semantic
                        .state()
                        .as_str_name()
                        .trim_start_matches("SEMANTIC_STATE_")
                        .to_lowercase()
                ),
            ));
        }

        let lexical = if mode == SearchMode::Semantic {
            Vec::new()
        } else {
            let found = space
                .search(&m.query, kind, limit)
                .await
                .map_err(|e| match e {
                    // The engine rejects malformed query syntax at execution.
                    StoreError::Db(inner) => invalid(format!("query: {inner}")),
                    other => Status::from(other),
                })?;
            // A mapping narrows the hits to the fields it names here.
            let mapped: Option<Vec<String>> = if m.mapping_id.is_empty() {
                None
            } else {
                let mapping = parse_id(&m.mapping_id, "mapping_id")?;
                Some(crate::mapping::fields_for(&self.store, space_id, mapping, space_id).await?)
            };
            let found = match &mapped {
                Some(wanted) => found
                    .into_iter()
                    .filter_map(|(o, score, fields)| {
                        let kept: Vec<String> =
                            fields.into_iter().filter(|f| wanted.contains(f)).collect();
                        (!kept.is_empty()).then_some((o, score, kept))
                    })
                    .collect(),
                None => found,
            };
            lexical_hits(found)
        };
        let meaning = if mode == SearchMode::Lexical || !ready {
            Vec::new()
        } else {
            let (model, vector) = self
                .semantic
                .embed_query(m.query.trim())
                .await
                .map_err(|e| Status::new(Code::Unavailable, e))?;
            semantic_hits(space.nearest(&model, &vector, kind, limit).await?)
        };
        let hits = match mode {
            SearchMode::Semantic => meaning,
            SearchMode::Hybrid => fuse(lexical, meaning, limit),
            _ => lexical,
        };

        // The text index is written in the data's transaction, so it
        // covers exactly what the Space holds; what the Space holds may
        // still be a partial initial copy, and the semantic index may lag.
        let data_seq = space.cursor().await?;
        let downloading = space.downloading().await?;
        Ok(Response::new(SearchResponse {
            hits,
            watermark: Some(Watermark {
                data_seq,
                index_seq: data_seq,
                partial: downloading.is_some(),
                target_seq: downloading.map_or(0, |(_, target)| target),
                semantic_pending: space.semantic_pending().await?,
                semantic_state: semantic.state,
            }),
        }))
    }

    async fn get_semantic_status(
        &self,
        _req: Request<GetSemanticStatusRequest>,
    ) -> Result<Response<GetSemanticStatusResponse>, Status> {
        Ok(Response::new(GetSemanticStatusResponse {
            status: Some(self.semantic_status().await?),
        }))
    }

    async fn prepare_semantic(
        &self,
        _req: Request<PrepareSemanticRequest>,
    ) -> Result<Response<PrepareSemanticResponse>, Status> {
        if self.semantic.status().await.state() == SemanticState::Off {
            return Err(Status::new(
                Code::FailedPrecondition,
                "semantic search is switched off in the settings",
            ));
        }
        let mut status = self.semantic.prepare().await;
        status.pending = self.semantic_status().await?.pending;
        Ok(Response::new(PrepareSemanticResponse {
            status: Some(status),
        }))
    }
}
