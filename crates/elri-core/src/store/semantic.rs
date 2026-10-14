//! The semantic index of a Space: Objects whose text changed wait in a
//! queue; a worker embeds them outside any write and installs the vectors
//! only if the Object was not queued again meanwhile.

use std::collections::HashSet;

use elri_proto::elri::v1::object::{value, Object, ObjectKind};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::models::{EmbedQueueRow, EmbeddingRow, ObjectRow};
use super::{load_doc, SpaceStore, StoreError};
use crate::model::{now_ms, ObjectId};

/// Characters of an Object's text that are embedded.
pub const TEXT_LIMIT: usize = 2000;

/// The text the semantic index sees for an Object: its name, description
/// and every string or text field, in field order. Other reserved fields
/// are the core's own and are left out.
pub fn embed_text(o: &Object) -> String {
    let mut keys: Vec<&String> = o.fields.keys().collect();
    keys.sort();
    let mut out = String::new();
    for k in keys {
        if k.starts_with('$') && k != "$name" && k != "$description" {
            continue;
        }
        let text = match o.fields[k].value.as_ref() {
            Some(value::Value::String(s) | value::Value::Text(s)) => s.as_str(),
            Some(value::Value::Rich(r)) => r.text.as_str(),
            _ => continue,
        };
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(text);
        if out.chars().count() >= TEXT_LIMIT {
            return out.chars().take(TEXT_LIMIT).collect();
        }
    }
    out
}

/// A stable digest of a text, kept with its vector so unchanged text is
/// not embedded again.
pub fn text_digest(text: &str) -> i64 {
    let d = Sha256::digest(text.as_bytes());
    let mut b = [0u8; 8];
    b.copy_from_slice(&d[..8]);
    i64::from_le_bytes(b)
}

/// Kinds the semantic index covers: what a person writes.
pub fn indexed(kind: ObjectKind) -> bool {
    matches!(
        kind,
        ObjectKind::Entity | ObjectKind::Relation | ObjectKind::Layer
    )
}

fn encode(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
}

fn decode(b: &[u8]) -> Vec<f32> {
    b.as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
        .collect()
}

/// One queued Object as the worker sees it.
#[derive(Debug, Clone)]
pub struct Pending {
    /// The Object.
    pub id: ObjectId,
    /// When it was queued; the vector is installed only if unchanged.
    pub queued_at: i64,
    /// Its text now; empty means nothing to embed.
    pub text: String,
    /// Digest of the text.
    pub digest: i64,
    /// Whether an embedding of this model already covers this text.
    pub current: bool,
}

impl SpaceStore {
    /// Up to `limit` queued Objects, oldest first, with their text.
    pub async fn embed_backlog(
        &self,
        model: &str,
        limit: usize,
    ) -> Result<Vec<Pending>, StoreError> {
        let mut db = self.db.lock().await;
        let mut rows = EmbedQueueRow::all().exec(&mut *db).await?;
        rows.sort_by_key(|r| (r.queued_at, r.object_id));
        rows.truncate(limit);
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let id = ObjectId::from_uuid(r.object_id);
            let text = match load_doc(&mut db, id).await {
                Ok((doc, _)) => match doc.read() {
                    Ok(o) if o.deleted_at_ms.is_none() && indexed(o.kind()) => embed_text(&o),
                    _ => String::new(),
                },
                Err(StoreError::ObjectNotFound(_)) => String::new(),
                Err(e) => return Err(e),
            };
            let digest = text_digest(&text);
            let current = EmbeddingRow::filter_by_object_id(r.object_id)
                .first()
                .exec(&mut *db)
                .await?
                .is_some_and(|e| e.model == model && e.revision == digest);
            out.push(Pending {
                id,
                queued_at: r.queued_at,
                text,
                digest,
                current,
            });
        }
        Ok(out)
    }

    /// Installs vectors computed for `items` by `model`: each one only if
    /// its Object was not queued again since it was read. An empty text
    /// removes the Object's vector. Returns how many were settled.
    pub async fn install_embeddings(
        &self,
        model: &str,
        items: &[(Pending, Option<Vec<f32>>)],
    ) -> Result<usize, StoreError> {
        let mut db = self.db.lock().await;
        let mut tx = db.transaction().await?;
        let mut settled = 0;
        for (p, vector) in items {
            let id = p.id.uuid();
            let still = EmbedQueueRow::filter_by_object_id(id)
                .first()
                .exec(&mut tx)
                .await?
                .is_some_and(|q| q.queued_at == p.queued_at);
            if !still {
                continue;
            }
            if !p.current {
                EmbeddingRow::filter_by_object_id(id)
                    .delete()
                    .exec(&mut tx)
                    .await?;
                if let Some(v) = vector {
                    toasty::create!(EmbeddingRow {
                        object_id: id,
                        model: model.to_owned(),
                        revision: p.digest,
                        vector: encode(v),
                    })
                    .exec(&mut tx)
                    .await?;
                }
            }
            EmbedQueueRow::filter_by_object_id(id)
                .delete()
                .exec(&mut tx)
                .await?;
            settled += 1;
        }
        tx.commit().await?;
        Ok(settled)
    }

    /// Objects waiting for the semantic index.
    pub async fn semantic_pending(&self) -> Result<i64, StoreError> {
        let mut db = self.db.lock().await;
        let n = EmbedQueueRow::all().exec(&mut *db).await?.len();
        Ok(i64::try_from(n).unwrap_or(i64::MAX))
    }

    /// Queues every indexed Object that has no vector of `model`: after a
    /// model change, or for Objects written before the index existed.
    pub async fn enqueue_missing(&self, model: &str) -> Result<usize, StoreError> {
        let mut db = self.db.lock().await;
        let have: HashSet<Uuid> = EmbeddingRow::all()
            .exec(&mut *db)
            .await?
            .into_iter()
            .filter(|e| e.model == model)
            .map(|e| e.object_id)
            .collect();
        let queued: HashSet<Uuid> = EmbedQueueRow::all()
            .exec(&mut *db)
            .await?
            .into_iter()
            .map(|q| q.object_id)
            .collect();
        let objects = ObjectRow::all().exec(&mut *db).await?;
        let now = now_ms();
        let mut tx = db.transaction().await?;
        let mut added = 0;
        for o in objects {
            let kind = i32::try_from(o.kind)
                .ok()
                .and_then(|k| ObjectKind::try_from(k).ok())
                .unwrap_or(ObjectKind::Unspecified);
            if !indexed(kind)
                || o.deleted_at.is_some()
                || have.contains(&o.object_id)
                || queued.contains(&o.object_id)
            {
                continue;
            }
            toasty::create!(EmbedQueueRow {
                object_id: o.object_id,
                queued_at: now,
            })
            .exec(&mut tx)
            .await?;
            added += 1;
        }
        tx.commit().await?;
        Ok(added)
    }

    /// The live Objects whose vectors of `model` are closest to `query`
    /// (a unit vector), best first, with their cosine similarity.
    pub async fn nearest(
        &self,
        model: &str,
        query: &[f32],
        kind: Option<ObjectKind>,
        limit: usize,
    ) -> Result<Vec<(Object, f32)>, StoreError> {
        let limit = if limit == 0 { 50 } else { limit };
        let mut db = self.db.lock().await;
        let mut scored: Vec<(Uuid, f32)> = EmbeddingRow::all()
            .exec(&mut *db)
            .await?
            .into_iter()
            .filter(|e| e.model == model)
            .map(|e| {
                let v = decode(&e.vector);
                let dot = v.iter().zip(query).map(|(a, b)| a * b).sum::<f32>();
                (e.object_id, dot)
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        let mut out = Vec::with_capacity(limit.min(scored.len()));
        for (id, score) in scored {
            let Ok((doc, _)) = load_doc(&mut db, ObjectId::from_uuid(id)).await else {
                continue;
            };
            let o = doc.read()?;
            if o.deleted_at_ms.is_some() || kind.is_some_and(|k| o.kind() != k) {
                continue;
            }
            out.push((o, score));
            if out.len() == limit {
                break;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vectors_round_trip_and_text_skips_reserved_fields() {
        let v = vec![0.5f32, -0.25, 1.0];
        assert_eq!(decode(&encode(&v)), v);
        let mut o = Object::default();
        o.fields
            .insert("title".into(), crate::model::string_value("Graphs"));
        o.fields
            .insert("$agent".into(), crate::model::string_value("x"));
        o.fields
            .insert("$name".into(), crate::model::string_value("Note"));
        o.fields.insert("n".into(), crate::model::int_value(3));
        assert_eq!(embed_text(&o), "Note\nGraphs");
        assert_eq!(text_digest("a"), text_digest("a"));
        assert_ne!(text_digest("a"), text_digest("b"));
    }
}
