//! `DocumentService`: fragments of block text, and blocks split or merged
//! as one Change.

use std::sync::Arc;

use elri_proto::elri::v1::document::document_service_ws::{
    register_document_service_arc, DocumentServiceHandler,
};
use elri_proto::elri::v1::document::{
    CaptureFragmentRequest, CaptureFragmentResponse, MergeBlocksRequest, MergeBlocksResponse,
    ResolveFragmentRequest, ResolveFragmentResponse, SplitBlockRequest, SplitBlockResponse,
};
use elri_proto::elri::v1::object::{ObjectKind, Placement};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::document;
use crate::model::{now_ms, reserved, string_value, text_value, ObjectDoc, ObjectId, SpaceId};
use crate::store::{Edit, ObjectFilter, StoreError};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_document_service_arc(server, services);
}

impl Services {
    /// A document's placement by id, with the document checked to be an
    /// Entity: (block id, parent placement id, index).
    async fn placement(
        &self,
        space_id: SpaceId,
        document_id: ObjectId,
        placement_id: &str,
    ) -> Result<(Vec<Placement>, Placement), Status> {
        let doc = self.store.space(space_id).await?.get(document_id).await?;
        if doc.kind() != ObjectKind::Entity {
            return Err(invalid("document_id must name an entity"));
        }
        let p = doc
            .placements
            .iter()
            .find(|p| p.id == placement_id)
            .cloned()
            .ok_or_else(|| Status::new(Code::NotFound, "no such placement"))?;
        Ok((doc.placements, p))
    }
}

impl DocumentServiceHandler for Services {
    async fn capture_fragment(
        &self,
        req: Request<CaptureFragmentRequest>,
    ) -> Result<Response<CaptureFragmentResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let object_id = parse_id(&m.object_id, "object_id")?;
        if m.field_id.trim().is_empty() {
            return Err(invalid("field_id is required"));
        }
        let start = usize::try_from(m.start).map_err(|_| invalid("start"))?;
        let end = usize::try_from(m.end).map_err(|_| invalid("end"))?;
        let doc = self.store.space(space_id).await?.load(object_id).await?;
        let fragment = document::capture(&doc, &m.field_id, start, end).map_err(invalid)?;
        Ok(Response::new(CaptureFragmentResponse {
            fragment: Some(fragment),
        }))
    }

    async fn resolve_fragment(
        &self,
        req: Request<ResolveFragmentRequest>,
    ) -> Result<Response<ResolveFragmentResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let f = m
            .fragment
            .as_ref()
            .ok_or_else(|| invalid("fragment is required"))?;
        parse_id(&f.object_id, "fragment.object_id")?;
        let r = document::resolve(&self.store, space_id, f).await?;
        Ok(Response::new(ResolveFragmentResponse {
            state: r.state.into(),
            range: r.range,
            candidates: r.candidates,
            redirected_to: r.redirected_to,
            reason: r.reason,
        }))
    }

    async fn split_block(
        &self,
        req: Request<SplitBlockRequest>,
    ) -> Result<Response<SplitBlockResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let document_id = parse_id(&m.document_id, "document_id")?;
        let at = usize::try_from(m.at).map_err(|_| invalid("at"))?;
        if m.field_id.trim().is_empty() {
            return Err(invalid("field_id is required"));
        }
        let (_, placement) = self
            .placement(space_id, document_id, &m.placement_id)
            .await?;
        let block_id = parse_id(&placement.block_id, "block")?;
        let space = self.store.space(space_id).await?;
        let block = space.load(block_id).await?;
        let block_rec = block.read().map_err(StoreError::from)?;
        if !document::is_block(block_rec.kind()) || block_rec.deleted_at_ms.is_some() {
            return Err(invalid("the placed block is not a live entity"));
        }
        let text = block
            .text_of(&m.field_id)
            .ok_or_else(|| invalid("the field is not text"))?;
        if at > text.chars().count() {
            return Err(invalid("at is beyond the text"));
        }
        let tail: String = text.chars().skip(at).collect();

        // The tail is a new block that remembers where it came from; it is
        // placed right after the head, in one Change with the cut.
        let tail_doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, now_ms())
            .map_err(StoreError::from)?;
        for t in &block_rec.types {
            tail_doc.add_type(t).map_err(StoreError::from)?;
        }
        tail_doc
            .set_field(&m.field_id, &text_value(&tail))
            .map_err(StoreError::from)?;
        tail_doc
            .set_field(reserved::SPLIT_FROM, &string_value(&block_rec.id))
            .map_err(StoreError::from)?;
        tail_doc.touch(now_ms()).map_err(StoreError::from)?;
        let tail_id = tail_doc.id();
        let field = m.field_id.clone();
        let parent = placement.parent_id.clone();
        let index = usize::try_from(placement.index).unwrap_or(0) + 1;
        let edits = vec![
            (
                space_id,
                Edit::Update(
                    block_id,
                    Box::new(move |d: &ObjectDoc| {
                        d.cut_text(&field, at)?;
                        d.touch(now_ms())
                    }),
                ),
            ),
            (space_id, Edit::Create(tail_doc)),
            (
                space_id,
                Edit::Update(
                    document_id,
                    Box::new(move |d: &ObjectDoc| {
                        d.place_block(&parent, index, tail_id)?;
                        d.touch(now_ms())
                    }),
                ),
            ),
        ];
        let (_, mut written) = self.store.commit(edits).await?;
        if written.len() != 3 {
            return Err(Status::new(
                Code::Internal,
                "split wrote an unexpected number of objects",
            ));
        }
        let doc_after = written.pop().unwrap_or_default();
        let tail_rec = written.pop().unwrap_or_default();
        let head = written.pop().unwrap_or_default();
        let placement = doc_after
            .placements
            .into_iter()
            .find(|p| p.block_id == tail_rec.id);
        Ok(Response::new(SplitBlockResponse {
            head: Some(head),
            tail: Some(tail_rec),
            placement,
        }))
    }

    async fn merge_blocks(
        &self,
        req: Request<MergeBlocksRequest>,
    ) -> Result<Response<MergeBlocksResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let document_id = parse_id(&m.document_id, "document_id")?;
        if m.field_id.trim().is_empty() {
            return Err(invalid("field_id is required"));
        }
        let (placements, first) = self
            .placement(space_id, document_id, &m.placement_id)
            .await?;
        let next = placements
            .iter()
            .find(|p| p.parent_id == first.parent_id && p.index == first.index + 1)
            .cloned()
            .ok_or_else(|| Status::new(Code::FailedPrecondition, "no next sibling to merge"))?;
        let keep_id = parse_id(&first.block_id, "block")?;
        let absorb_id = parse_id(&next.block_id, "next block")?;
        if keep_id == absorb_id {
            return Err(Status::new(
                Code::FailedPrecondition,
                "the next placement is the same block",
            ));
        }
        let space = self.store.space(space_id).await?;
        let keep = space.load(keep_id).await?;
        if keep.text_of(&m.field_id).is_none() {
            return Err(invalid("the field is not text on the kept block"));
        }
        let absorb = space.get(absorb_id).await?;
        if !document::is_block(absorb.kind()) || absorb.deleted_at_ms.is_some() {
            return Err(invalid("the next block is not a live entity"));
        }
        let absorbed_text = space
            .load(absorb_id)
            .await?
            .text_of(&m.field_id)
            .unwrap_or_default();
        // The absorbed block is deleted with a redirect only when nothing
        // else places it: another placement, here or elsewhere, keeps it.
        let placed_here = placements
            .iter()
            .filter(|p| p.block_id == next.block_id)
            .count();
        let placed_in = space
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Entity),
                placed_block: Some(absorb_id),
                limit: 2,
                ..ObjectFilter::default()
            })
            .await?;
        let elsewhere = placed_in.iter().any(|d| d.id != document_id.to_string());
        let absorbed_deleted = placed_here == 1 && !elsewhere;

        let field = m.field_id.clone();
        let next_id = next.id.clone();
        let mut edits = vec![
            (
                space_id,
                Edit::Update(
                    keep_id,
                    Box::new(move |d: &ObjectDoc| {
                        d.append_text(&field, &absorbed_text)?;
                        d.touch(now_ms())
                    }),
                ),
            ),
            (
                space_id,
                Edit::Update(
                    document_id,
                    Box::new(move |d: &ObjectDoc| {
                        d.remove_placement(&next_id)?;
                        d.touch(now_ms())
                    }),
                ),
            ),
        ];
        if absorbed_deleted {
            let into = keep_id.to_string();
            edits.push((
                space_id,
                Edit::Update(
                    absorb_id,
                    Box::new(move |d: &ObjectDoc| {
                        d.set_field(reserved::MERGED_INTO, &string_value(&into))?;
                        d.mark_deleted(now_ms())
                    }),
                ),
            ));
        }
        let (_, written) = self.store.commit(edits).await?;
        Ok(Response::new(MergeBlocksResponse {
            merged: written.into_iter().next(),
            absorbed_id: absorb_id.to_string(),
            absorbed_deleted,
        }))
    }
}
