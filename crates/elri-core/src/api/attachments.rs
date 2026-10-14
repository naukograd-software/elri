//! `AttachmentService`: bytes in, bytes out, with the Object that names
//! them kept in step.

use std::sync::Arc;

use elri_proto::elri::v1::attachment::attachment_service_ws::{
    register_attachment_service_arc, AttachmentServiceHandler, DownloadServerStream,
    UploadServerStream,
};
use elri_proto::elri::v1::attachment::{
    upload_request, Attachment, Chunk, DownloadRequest, GetAttachmentRequest,
    GetAttachmentResponse, ListAttachmentsRequest, ListAttachmentsResponse, PinRequest,
    PinResponse, UploadHeader, UploadResponse,
};
use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::space::Authority;
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::blobs::{check_digest, BlobError};
use crate::model::{
    attachment_sha256, int_field, int_value, now_ms, reserved, string_field, string_value,
    ObjectDoc, ObjectId, SpaceId,
};
use crate::store::{ObjectFilter, SpaceStore, StoreError};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_attachment_service_arc(server, services);
}

impl From<BlobError> for Status {
    fn from(e: BlobError) -> Self {
        match e {
            BlobError::Hash { .. }
            | BlobError::Size { .. }
            | BlobError::TooLarge
            | BlobError::BadDigest(_) => Status::new(Code::InvalidArgument, e.to_string()),
            other @ BlobError::Io { .. } => {
                tracing::error!(error = %other, "attachment storage failure");
                Status::new(Code::Internal, "attachment storage failure")
            }
        }
    }
}

/// The protocol view of an attachment Object.
pub(super) fn attachment_of(o: &Object, available: bool, pinned: bool) -> Attachment {
    Attachment {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        media_type: string_field(o, reserved::MEDIA_TYPE)
            .unwrap_or_default()
            .to_owned(),
        size: int_field(o, reserved::SIZE).unwrap_or_default(),
        sha256: attachment_sha256(o).unwrap_or_default().to_owned(),
        available,
        pinned,
        created_at_ms: o.created_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

impl Services {
    async fn describe(&self, space: &SpaceStore, o: &Object) -> Result<Attachment, Status> {
        let sha = attachment_sha256(o).unwrap_or_default();
        let available = space.blobs().exists(space.id(), sha);
        let pinned = space.blob(sha).await?.is_some_and(|(_, p, _)| p);
        Ok(attachment_of(o, available, pinned))
    }

    async fn attachment(
        &self,
        space_id: SpaceId,
        id: ObjectId,
    ) -> Result<(Arc<SpaceStore>, Object), Status> {
        let space = self.store.space(space_id).await?;
        let o = space.get(id).await?;
        if o.kind() != ObjectKind::Attachment {
            return Err(Status::new(Code::NotFound, "not an attachment"));
        }
        Ok((space, o))
    }

    /// Makes sure the bytes are on this device, fetching them from the
    /// server for a server Space.
    async fn ensure_bytes(&self, space: &SpaceStore, sha: &str, size: i64) -> Result<(), Status> {
        if space.blobs().exists(space.id(), sha) {
            return Ok(());
        }
        let authority = self
            .store
            .list_spaces()
            .await?
            .into_iter()
            .find(|s| s.id == space.id().to_string())
            .map_or(Authority::Local, |s| s.authority());
        if authority != Authority::Server {
            return Err(Status::new(
                Code::NotFound,
                "attachment bytes are not on this device",
            ));
        }
        self.remote.fetch_blob(space.id(), sha, size).await?;
        space.record_blob(sha, size, true).await?;
        Ok(())
    }
}

impl AttachmentServiceHandler for Services {
    async fn upload(&self, stream: UploadServerStream) -> Result<Response<UploadResponse>, Status> {
        let header = match stream.recv().await? {
            Some(m) => match m.request {
                Some(upload_request::Request::Header(h)) => h,
                _ => return Err(invalid("the first message must be the header")),
            },
            None => return Err(invalid("empty upload")),
        };
        let UploadHeader {
            space_id,
            name,
            media_type,
            size,
        } = header;
        let space_id = parse_id(&space_id, "space_id")?;
        let name = name.trim().to_owned();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        let expected = u64::try_from(size).map_err(|_| invalid("size must not be negative"))?;
        let space = self.store.space(space_id).await?;
        let mut writer = space.blobs().begin(space_id)?;
        while let Some(m) = stream.recv().await? {
            match m.request {
                Some(upload_request::Request::Chunk(bytes)) => writer.write(&bytes)?,
                Some(upload_request::Request::Header(_)) => {
                    return Err(invalid("a second header"));
                }
                None => {}
            }
        }
        let sha = writer.finish(None, Some(expected))?;
        let size = i64::try_from(expected).unwrap_or(i64::MAX);

        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Attachment, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        if !media_type.trim().is_empty() {
            doc.set_field(reserved::MEDIA_TYPE, &string_value(media_type.trim()))
                .map_err(StoreError::from)?;
        }
        doc.set_field(reserved::SIZE, &int_value(size))
            .map_err(StoreError::from)?;
        doc.set_field(reserved::SHA256, &string_value(&sha))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        // Bytes first, then the Object: a Change naming the digest is only
        // submitted once the authority has the bytes.
        space.record_blob(&sha, size, false).await?;
        let rec = space.insert(&doc).await?;
        Ok(Response::new(UploadResponse {
            attachment: Some(self.describe(&space, &rec).await?),
        }))
    }

    async fn download(
        &self,
        req: Request<DownloadRequest>,
        stream: DownloadServerStream,
    ) -> Result<(), Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let (space, o) = self.attachment(space_id, id).await?;
        let sha = attachment_sha256(&o)
            .ok_or_else(|| Status::new(Code::Internal, "attachment without digest"))?
            .to_owned();
        check_digest(&sha)?;
        self.ensure_bytes(&space, &sha, int_field(&o, reserved::SIZE).unwrap_or(0))
            .await?;
        // Read on a blocking thread, send on this one.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);
        let blobs = space.blobs().clone();
        let reader = tokio::task::spawn_blocking(move || {
            blobs.read_chunks(space_id, &sha, |chunk| {
                tx.blocking_send(chunk.to_vec())
                    .map_err(|_| BlobError::TooLarge)
            })
        });
        while let Some(data) = rx.recv().await {
            stream.send(&Chunk { data: data.into() }).await?;
        }
        reader
            .await
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?
            .map_err(|e| match e {
                BlobError::TooLarge => Status::new(Code::Cancelled, "download abandoned"),
                other => other.into(),
            })?;
        Ok(())
    }

    async fn get_attachment(
        &self,
        req: Request<GetAttachmentRequest>,
    ) -> Result<Response<GetAttachmentResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let (space, o) = self.attachment(space_id, id).await?;
        Ok(Response::new(GetAttachmentResponse {
            attachment: Some(self.describe(&space, &o).await?),
        }))
    }

    async fn list_attachments(
        &self,
        req: Request<ListAttachmentsRequest>,
    ) -> Result<Response<ListAttachmentsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let space = self.store.space(space_id).await?;
        let objects = space
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Attachment),
                include_deleted: req.msg.include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        let mut attachments = Vec::with_capacity(objects.len());
        for o in &objects {
            attachments.push(self.describe(&space, o).await?);
        }
        Ok(Response::new(ListAttachmentsResponse { attachments }))
    }

    async fn pin(&self, req: Request<PinRequest>) -> Result<Response<PinResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let (space, o) = self.attachment(space_id, id).await?;
        let sha = attachment_sha256(&o).unwrap_or_default().to_owned();
        if req.msg.pinned {
            self.ensure_bytes(&space, &sha, int_field(&o, reserved::SIZE).unwrap_or(0))
                .await?;
        }
        space.set_blob_pinned(&sha, req.msg.pinned).await?;
        Ok(Response::new(PinResponse {
            attachment: Some(self.describe(&space, &o).await?),
        }))
    }
}
