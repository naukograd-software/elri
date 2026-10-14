//! `TransferService`: export, import, and move Objects between Spaces.

use std::sync::Arc;

use elri_proto::elri::v1::space::Authority;
use elri_proto::elri::v1::transfer::transfer_service_ws::{
    register_transfer_service_arc, TransferServiceHandler,
};
use elri_proto::elri::v1::transfer::{
    Bundle, ExportObjectsRequest, ExportObjectsResponse, ImportObjectsRequest,
    ImportObjectsResponse, MoveObjectsRequest, MoveObjectsResponse,
};
use prost::Message as _;
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::model::{now_ms, string_value, ObjectId, SpaceId};
use crate::store::{Edit, StoreError};
use crate::transfer::{self, TransferError};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_transfer_service_arc(server, services);
}

impl From<TransferError> for Status {
    fn from(e: TransferError) -> Self {
        match e {
            TransferError::Store(s) => s.into(),
            TransferError::Model(m) => StoreError::from(m).into(),
            TransferError::Bundle(_) => Status::new(Code::InvalidArgument, e.to_string()),
            TransferError::Exists(_) => Status::new(Code::AlreadyExists, e.to_string()),
        }
    }
}

/// A transfer policy's answer as a status: a denial is final, a request
/// for confirmation names what to confirm.
async fn policy_gate(
    store: &crate::store::Store,
    from: SpaceId,
    op: elri_proto::elri::v1::space::TransferOp,
    dest: &str,
    confirmed: bool,
) -> Result<(), Status> {
    match crate::policy::check(store, from, op, dest, confirmed).await? {
        Ok(()) => Ok(()),
        Err(e @ crate::policy::PolicyError::Denied { .. }) => {
            Err(Status::new(Code::PermissionDenied, e.to_string()))
        }
        Err(e @ crate::policy::PolicyError::Ask { .. }) => {
            Err(Status::new(Code::FailedPrecondition, e.to_string()))
        }
    }
}

impl Services {
    async fn authority(&self, space_id: SpaceId) -> Result<Authority, Status> {
        Ok(self
            .store
            .list_spaces()
            .await?
            .into_iter()
            .find(|s| s.id == space_id.to_string())
            .ok_or_else(|| Status::new(Code::NotFound, format!("space {space_id} not found")))?
            .authority())
    }

    async fn import_bundle(
        &self,
        dest: SpaceId,
        bundle: &Bundle,
        keep_ids: bool,
        origin: bool,
    ) -> Result<ImportObjectsResponse, Status> {
        let space = self.store.space(dest).await?;
        let ids = transfer::mapping(bundle, keep_ids);
        if keep_ids {
            for o in &bundle.objects {
                let id = parse_id(&o.id, "object id")?;
                if space.exists(id).await? || space.is_purged(id).await? {
                    return Err(TransferError::Exists(o.id.clone()).into());
                }
            }
        }
        let edits = transfer::plan(bundle, dest, &ids, origin).map_err(StoreError::from)?;
        let (_, objects) = self.store.commit(edits).await?;
        Ok(ImportObjectsResponse { ids, objects })
    }
}

impl TransferServiceHandler for Services {
    async fn export_objects(
        &self,
        req: Request<ExportObjectsRequest>,
    ) -> Result<Response<ExportObjectsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        if req.msg.ids.is_empty() {
            return Err(invalid("ids are required"));
        }
        let mut ids = Vec::with_capacity(req.msg.ids.len());
        for id in &req.msg.ids {
            ids.push(parse_id(id, "ids")?);
        }
        policy_gate(
            &self.store,
            space_id,
            elri_proto::elri::v1::space::TransferOp::Export,
            "",
            req.msg.confirm,
        )
        .await?;
        let space = self.store.space(space_id).await?;
        let bundle = transfer::export(&space, &ids, req.msg.with_dependencies).await?;
        Ok(Response::new(ExportObjectsResponse {
            count: i32::try_from(bundle.objects.len()).unwrap_or(i32::MAX),
            bundle: bundle.encode_to_vec().into(),
        }))
    }

    async fn import_objects(
        &self,
        req: Request<ImportObjectsRequest>,
    ) -> Result<Response<ImportObjectsResponse>, Status> {
        let dest = parse_id(&req.msg.space_id, "space_id")?;
        let bundle = Bundle::decode(req.msg.bundle.as_ref())
            .map_err(|e| TransferError::Bundle(e.to_string()))?;
        if bundle.objects.is_empty() {
            return Err(invalid("the bundle is empty"));
        }
        // The origin's rules follow its data: an import is a copy from it.
        if let Ok(from) = ObjectId::parse(&bundle.exported_from) {
            if from != dest {
                policy_gate(
                    &self.store,
                    from,
                    elri_proto::elri::v1::space::TransferOp::Copy,
                    &dest.to_string(),
                    req.msg.confirm,
                )
                .await?;
            }
        }
        let res = self
            .import_bundle(dest, &bundle, req.msg.keep_ids, true)
            .await?;
        Ok(Response::new(res))
    }

    async fn move_objects(
        &self,
        req: Request<MoveObjectsRequest>,
    ) -> Result<Response<MoveObjectsResponse>, Status> {
        let source = parse_id(&req.msg.source_space_id, "source_space_id")?;
        let dest = parse_id(&req.msg.dest_space_id, "dest_space_id")?;
        if source == dest {
            return Err(invalid("source and destination are the same space"));
        }
        if req.msg.ids.is_empty() {
            return Err(invalid("ids are required"));
        }
        for s in [source, dest] {
            if self.authority(s).await? != Authority::Server {
                continue;
            }
            return Err(Status::new(
                Code::FailedPrecondition,
                "moving between server spaces is an administrative command the server does not offer yet",
            ));
        }
        let mut ids = Vec::with_capacity(req.msg.ids.len());
        for id in &req.msg.ids {
            ids.push(parse_id(id, "ids")?);
        }
        policy_gate(
            &self.store,
            source,
            elri_proto::elri::v1::space::TransferOp::Copy,
            &dest.to_string(),
            req.msg.confirm,
        )
        .await?;
        let from = self.store.space(source).await?;
        let bundle = transfer::export(&from, &ids, false).await?;
        let res = self.import_bundle(dest, &bundle, true, false).await?;
        // The source keeps a tombstone naming where the Object went.
        let mut edits = Vec::with_capacity(ids.len());
        for id in ids {
            edits.push((
                source,
                Edit::Update(
                    id,
                    Box::new(move |doc| {
                        doc.set_field(transfer::MOVED_TO, &string_value(dest.to_string()))?;
                        doc.mark_deleted(now_ms())
                    }),
                ),
            ));
        }
        self.store.commit(edits).await?;
        Ok(Response::new(MoveObjectsResponse {
            objects: res.objects,
        }))
    }
}
