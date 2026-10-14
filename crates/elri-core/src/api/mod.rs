//! ws-proto surface of the core. Every client goes through the same server:
//! a token middleware first, then the typed service handlers. Protocol
//! messages are the model, so handlers mostly validate and delegate.

mod definitions;

use std::sync::Arc;

use elri_proto::elri::v1::change::change_service_ws::{
    register_change_service_arc, ChangeServiceHandler,
};
use elri_proto::elri::v1::change::{ListChangesRequest, ListChangesResponse};
use elri_proto::elri::v1::core::core_service_ws::{register_core_service_arc, CoreServiceHandler};
use elri_proto::elri::v1::core::{EndpointKind, GetInfoRequest, GetInfoResponse};
use elri_proto::elri::v1::object::object_service_ws::{
    register_object_service_arc, ObjectServiceHandler,
};
use elri_proto::elri::v1::object::{
    CreateObjectRequest, CreateObjectResponse, DeleteObjectRequest, DeleteObjectResponse,
    GetObjectRequest, GetObjectResponse, ListObjectsRequest, ListObjectsResponse, ObjectKind,
    Participant, Placement, UpdateObjectRequest, UpdateObjectResponse, Value,
};
use elri_proto::elri::v1::remote::remote_service_ws::{
    register_remote_service_arc, RemoteServiceHandler,
};
use elri_proto::elri::v1::remote::{
    AddMemberRequest, AddMemberResponse, ConnectRequest, ConnectResponse, CreateRemoteSpaceRequest,
    CreateRemoteSpaceResponse, DisconnectRequest, DisconnectResponse, GetStatusRequest,
    GetStatusResponse, JoinSpaceRequest, JoinSpaceResponse, ListRemoteSpacesRequest,
    ListRemoteSpacesResponse, RemoteSpace, SpaceSyncStatus,
};
use elri_proto::elri::v1::space::space_service_ws::{
    register_space_service_arc, SpaceServiceHandler,
};
use elri_proto::elri::v1::space::{
    CreateSpaceRequest, CreateSpaceResponse, ListSpacesRequest, ListSpacesResponse,
};
use ws_proto::server::{Handler, OriginPolicy};
use ws_proto::{Code, Request, Response, Server, ServerConfig, Status};

use crate::device::{DeviceId, LaunchToken};
use crate::model::{
    is_definition, is_scalar, kind_accepts, now_ms, ModelError, ObjectDoc, ObjectId,
};
use crate::remote::{Remote, RemoteError};
use crate::store::{ObjectFilter, SpaceStore, Store, StoreError};
use crate::BuildInfo;

/// Everything the handlers need.
pub struct Services {
    /// Version of the embedding binary.
    pub build: BuildInfo,
    /// This device.
    pub device_id: DeviceId,
    /// Token every request must carry.
    pub token: LaunchToken,
    /// Storage.
    pub store: Arc<Store>,
    /// Server link.
    pub remote: Arc<Remote>,
}

/// Builds the loopback server: origin policy, bearer-token middleware, all
/// services registered. The token is still required on every call, so the
/// origin list only decides which browser contexts may open the socket.
pub fn build_server(services: Services, allowed_origins: Vec<String>) -> Server {
    let server = Server::new(ServerConfig {
        origin: OriginPolicy::Patterns(allowed_origins),
        ..ServerConfig::default()
    });
    let token = services.token.clone();
    server.add_middleware(move |next: Handler| require_token(token.clone(), next));

    let services = Arc::new(services);
    register_core_service_arc(&server, Arc::clone(&services));
    register_space_service_arc(&server, Arc::clone(&services));
    register_object_service_arc(&server, Arc::clone(&services));
    register_change_service_arc(&server, Arc::clone(&services));
    register_remote_service_arc(&server, Arc::clone(&services));
    definitions::register(&server, services);
    server
}

fn require_token(token: LaunchToken, next: Handler) -> Handler {
    Arc::new(move |stream| {
        let next = Arc::clone(&next);
        let presented = stream.header().get("authorization").cloned();
        let ok = presented
            .as_deref()
            .is_some_and(|h| token.matches_header(h));
        Box::pin(async move {
            if ok {
                next(stream).await
            } else {
                Err(Status::new(
                    Code::Unauthenticated,
                    "missing or invalid token",
                ))
            }
        })
    })
}

// --- error mapping -----------------------------------------------------------

impl From<StoreError> for Status {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::SpaceNotFound(_) | StoreError::ObjectNotFound(_) => {
                Status::new(Code::NotFound, e.to_string())
            }
            StoreError::Model(
                ModelError::InvalidId(_)
                | ModelError::EmptyValue(_)
                | ModelError::NestedContainer(_)
                | ModelError::Placement(..),
            ) => Status::new(Code::InvalidArgument, e.to_string()),
            other => {
                tracing::error!(error = %other, "storage failure");
                Status::new(Code::Internal, "storage failure")
            }
        }
    }
}

impl From<RemoteError> for Status {
    fn from(e: RemoteError) -> Self {
        match e {
            RemoteError::NotConfigured => Status::new(Code::FailedPrecondition, e.to_string()),
            RemoteError::Rpc(status) => status,
            RemoteError::Store(s) => s.into(),
            RemoteError::BadReply(_) => Status::new(Code::Internal, e.to_string()),
        }
    }
}

pub(crate) fn invalid(msg: impl Into<String>) -> Status {
    Status::new(Code::InvalidArgument, msg)
}

pub(crate) fn parse_id(s: &str, what: &str) -> Result<ObjectId, Status> {
    ObjectId::parse(s).map_err(|_| invalid(format!("{what}: not a valid id")))
}

fn kind_arg(kind: i32) -> Result<ObjectKind, Status> {
    ObjectKind::try_from(kind)
        .ok()
        .filter(|k| *k != ObjectKind::Unspecified)
        .ok_or_else(|| invalid("kind is required"))
}

// --- validation --------------------------------------------------------------

fn check_fields(fields: &std::collections::HashMap<String, Value>) -> Result<(), Status> {
    use elri_proto::elri::v1::object::value::Value as V;
    for (id, v) in fields {
        if id.is_empty() {
            return Err(invalid("field id must not be empty"));
        }
        match &v.value {
            None => return Err(invalid(format!("field {id}: value is empty"))),
            Some(V::Ref(r)) => {
                parse_id(r, &format!("field {id}"))?;
            }
            Some(V::List(l)) => {
                for item in &l.items {
                    if !is_scalar(item) {
                        return Err(invalid(format!("field {id}: list items must be scalars")));
                    }
                    if let Some(V::Ref(r)) = &item.value {
                        parse_id(r, &format!("field {id}"))?;
                    }
                }
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn check_participants(kind: ObjectKind, ps: &[Participant]) -> Result<(), Status> {
    if ps.is_empty() {
        return Ok(());
    }
    if kind != ObjectKind::Relation {
        return Err(invalid("participants are only valid on relations"));
    }
    for p in ps {
        if p.slot.is_empty() {
            return Err(invalid("participant slot must not be empty"));
        }
        parse_id(&p.object_id, &format!("participant {}", p.slot))?;
    }
    Ok(())
}

impl Services {
    /// Rejects placements that do not name live blocks of the Space, or
    /// that go on anything but an Entity.
    async fn check_placements(
        &self,
        space: &SpaceStore,
        kind: ObjectKind,
        set: &[Placement],
    ) -> Result<(), Status> {
        if kind != ObjectKind::Entity {
            return Err(invalid("placements are only valid on entities"));
        }
        for p in set {
            if p.index < 0 {
                return Err(invalid("placement index must not be negative"));
            }
            let block = parse_id(&p.block_id, "block_id")?;
            if p.id.is_empty() && !space.exists(block).await? {
                return Err(Status::new(
                    Code::NotFound,
                    format!("block {block} is not in this space"),
                ));
            }
        }
        Ok(())
    }

    /// Rejects values whose field definition fixes a different kind. Field
    /// ids without a definition are free-form and pass through.
    async fn check_field_kinds(
        &self,
        space: &SpaceStore,
        fields: &std::collections::HashMap<String, Value>,
    ) -> Result<(), Status> {
        for (id, value) in fields {
            if let Some(kind) = space.field_kind(id).await? {
                if !kind_accepts(kind, value) {
                    return Err(invalid(format!(
                        "field {id}: definition requires {}",
                        kind.as_str_name()
                    )));
                }
            }
        }
        Ok(())
    }
}

// --- handlers ----------------------------------------------------------------

impl CoreServiceHandler for Services {
    async fn get_info(
        &self,
        _req: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            version: self.build.version.to_owned(),
            commit: self.build.commit.to_owned(),
            kind: EndpointKind::LocalCore.into(),
            device_id: self.device_id.as_string(),
        }))
    }
}

impl SpaceServiceHandler for Services {
    async fn list_spaces(
        &self,
        _req: Request<ListSpacesRequest>,
    ) -> Result<Response<ListSpacesResponse>, Status> {
        Ok(Response::new(ListSpacesResponse {
            spaces: self.store.list_spaces().await?,
        }))
    }

    async fn create_space(
        &self,
        req: Request<CreateSpaceRequest>,
    ) -> Result<Response<CreateSpaceResponse>, Status> {
        let name = req.msg.name.trim();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        Ok(Response::new(CreateSpaceResponse {
            space: Some(self.store.create_space(name).await?),
        }))
    }
}

impl ObjectServiceHandler for Services {
    async fn create_object(
        &self,
        req: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let kind = kind_arg(m.kind)?;
        if is_definition(kind) {
            return Err(invalid("definitions are created through DefinitionService"));
        }
        check_fields(&m.fields)?;
        check_participants(kind, &m.participants)?;
        let target = match (kind, m.target_id.is_empty()) {
            (ObjectKind::Layer, true) => return Err(invalid("target_id is required for layers")),
            (ObjectKind::Layer, false) => Some(parse_id(&m.target_id, "target_id")?),
            (_, false) => return Err(invalid("target_id is only valid on layers")),
            (_, true) => None,
        };
        let space = self.store.space(space_id).await?;
        self.check_field_kinds(&space, &m.fields).await?;

        let doc = match target {
            Some(t) => ObjectDoc::create_layer(ObjectId::new(), space_id, t, now_ms()),
            None => ObjectDoc::create(ObjectId::new(), space_id, kind, now_ms()),
        }
        .map_err(StoreError::from)?;
        for p in &m.participants {
            doc.set_participant(p).map_err(StoreError::from)?;
        }
        for t in &m.types {
            doc.add_type(t).map_err(StoreError::from)?;
        }
        for (k, v) in &m.fields {
            doc.set_field(k, v).map_err(StoreError::from)?;
        }
        doc.touch(now_ms()).map_err(StoreError::from)?;
        Ok(Response::new(CreateObjectResponse {
            object: Some(space.insert(&doc).await?),
        }))
    }

    async fn get_object(
        &self,
        req: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        Ok(Response::new(GetObjectResponse {
            object: Some(self.store.space(space_id).await?.get(id).await?),
        }))
    }

    async fn update_object(
        &self,
        req: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        check_fields(&m.set_fields)?;
        let space = self.store.space(space_id).await?;
        self.check_field_kinds(&space, &m.set_fields).await?;
        let touches_participants =
            !m.set_participants.is_empty() || !m.remove_participants.is_empty();
        let touches_placements = !m.set_placements.is_empty() || !m.remove_placements.is_empty();
        if touches_participants || touches_placements {
            let kind = space.get(id).await?.kind();
            if touches_participants {
                check_participants(kind, &m.set_participants)?;
                if kind != ObjectKind::Relation {
                    return Err(invalid("participants are only valid on relations"));
                }
            }
            if touches_placements {
                self.check_placements(&space, kind, &m.set_placements)
                    .await?;
            }
        }
        let object = space
            .update(id, |doc| {
                for (k, v) in &m.set_fields {
                    doc.set_field(k, v)?;
                }
                for p in &m.set_participants {
                    doc.set_participant(p)?;
                }
                for slot in &m.remove_participants {
                    doc.remove_participant(slot)?;
                }
                for k in &m.remove_fields {
                    doc.remove_field(k)?;
                }
                for t in &m.add_types {
                    doc.add_type(t)?;
                }
                for t in &m.remove_types {
                    doc.remove_type(t)?;
                }
                for p in &m.set_placements {
                    let index = usize::try_from(p.index).unwrap_or(0);
                    if p.id.is_empty() {
                        doc.place_block(&p.parent_id, index, ObjectId::parse(&p.block_id)?)?;
                    } else {
                        doc.move_placement(&p.id, &p.parent_id, index)?;
                    }
                }
                for p in &m.remove_placements {
                    doc.remove_placement(p)?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdateObjectResponse {
            object: Some(object),
        }))
    }

    async fn delete_object(
        &self,
        req: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        self.store
            .space(space_id)
            .await?
            .update(id, |doc| doc.mark_deleted(now_ms()))
            .await?;
        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn list_objects(
        &self,
        req: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let kind = ObjectKind::try_from(m.kind)
            .ok()
            .filter(|k| *k != ObjectKind::Unspecified);
        let filter = ObjectFilter {
            kind,
            type_id: (!m.type_id.is_empty()).then(|| m.type_id.clone()),
            include_deleted: m.include_deleted,
            limit: usize::try_from(m.limit).unwrap_or(0),
            participant: if m.participant_object_id.is_empty() {
                None
            } else {
                Some(parse_id(&m.participant_object_id, "participant_object_id")?)
            },
            target: if m.target_id.is_empty() {
                None
            } else {
                Some(parse_id(&m.target_id, "target_id")?)
            },
            placed_block: if m.placed_block_id.is_empty() {
                None
            } else {
                Some(parse_id(&m.placed_block_id, "placed_block_id")?)
            },
        };
        Ok(Response::new(ListObjectsResponse {
            objects: self.store.space(space_id).await?.list(&filter).await?,
        }))
    }
}

impl ChangeServiceHandler for Services {
    async fn list_changes(
        &self,
        req: Request<ListChangesRequest>,
    ) -> Result<Response<ListChangesResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let limit = usize::try_from(req.msg.limit).unwrap_or(0);
        Ok(Response::new(ListChangesResponse {
            changes: self
                .store
                .space(space_id)
                .await?
                .list_changes(limit)
                .await?,
        }))
    }
}

impl RemoteServiceHandler for Services {
    async fn connect(
        &self,
        req: Request<ConnectRequest>,
    ) -> Result<Response<ConnectResponse>, Status> {
        let url = req.msg.url.trim();
        if url.is_empty() || req.msg.token.is_empty() {
            return Err(invalid("url and token are required"));
        }
        let cfg = self.remote.connect(url, &req.msg.token).await?;
        Ok(Response::new(ConnectResponse {
            user_id: cfg.user_id,
            user_name: cfg.user_name,
        }))
    }

    async fn disconnect(
        &self,
        _req: Request<DisconnectRequest>,
    ) -> Result<Response<DisconnectResponse>, Status> {
        self.remote.disconnect().await?;
        Ok(Response::new(DisconnectResponse {}))
    }

    async fn get_status(
        &self,
        _req: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let st = self.remote.status().await?;
        Ok(Response::new(GetStatusResponse {
            configured: st.configured,
            url: st.url,
            user_name: st.user_name,
            connected: st.connected,
            spaces: st
                .spaces
                .iter()
                .map(|s| SpaceSyncStatus {
                    space_id: s.space_id.to_string(),
                    cursor: s.cursor,
                    pending: i32::try_from(s.pending).unwrap_or(i32::MAX),
                    rejected: i32::try_from(s.rejected).unwrap_or(i32::MAX),
                    connected: st.connected,
                })
                .collect(),
        }))
    }

    async fn list_remote_spaces(
        &self,
        _req: Request<ListRemoteSpacesRequest>,
    ) -> Result<Response<ListRemoteSpacesResponse>, Status> {
        let spaces = self.remote.list_remote_spaces().await?;
        Ok(Response::new(ListRemoteSpacesResponse {
            spaces: spaces
                .into_iter()
                .map(|(space, joined)| RemoteSpace {
                    space: Some(space),
                    joined,
                })
                .collect(),
        }))
    }

    async fn create_remote_space(
        &self,
        req: Request<CreateRemoteSpaceRequest>,
    ) -> Result<Response<CreateRemoteSpaceResponse>, Status> {
        let name = req.msg.name.trim();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        let space = self.remote.create_remote_space(name).await?;
        Ok(Response::new(CreateRemoteSpaceResponse {
            space: Some(space),
        }))
    }

    async fn join_space(
        &self,
        req: Request<JoinSpaceRequest>,
    ) -> Result<Response<JoinSpaceResponse>, Status> {
        let id = parse_id(&req.msg.space_id, "space_id")?;
        let space = self.remote.join_space(id).await?;
        Ok(Response::new(JoinSpaceResponse { space: Some(space) }))
    }

    async fn add_member(
        &self,
        req: Request<AddMemberRequest>,
    ) -> Result<Response<AddMemberResponse>, Status> {
        let id = parse_id(&req.msg.space_id, "space_id")?;
        if req.msg.user_name.trim().is_empty() {
            return Err(invalid("user_name is required"));
        }
        self.remote
            .add_member(id, req.msg.user_name.trim(), req.msg.role)
            .await?;
        Ok(Response::new(AddMemberResponse {}))
    }
}
