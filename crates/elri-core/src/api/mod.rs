//! ws-proto surface of the core. Every client goes through the same server:
//! a token middleware first, then the typed service handlers. Protocol
//! messages are the model, so handlers mostly validate and delegate.

mod attachments;
mod definitions;
mod scripts;
mod ui;

use std::sync::Arc;

use elri_proto::elri::v1::backup::backup_service_ws::{
    register_backup_service_arc, BackupServiceHandler,
};
use elri_proto::elri::v1::backup::{BackupSpace, CreateBackupRequest, CreateBackupResponse};
use elri_proto::elri::v1::change::change_service_ws::{
    register_change_service_arc, ChangeServiceHandler,
};
use elri_proto::elri::v1::change::{
    DiscardChangeRequest, DiscardChangeResponse, ListChangesRequest, ListChangesResponse,
    RetryChangeRequest, RetryChangeResponse,
};
use elri_proto::elri::v1::core::core_service_ws::{register_core_service_arc, CoreServiceHandler};
use elri_proto::elri::v1::core::{EndpointKind, GetInfoRequest, GetInfoResponse};
use elri_proto::elri::v1::events::event_service_ws::{
    register_event_service_arc, EventServiceHandler, WatchServerStream,
};
use elri_proto::elri::v1::events::{event, Event, Lagged, WatchRequest};
use elri_proto::elri::v1::object::object_service_ws::{
    register_object_service_arc, ObjectServiceHandler,
};
use elri_proto::elri::v1::object::{
    write, CommitRequest, CommitResponse, CreateObjectRequest, CreateObjectResponse,
    DeleteObjectRequest, DeleteObjectResponse, GetObjectRequest, GetObjectResponse,
    ListObjectsRequest, ListObjectsResponse, ObjectKind, Participant, Placement,
    PurgeObjectRequest, PurgeObjectResponse, UpdateObjectRequest, UpdateObjectResponse, Value,
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
use elri_proto::elri::v1::search::search_service_ws::{
    register_search_service_arc, SearchServiceHandler,
};
use elri_proto::elri::v1::search::{Hit, SearchRequest, SearchResponse};
use elri_proto::elri::v1::space::space_service_ws::{
    register_space_service_arc, SpaceServiceHandler,
};
use elri_proto::elri::v1::space::{
    Authority, CreateSpaceRequest, CreateSpaceResponse, ListSpacesRequest, ListSpacesResponse,
};
use ws_proto::server::{Handler, OriginPolicy};
use ws_proto::{Code, Request, Response, Server, ServerConfig, Status};

use crate::device::{DeviceId, LaunchToken};
use crate::model::SpaceId;
use crate::model::{is_managed, is_scalar, kind_accepts, now_ms, ModelError, ObjectDoc, ObjectId};
use crate::remote::{Remote, RemoteError};
use crate::store::{Edit, ObjectFilter, SpaceStore, Store, StoreError};
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
    /// Script runtime.
    pub scripts: crate::script::Host,
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
    register_event_service_arc(&server, Arc::clone(&services));
    register_search_service_arc(&server, Arc::clone(&services));
    register_backup_service_arc(&server, Arc::clone(&services));
    definitions::register(&server, Arc::clone(&services));
    attachments::register(&server, Arc::clone(&services));
    scripts::register(&server, Arc::clone(&services));
    ui::register(&server, services);
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
            StoreError::SpaceNotFound(_)
            | StoreError::ObjectNotFound(_)
            | StoreError::ChangeNotFound(_) => Status::new(Code::NotFound, e.to_string()),
            StoreError::NotRejected(_)
            | StoreError::Downloading { .. }
            | StoreError::NotDeleted(_)
            | StoreError::Purged(_) => Status::new(Code::FailedPrecondition, e.to_string()),
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
            RemoteError::NotConfigured | RemoteError::TokenMissing => {
                Status::new(Code::FailedPrecondition, e.to_string())
            }
            RemoteError::Rpc(status) => status,
            RemoteError::Store(s) => s.into(),
            RemoteError::Blob(b) => b.into(),
            RemoteError::Model(m) => StoreError::from(m).into(),
            RemoteError::Secrets(_) | RemoteError::BadReply(_) => {
                tracing::error!(error = %e, "remote failure");
                Status::new(Code::Internal, e.to_string())
            }
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

impl Services {
    /// Checks a create request and builds its document, without writing.
    async fn plan_create(&self, m: &CreateObjectRequest) -> Result<(SpaceId, ObjectDoc), Status> {
        let space_id = parse_id(&m.space_id, "space_id")?;
        let kind = kind_arg(m.kind)?;
        if is_managed(kind) {
            return Err(invalid(
                "definitions and attachments are created through their own services",
            ));
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
        Ok((space_id, doc))
    }

    /// Checks an update request and builds the edit, without writing.
    async fn plan_update(
        &self,
        m: &UpdateObjectRequest,
    ) -> Result<(SpaceId, ObjectId, UpdateEdit), Status> {
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
        let m = m.clone();
        let edit: UpdateEdit = Box::new(move |doc: &ObjectDoc| {
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
        });
        Ok((space_id, id, edit))
    }
}

/// A checked update, ready to apply to a document.
type UpdateEdit = Box<dyn FnOnce(&ObjectDoc) -> Result<(), ModelError> + Send>;

impl ObjectServiceHandler for Services {
    async fn create_object(
        &self,
        req: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>, Status> {
        let (space_id, doc) = self.plan_create(&req.msg).await?;
        let space = self.store.space(space_id).await?;
        Ok(Response::new(CreateObjectResponse {
            object: Some(space.insert(&doc).await?),
        }))
    }

    async fn commit(
        &self,
        req: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        if req.msg.writes.is_empty() {
            return Err(invalid("writes are required"));
        }
        let mut edits = Vec::with_capacity(req.msg.writes.len());
        for w in &req.msg.writes {
            match &w.write {
                Some(write::Write::Create(m)) => {
                    let (space, doc) = self.plan_create(m).await?;
                    edits.push((space, Edit::Create(doc)));
                }
                Some(write::Write::Update(m)) => {
                    let (space, id, edit) = self.plan_update(m).await?;
                    edits.push((space, Edit::Update(id, edit)));
                }
                Some(write::Write::Delete(m)) => {
                    let space = parse_id(&m.space_id, "space_id")?;
                    let id = parse_id(&m.id, "id")?;
                    edits.push((
                        space,
                        Edit::Update(id, Box::new(|doc| doc.mark_deleted(now_ms()))),
                    ));
                }
                None => return Err(invalid("empty write")),
            }
        }
        let (change_id, objects) = self.store.commit(edits).await?;
        Ok(Response::new(CommitResponse { change_id, objects }))
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
        let (space_id, id, edit) = self.plan_update(&req.msg).await?;
        let object = self.store.space(space_id).await?.update(id, edit).await?;
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

    async fn purge_object(
        &self,
        req: Request<PurgeObjectRequest>,
    ) -> Result<Response<PurgeObjectResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let authority = self
            .store
            .list_spaces()
            .await?
            .into_iter()
            .find(|s| s.id == space_id.to_string())
            .ok_or_else(|| Status::new(Code::NotFound, format!("space {space_id} not found")))?
            .authority();
        match authority {
            Authority::Server => {
                self.remote.purge_object(space_id, id).await?;
            }
            _ => {
                self.store.space(space_id).await?.purge_object(id).await?;
            }
        }
        Ok(Response::new(PurgeObjectResponse {}))
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

impl From<crate::backup::BackupError> for Status {
    fn from(e: crate::backup::BackupError) -> Self {
        use crate::backup::BackupError as E;
        match e {
            E::Store(s) => s.into(),
            E::Occupied(_) | E::InUse(_) => Status::new(Code::FailedPrecondition, e.to_string()),
            E::Format(_) | E::Checksum { .. } | E::BadEntry(_) => {
                Status::new(Code::InvalidArgument, e.to_string())
            }
            other => {
                tracing::error!(error = %other, "backup failure");
                Status::new(Code::Internal, other.to_string())
            }
        }
    }
}

impl BackupServiceHandler for Services {
    async fn create_backup(
        &self,
        req: Request<CreateBackupRequest>,
    ) -> Result<Response<CreateBackupResponse>, Status> {
        let dest = req.msg.dest_dir.trim();
        if dest.is_empty() {
            return Err(invalid("dest_dir is required"));
        }
        let (manifest, dir) =
            crate::backup::create(&self.store, std::path::Path::new(dest)).await?;
        Ok(Response::new(CreateBackupResponse {
            dir: dir.to_string_lossy().into_owned(),
            created_at_ms: manifest.created_at_ms,
            verified: manifest.verified,
            bytes: i64::try_from(manifest.files.iter().map(|f| f.bytes).sum::<u64>())
                .unwrap_or(i64::MAX),
            spaces: manifest
                .spaces
                .iter()
                .map(|s| BackupSpace {
                    id: s.id.clone(),
                    name: s.name.clone(),
                    cursor: s.cursor,
                    pending: i32::try_from(s.pending).unwrap_or(i32::MAX),
                    rejected: i32::try_from(s.rejected).unwrap_or(i32::MAX),
                })
                .collect(),
            omissions: manifest.omissions,
        }))
    }
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
        let hits = self
            .store
            .space(space_id)
            .await?
            .search(&m.query, kind, limit)
            .await
            .map_err(|e| match e {
                // The engine rejects malformed query syntax at execution.
                StoreError::Db(inner) => invalid(format!("query: {inner}")),
                other => Status::from(other),
            })?;
        Ok(Response::new(SearchResponse {
            hits: hits
                .into_iter()
                .map(|(object, score, fields)| Hit {
                    object: Some(object),
                    score,
                    fields,
                })
                .collect(),
        }))
    }
}

impl EventServiceHandler for Services {
    async fn watch(
        &self,
        req: Request<WatchRequest>,
        stream: WatchServerStream,
    ) -> Result<(), Status> {
        let mut wanted = Vec::with_capacity(req.msg.space_ids.len());
        for id in &req.msg.space_ids {
            wanted.push(parse_id(id, "space_ids")?.to_string());
        }
        let passes = |ev: &Event| match &ev.event {
            Some(event::Event::Object(o)) => wanted.is_empty() || wanted.contains(&o.space_id),
            Some(event::Event::Change(c)) => wanted.is_empty() || wanted.contains(&c.space_id),
            _ => true,
        };
        let mut events = self.store.watch();
        // The client half-closes right after its request; only an error
        // means it went away.
        let mut open = true;
        loop {
            tokio::select! {
                closed = stream.recv(), if open => match closed {
                    Ok(Some(_)) => {}
                    Ok(None) => open = false,
                    Err(_) => return Ok(()),
                },
                ev = events.recv() => match ev {
                    Ok(ev) if passes(&ev) => stream.send(&ev).await?,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        stream
                            .send(&Event {
                                at_ms: now_ms(),
                                event: Some(event::Event::Lagged(Lagged {})),
                            })
                            .await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
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

    async fn retry_change(
        &self,
        req: Request<RetryChangeRequest>,
    ) -> Result<Response<RetryChangeResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        parse_id(&req.msg.id, "id")?;
        let change = self.store.retry_change(space_id, &req.msg.id).await?;
        Ok(Response::new(RetryChangeResponse {
            change: Some(change),
        }))
    }

    async fn discard_change(
        &self,
        req: Request<DiscardChangeRequest>,
    ) -> Result<Response<DiscardChangeResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        parse_id(&req.msg.id, "id")?;
        let discarded = self.store.discard_change(space_id, &req.msg.id).await?;
        Ok(Response::new(DiscardChangeResponse { discarded }))
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
                    downloading: s.downloading.is_some(),
                    target_seq: s.downloading.map_or(0, |(_, t)| t),
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
