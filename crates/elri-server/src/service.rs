//! `SyncService` handlers.

use std::sync::Arc;

use elri_core::change::{rejected, Outcome, Reason};
use elri_core::model::ObjectId;
use elri_core::sync::{envelope_space, validate_envelope};
use elri_proto::elri::v1::space::{self as spb, Authority};
use elri_proto::elri::v1::sync::sync_service_ws::{
    register_sync_service_arc, SubscribeServerStream, SyncServiceHandler,
};
use elri_proto::elri::v1::sync::{
    AddMemberRequest, AddMemberResponse, CreateSpaceRequest, CreateSpaceResponse, HelloRequest,
    HelloResponse, ListSpacesRequest, ListSpacesResponse, PullRequest, PullResponse, SubmitRequest,
    SubmitResponse, SubscribeEvent, SubscribeRequest,
};
use uuid::Uuid;
use ws_proto::{Code, Metadata, Request, Response, Server, Status};

use crate::store::{Role, ServerStoreError, User};
use crate::State;

/// Protocol version this server speaks.
pub const PROTOCOL_VERSION: u32 = 1;

pub(crate) fn register(server: &Server, state: Arc<State>) {
    register_sync_service_arc(server, state);
}

impl From<ServerStoreError> for Status {
    fn from(e: ServerStoreError) -> Self {
        match e {
            ServerStoreError::UserNotFound(_) => Status::new(Code::NotFound, e.to_string()),
            ServerStoreError::UserExists(_) => Status::new(Code::AlreadyExists, e.to_string()),
            other => {
                tracing::error!(error = %other, "registry failure");
                Status::new(Code::Internal, "registry failure")
            }
        }
    }
}

fn storage(e: elri_core::store::StoreError) -> Status {
    use elri_core::store::StoreError as E;
    match e {
        E::SpaceNotFound(_) | E::ObjectNotFound(_) => Status::new(Code::NotFound, e.to_string()),
        other => {
            tracing::error!(error = %other, "storage failure");
            Status::new(Code::Internal, "storage failure")
        }
    }
}

fn invalid(msg: impl Into<String>) -> Status {
    Status::new(Code::InvalidArgument, msg)
}

fn parse_id(s: &str, what: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s).map_err(|_| invalid(format!("{what}: not a valid id")))
}

impl State {
    async fn user(&self, header: &Metadata) -> Result<User, Status> {
        let token = header
            .get("authorization")
            .and_then(|h| h.strip_prefix("Bearer "))
            .ok_or_else(|| Status::new(Code::Unauthenticated, "missing token"))?;
        self.registry
            .authenticate(token)
            .await?
            .ok_or_else(|| Status::new(Code::Unauthenticated, "invalid token"))
    }

    async fn role(&self, space: Uuid, user: &User) -> Result<Role, Status> {
        self.registry
            .role(space, user.id)
            .await?
            .ok_or_else(|| Status::new(Code::PermissionDenied, "not a member of this space"))
    }
}

fn role_to_pb(r: Role) -> spb::Role {
    match r {
        Role::Owner => spb::Role::Owner,
        Role::Admin => spb::Role::Admin,
        Role::Editor => spb::Role::Editor,
        Role::Viewer => spb::Role::Viewer,
    }
}

impl SyncServiceHandler for State {
    async fn hello(&self, req: Request<HelloRequest>) -> Result<Response<HelloResponse>, Status> {
        let user = self.user(&req.header).await?;
        if req.msg.protocol_version != PROTOCOL_VERSION {
            return Err(Status::new(
                Code::FailedPrecondition,
                format!("protocol {PROTOCOL_VERSION} required"),
            ));
        }
        Ok(Response::new(HelloResponse {
            user_id: user.id.to_string(),
            user_name: user.name,
            protocol_version: PROTOCOL_VERSION,
        }))
    }

    async fn list_spaces(
        &self,
        req: Request<ListSpacesRequest>,
    ) -> Result<Response<ListSpacesResponse>, Status> {
        let user = self.user(&req.header).await?;
        let memberships = self.registry.memberships(user.id).await?;
        let known = self.spaces.list_spaces().await.map_err(storage)?;
        let spaces = memberships
            .into_iter()
            .filter_map(|(space_id, role)| {
                let id = space_id.to_string();
                known.iter().find(|s| s.id == id).map(|s| spb::Space {
                    id: s.id.clone(),
                    name: s.name.clone(),
                    role: role_to_pb(role).into(),
                    authority: Authority::Server.into(),
                })
            })
            .collect();
        Ok(Response::new(ListSpacesResponse { spaces }))
    }

    async fn create_space(
        &self,
        req: Request<CreateSpaceRequest>,
    ) -> Result<Response<CreateSpaceResponse>, Status> {
        let user = self.user(&req.header).await?;
        let name = req.msg.name.trim();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        let id = ObjectId::new();
        let mut space = self
            .spaces
            .register_space(id, name, Authority::Local)
            .await
            .map_err(storage)?;
        self.registry
            .set_role(id.uuid(), user.id, Role::Owner)
            .await?;
        space.authority = Authority::Server.into();
        Ok(Response::new(CreateSpaceResponse { space: Some(space) }))
    }

    async fn add_member(
        &self,
        req: Request<AddMemberRequest>,
    ) -> Result<Response<AddMemberResponse>, Status> {
        let user = self.user(&req.header).await?;
        let space = parse_id(&req.msg.space_id, "space_id")?;
        if !self.role(space, &user).await?.can_manage() {
            return Err(Status::new(
                Code::PermissionDenied,
                "owner or admin required",
            ));
        }
        let role = match spb::Role::try_from(req.msg.role) {
            Ok(spb::Role::Admin) => Role::Admin,
            Ok(spb::Role::Editor) => Role::Editor,
            Ok(spb::Role::Viewer) => Role::Viewer,
            Ok(spb::Role::Owner) => return Err(invalid("ownership is transferred, not granted")),
            _ => return Err(invalid("role is required")),
        };
        let member = self
            .registry
            .user_by_name(&req.msg.user_name)
            .await?
            .ok_or_else(|| Status::new(Code::NotFound, "user not found"))?;
        self.registry.set_role(space, member.id, role).await?;
        Ok(Response::new(AddMemberResponse {}))
    }

    async fn submit(
        &self,
        req: Request<SubmitRequest>,
    ) -> Result<Response<SubmitResponse>, Status> {
        // Authority comes from the token. The device id inside the envelope is
        // a label the client chose; it is recorded for attribution, never
        // trusted for authorization.
        let user = self.user(&req.header).await?;
        let env = req
            .msg
            .change
            .ok_or_else(|| invalid("change is required"))?;
        let reply = |outcome: Outcome| {
            Ok(Response::new(SubmitResponse {
                outcome: Some(outcome),
            }))
        };
        if let Err(e) = validate_envelope(&env) {
            return reply(rejected(Reason::Invalid, e.to_string()));
        }
        let space_id = envelope_space(&env).map_err(|e| invalid(e.to_string()))?;
        let Some(role) = self.registry.role(space_id.uuid(), user.id).await? else {
            return reply(rejected(Reason::PermissionDenied, "not a member"));
        };
        if !role.can_write() {
            return reply(rejected(Reason::PermissionDenied, "viewer cannot write"));
        }
        let space = self.spaces.space(space_id).await.map_err(storage)?;
        let outcome = space.admit(&env).await.map_err(storage)?;
        if let Outcome::AcceptedSeq(seq) = outcome {
            let _ = self.events.send((space_id.uuid(), seq));
        }
        reply(outcome)
    }

    async fn pull(&self, req: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let user = self.user(&req.header).await?;
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        self.role(space_id, &user).await?;
        let limit = usize::try_from(req.msg.limit).unwrap_or(0);
        let limit = if limit == 0 { 100 } else { limit.min(500) };
        let space = self
            .spaces
            .space(ObjectId::from_uuid(space_id))
            .await
            .map_err(storage)?;
        let entries = space
            .log_after(req.msg.after_seq, limit + 1)
            .await
            .map_err(storage)?;
        let has_more = entries.len() > limit;
        let entries = entries.into_iter().take(limit).collect();
        Ok(Response::new(PullResponse { entries, has_more }))
    }

    async fn subscribe(
        &self,
        req: Request<SubscribeRequest>,
        stream: SubscribeServerStream,
    ) -> Result<(), Status> {
        let user = self.user(&req.header).await?;
        let mut wanted = Vec::with_capacity(req.msg.space_ids.len());
        for id in &req.msg.space_ids {
            let space = parse_id(id, "space_ids")?;
            self.role(space, &user).await?;
            wanted.push(space);
        }
        let mut events = self.events.subscribe();
        loop {
            tokio::select! {
                closed = stream.recv() => {
                    // The client half-closed or went away: stop.
                    if matches!(closed, Ok(None) | Err(_)) {
                        return Ok(());
                    }
                }
                ev = events.recv() => match ev {
                    Ok((space, seq)) if wanted.contains(&space) => {
                        stream
                            .send(&SubscribeEvent { space_id: space.to_string(), seq })
                            .await?;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // The client will catch up by pulling after its cursor.
                        for space in &wanted {
                            let s = self.spaces.space(ObjectId::from_uuid(*space)).await.map_err(storage)?;
                            let seq = s.last_seq().await.map_err(storage)?;
                            stream.send(&SubscribeEvent { space_id: space.to_string(), seq }).await?;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                },
            }
        }
    }
}
