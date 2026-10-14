//! `SyncService` handlers.

use std::sync::Arc;

use elri_core::change::{rejected, Outcome, Reason};
use elri_core::model::ObjectId;
use elri_core::sync::{envelope_space, envelope_spaces, validate_envelope};
use elri_proto::elri::v1::account::account_service_ws::{
    register_account_service_arc, AccountServiceHandler,
};
use elri_proto::elri::v1::account::admin_service_ws::{
    register_admin_service_arc, AdminServiceHandler,
};
use elri_proto::elri::v1::account::{
    self as apb, CreateUserRequest, CreateUserResponse, IssueTokenRequest, IssueTokenResponse,
    IssueUserTokenRequest, IssueUserTokenResponse, ListTokensRequest, ListTokensResponse,
    ListUserTokensRequest, ListUserTokensResponse, ListUsersRequest, ListUsersResponse,
    RevokeTokenRequest, RevokeTokenResponse, RevokeUserTokenRequest, RevokeUserTokenResponse,
    SetAdminRequest, SetAdminResponse, WhoAmIRequest, WhoAmIResponse,
};
use elri_proto::elri::v1::space::{self as spb, Authority};
use elri_proto::elri::v1::sync::sync_service_ws::{
    register_sync_service_arc, SubscribeServerStream, SyncServiceHandler,
};
use elri_proto::elri::v1::sync::{
    AddMemberRequest, AddMemberResponse, CreateSpaceRequest, CreateSpaceResponse, HelloRequest,
    HelloResponse, ListSpacesRequest, ListSpacesResponse, PullRequest, PullResponse,
    PurgeObjectRequest, PurgeObjectResponse, SubmitRequest, SubmitResponse, SubscribeEvent,
    SubscribeRequest,
};
use uuid::Uuid;
use ws_proto::{Code, Metadata, Request, Response, Server, Status};

use crate::store::{Auth, Role, ServerStoreError, Token, User};
use crate::State;

/// Protocol version this server speaks.
pub const PROTOCOL_VERSION: u32 = 1;

pub(crate) fn register(server: &Server, state: Arc<State>) {
    register_sync_service_arc(server, Arc::clone(&state));
    register_account_service_arc(server, Arc::clone(&state));
    register_admin_service_arc(server, state);
}

impl From<ServerStoreError> for Status {
    fn from(e: ServerStoreError) -> Self {
        match e {
            ServerStoreError::UserNotFound(_) | ServerStoreError::TokenNotFound(_) => {
                Status::new(Code::NotFound, e.to_string())
            }
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
        E::NotDeleted(_) | E::Purged(_) => Status::new(Code::FailedPrecondition, e.to_string()),
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
    async fn auth(&self, header: &Metadata) -> Result<Auth, Status> {
        let token = header
            .get("authorization")
            .and_then(|h| h.strip_prefix("Bearer "))
            .ok_or_else(|| Status::new(Code::Unauthenticated, "missing token"))?;
        self.registry
            .authenticate(token)
            .await?
            .ok_or_else(|| Status::new(Code::Unauthenticated, "invalid token"))
    }

    async fn user(&self, header: &Metadata) -> Result<User, Status> {
        Ok(self.auth(header).await?.user)
    }

    async fn admin(&self, header: &Metadata) -> Result<User, Status> {
        let user = self.user(header).await?;
        if !user.admin {
            return Err(Status::new(Code::PermissionDenied, "admin required"));
        }
        Ok(user)
    }

    async fn named_user(&self, name: &str) -> Result<User, Status> {
        self.registry
            .user_by_name(name)
            .await?
            .ok_or_else(|| Status::new(Code::NotFound, "user not found"))
    }

    /// A token the caller may manage: their own, or any for an admin.
    async fn owned_token(&self, auth: &Auth, id: &str) -> Result<Token, Status> {
        let id = parse_id(id, "id")?;
        let token = self.registry.token(id).await?;
        if token.user_id != auth.user.id && !auth.user.admin {
            return Err(Status::new(Code::NotFound, "token not found"));
        }
        Ok(token)
    }

    async fn role(&self, space: Uuid, user: &User) -> Result<Role, Status> {
        self.registry
            .role(space, user.id)
            .await?
            .ok_or_else(|| Status::new(Code::PermissionDenied, "not a member of this space"))
    }
}

fn user_to_pb(u: &User) -> apb::User {
    apb::User {
        id: u.id.to_string(),
        name: u.name.clone(),
        admin: u.admin,
        created_at_ms: u.created_at,
    }
}

fn token_to_pb(t: &Token) -> apb::Token {
    apb::Token {
        id: t.id.to_string(),
        user_id: t.user_id.to_string(),
        label: t.label.clone(),
        created_at_ms: t.created_at,
        last_used_at_ms: t.last_used_at,
        last_device_id: t.last_device_id.clone(),
        revoked_at_ms: t.revoked_at,
    }
}

fn require_name(name: &str) -> Result<&str, Status> {
    let n = name.trim();
    if n.is_empty() {
        return Err(invalid("name is required"));
    }
    Ok(n)
}

impl AccountServiceHandler for State {
    async fn who_am_i(
        &self,
        req: Request<WhoAmIRequest>,
    ) -> Result<Response<WhoAmIResponse>, Status> {
        let auth = self.auth(&req.header).await?;
        let token = self.registry.token(auth.token_id).await?;
        Ok(Response::new(WhoAmIResponse {
            user: Some(user_to_pb(&auth.user)),
            token: Some(token_to_pb(&token)),
        }))
    }

    async fn list_tokens(
        &self,
        req: Request<ListTokensRequest>,
    ) -> Result<Response<ListTokensResponse>, Status> {
        let user = self.user(&req.header).await?;
        let tokens = self
            .registry
            .tokens(user.id, req.msg.include_revoked)
            .await?;
        Ok(Response::new(ListTokensResponse {
            tokens: tokens.iter().map(token_to_pb).collect(),
        }))
    }

    async fn issue_token(
        &self,
        req: Request<IssueTokenRequest>,
    ) -> Result<Response<IssueTokenResponse>, Status> {
        let user = self.user(&req.header).await?;
        let (token, secret) = self.registry.issue_token(user.id, &req.msg.label).await?;
        Ok(Response::new(IssueTokenResponse {
            token: Some(token_to_pb(&token)),
            secret,
        }))
    }

    async fn revoke_token(
        &self,
        req: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        let auth = self.auth(&req.header).await?;
        let token = self.owned_token(&auth, &req.msg.id).await?;
        self.registry.revoke_token(token.id).await?;
        Ok(Response::new(RevokeTokenResponse {}))
    }
}

impl AdminServiceHandler for State {
    async fn create_user(
        &self,
        req: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        self.admin(&req.header).await?;
        let name = require_name(&req.msg.name)?;
        let (user, token, secret) = self
            .registry
            .create_user(name, req.msg.admin, &req.msg.label)
            .await?;
        Ok(Response::new(CreateUserResponse {
            user: Some(user_to_pb(&user)),
            token: Some(token_to_pb(&token)),
            secret,
        }))
    }

    async fn list_users(
        &self,
        req: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        self.admin(&req.header).await?;
        let users = self.registry.users().await?;
        Ok(Response::new(ListUsersResponse {
            users: users.iter().map(user_to_pb).collect(),
        }))
    }

    async fn set_admin(
        &self,
        req: Request<SetAdminRequest>,
    ) -> Result<Response<SetAdminResponse>, Status> {
        let me = self.admin(&req.header).await?;
        let target = self.named_user(&req.msg.user_name).await?;
        if target.id == me.id && !req.msg.admin {
            return Err(Status::new(
                Code::FailedPrecondition,
                "an admin cannot demote themself",
            ));
        }
        let user = self.registry.set_admin(target.id, req.msg.admin).await?;
        Ok(Response::new(SetAdminResponse {
            user: Some(user_to_pb(&user)),
        }))
    }

    async fn issue_user_token(
        &self,
        req: Request<IssueUserTokenRequest>,
    ) -> Result<Response<IssueUserTokenResponse>, Status> {
        self.admin(&req.header).await?;
        let target = self.named_user(&req.msg.user_name).await?;
        let (token, secret) = self.registry.issue_token(target.id, &req.msg.label).await?;
        Ok(Response::new(IssueUserTokenResponse {
            token: Some(token_to_pb(&token)),
            secret,
        }))
    }

    async fn list_user_tokens(
        &self,
        req: Request<ListUserTokensRequest>,
    ) -> Result<Response<ListUserTokensResponse>, Status> {
        self.admin(&req.header).await?;
        let target = self.named_user(&req.msg.user_name).await?;
        let tokens = self
            .registry
            .tokens(target.id, req.msg.include_revoked)
            .await?;
        Ok(Response::new(ListUserTokensResponse {
            tokens: tokens.iter().map(token_to_pb).collect(),
        }))
    }

    async fn revoke_user_token(
        &self,
        req: Request<RevokeUserTokenRequest>,
    ) -> Result<Response<RevokeUserTokenResponse>, Status> {
        self.admin(&req.header).await?;
        let id = parse_id(&req.msg.id, "id")?;
        self.registry.revoke_token(id).await?;
        Ok(Response::new(RevokeUserTokenResponse {}))
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
        let auth = self.auth(&req.header).await?;
        if req.msg.protocol_version != PROTOCOL_VERSION {
            return Err(Status::new(
                Code::FailedPrecondition,
                format!("protocol {PROTOCOL_VERSION} required"),
            ));
        }
        if !req.msg.device_id.is_empty() {
            self.registry
                .note_device(auth.token_id, &req.msg.device_id)
                .await?;
        }
        Ok(Response::new(HelloResponse {
            user_id: auth.user.id.to_string(),
            user_name: auth.user.name,
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
        let mut spaces = Vec::with_capacity(memberships.len());
        for (space_id, role) in memberships {
            let id = space_id.to_string();
            let Some(s) = known.iter().find(|s| s.id == id) else {
                continue;
            };
            let last_seq = self
                .spaces
                .space(ObjectId::from_uuid(space_id))
                .await
                .map_err(storage)?
                .last_seq()
                .await
                .map_err(storage)?;
            spaces.push(spb::Space {
                id: s.id.clone(),
                name: s.name.clone(),
                role: role_to_pb(role).into(),
                authority: Authority::Server.into(),
                last_seq,
            });
        }
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
        let spaces = envelope_spaces(&env).map_err(|e| invalid(e.to_string()))?;
        for space_id in &spaces {
            let Some(role) = self.registry.role(space_id.uuid(), user.id).await? else {
                return reply(rejected(
                    Reason::PermissionDenied,
                    format!("not a member of space {space_id}"),
                ));
            };
            if !role.can_write() {
                return reply(rejected(
                    Reason::PermissionDenied,
                    format!("viewer cannot write to space {space_id}"),
                ));
            }
        }
        if spaces.len() > 1 {
            // Whole or nothing across Spaces: the coordinator admits it.
            return match self.spaces.admit_coordinated(&env).await.map_err(storage)? {
                Ok(seqs) => {
                    let mut first = 0;
                    for (space_id, seq) in &seqs {
                        let _ = self.events.send((space_id.uuid(), *seq));
                        if *space_id == envelope_space(&env).map_err(|e| invalid(e.to_string()))? {
                            first = *seq;
                        }
                    }
                    reply(Outcome::AcceptedSeq(first))
                }
                Err(outcome) => reply(outcome),
            };
        }
        let space_id = spaces[0];
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

    async fn purge_object(
        &self,
        req: Request<PurgeObjectRequest>,
    ) -> Result<Response<PurgeObjectResponse>, Status> {
        let user = self.user(&req.header).await?;
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let object_id = parse_id(&req.msg.object_id, "object_id")?;
        if !self.role(space_id, &user).await?.can_manage() {
            return Err(Status::new(
                Code::PermissionDenied,
                "owner or admin required",
            ));
        }
        let space = self
            .spaces
            .space(ObjectId::from_uuid(space_id))
            .await
            .map_err(storage)?;
        let seq = space
            .purge_object(ObjectId::from_uuid(object_id))
            .await
            .map_err(storage)?;
        let _ = self.events.send((space_id, seq));
        Ok(Response::new(PurgeObjectResponse { seq }))
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
