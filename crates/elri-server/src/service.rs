//! `SyncService` handlers.

use std::sync::Arc;

use elri_core::blobs::{check_digest, BlobError};
use elri_core::change::{rejected, Outcome, Reason};
use elri_core::model::attachment_sha256;
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
    register_sync_service_arc, DownloadBlobServerStream, SubscribeServerStream, SyncServiceHandler,
    UploadBlobServerStream,
};
use elri_proto::elri::v1::sync::{
    upload_blob_request, AddMemberRequest, AddMemberResponse, BlobChunk, CreateSpaceRequest,
    CreateSpaceResponse, DownloadBlobRequest, HelloRequest, HelloResponse, ListSpacesRequest,
    ListSpacesResponse, PinDefinitionRequest, PinDefinitionResponse, PullRequest, PullResponse,
    PurgeObjectRequest, PurgeObjectResponse, SubmitRequest, SubmitResponse, SubscribeEvent,
    SubscribeRequest, UploadBlobRequest, UploadBlobResponse,
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
        E::NotDeleted(_) | E::Purged(_) | E::EpochMoved { .. } | E::Constraint { .. } => {
            Status::new(Code::FailedPrecondition, e.to_string())
        }
        E::NotPublication(_) => invalid(e.to_string()),
        other => {
            tracing::error!(error = %other, "storage failure");
            Status::new(Code::Internal, "storage failure")
        }
    }
}

fn invalid(msg: impl Into<String>) -> Status {
    Status::new(Code::InvalidArgument, msg)
}

fn blob_status(e: BlobError) -> Status {
    match e {
        BlobError::Hash { .. }
        | BlobError::Size { .. }
        | BlobError::TooLarge
        | BlobError::BadDigest(_) => invalid(e.to_string()),
        other @ BlobError::Io { .. } => {
            tracing::error!(error = %other, "blob storage failure");
            Status::new(Code::Internal, "blob storage failure")
        }
    }
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
            let store = self
                .spaces
                .space(ObjectId::from_uuid(space_id))
                .await
                .map_err(storage)?;
            let last_seq = store.last_seq().await.map_err(storage)?;
            let retention_days =
                i32::try_from(store.retention_days().await.map_err(storage)?).unwrap_or(i32::MAX);
            spaces.push(spb::Space {
                id: s.id.clone(),
                name: s.name.clone(),
                role: role_to_pb(role).into(),
                authority: Authority::Server.into(),
                last_seq,
                retention_days,
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
        // An accepted attachment must name bytes this server holds; a strict
        // constraint may only be set by an Owner or Admin of its Space.
        let blobs = self.blobs.clone();
        let mut managers: Vec<Uuid> = Vec::new();
        for space_id in &spaces {
            if self
                .registry
                .role(space_id.uuid(), user.id)
                .await?
                .is_some_and(Role::can_manage)
            {
                managers.push(space_id.uuid());
            }
        }
        let check = move |o: &elri_proto::elri::v1::object::Object| -> Result<(), String> {
            if let Some(c) = elri_core::model::constraint_of(o) {
                let space = ObjectId::parse(&o.space_id).map_err(|e| e.to_string())?;
                if c.strict && !managers.contains(&space.uuid()) {
                    return Err("a strict constraint needs an owner or admin".to_owned());
                }
            }
            if let Some(sha) = attachment_sha256(o) {
                let space = ObjectId::parse(&o.space_id).map_err(|e| e.to_string())?;
                if !blobs.exists(space, sha) {
                    return Err(format!("attachment {} bytes were not uploaded", o.id));
                }
            }
            Ok(())
        };
        if spaces.len() > 1 {
            // Whole or nothing across Spaces: the coordinator admits it.
            return match self
                .spaces
                .admit_coordinated(&env, &check)
                .await
                .map_err(storage)?
            {
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
        let outcome = space.admit_with(&env, &check).await.map_err(storage)?;
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

    async fn pin_definition(
        &self,
        req: Request<PinDefinitionRequest>,
    ) -> Result<Response<PinDefinitionResponse>, Status> {
        let user = self.user(&req.header).await?;
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let publication = parse_id(&req.msg.publication_id, "publication_id")?;
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
        let pin = space
            .pin_definition(ObjectId::from_uuid(publication), req.msg.expected_epoch)
            .await
            .map_err(storage)?;
        let seq = space.last_seq().await.map_err(storage)?;
        let _ = self.events.send((space_id, seq));
        Ok(Response::new(PinDefinitionResponse { pin: Some(pin) }))
    }

    async fn upload_blob(
        &self,
        stream: UploadBlobServerStream,
    ) -> Result<Response<UploadBlobResponse>, Status> {
        let user = self.user(&stream.header()).await?;
        let Some(UploadBlobRequest {
            chunk: Some(upload_blob_request::Chunk::Header(header)),
        }) = stream.recv().await?
        else {
            return Err(invalid("the first message must be the header"));
        };
        let space_id = parse_id(&header.space_id, "space_id")?;
        if !self.role(space_id, &user).await?.can_write() {
            return Err(Status::new(Code::PermissionDenied, "viewer cannot write"));
        }
        check_digest(&header.sha256).map_err(blob_status)?;
        let space = ObjectId::from_uuid(space_id);
        if self.blobs.exists(space, &header.sha256) {
            // Drain politely; the bytes are already here.
            while stream.recv().await?.is_some() {}
            return Ok(Response::new(UploadBlobResponse { existed: true }));
        }
        let mut writer = self.blobs.begin(space).map_err(blob_status)?;
        while let Some(m) = stream.recv().await? {
            match m.chunk {
                Some(upload_blob_request::Chunk::Data(d)) => {
                    writer.write(&d).map_err(blob_status)?;
                }
                Some(upload_blob_request::Chunk::Header(_)) => {
                    return Err(invalid("a second header"))
                }
                None => {}
            }
        }
        writer
            .finish(Some(&header.sha256), u64::try_from(header.size).ok())
            .map_err(blob_status)?;
        Ok(Response::new(UploadBlobResponse { existed: false }))
    }

    async fn download_blob(
        &self,
        req: Request<DownloadBlobRequest>,
        stream: DownloadBlobServerStream,
    ) -> Result<(), Status> {
        let user = self.user(&req.header).await?;
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        self.role(space_id, &user).await?;
        check_digest(&req.msg.sha256).map_err(blob_status)?;
        let space = ObjectId::from_uuid(space_id);
        if !self.blobs.exists(space, &req.msg.sha256) {
            return Err(Status::new(Code::NotFound, "no such attachment bytes"));
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);
        let blobs = self.blobs.clone();
        let sha = req.msg.sha256.clone();
        let reader = tokio::task::spawn_blocking(move || {
            blobs.read_chunks(space, &sha, |chunk| {
                tx.blocking_send(chunk.to_vec())
                    .map_err(|_| BlobError::TooLarge)
            })
        });
        while let Some(data) = rx.recv().await {
            stream.send(&BlobChunk { data: data.into() }).await?;
        }
        match reader.await {
            Ok(Ok(()) | Err(BlobError::TooLarge)) => Ok(()),
            Ok(Err(e)) => Err(blob_status(e)),
            Err(e) => Err(Status::new(Code::Internal, e.to_string())),
        }
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
