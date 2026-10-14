//! The device's connection to its server and the replication workers that
//! keep joined Spaces in step: push the outbox, pull after the cursor, and
//! wake up on local writes and server announcements.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use elri_proto::elri::v1::object::ObjectKind;
use elri_proto::elri::v1::space::{Authority, Space as PbSpace};
use elri_proto::elri::v1::sync::sync_service_ws::{SubscribeClientStream, SyncServiceClient};
use elri_proto::elri::v1::sync::{
    submit_chunked_request, upload_blob_request, AddMemberRequest, BlobHeader, CreateSpaceRequest,
    DownloadBlobRequest, FetchEntryRequest, HelloRequest, ListSpacesRequest, PinDefinitionRequest,
    PullRequest, PurgeObjectRequest, SubmitChunkedRequest, SubmitRequest, SubmitResponse,
    SubscribeRequest, UploadBlobRequest,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use ws_proto::{CallOptions, Client, ClientConfig, Code, Reconnect, Request, Status};

use crate::change::{Outcome, Reason};
use crate::model::{ObjectId, SpaceId};
use crate::net::Net;
use crate::secrets::{Secrets, SecretsError};
use crate::store::{RemoteConfig, SpaceStore, Store, StoreError};

/// Secret-store key of the server token.
const TOKEN_KEY: &str = "remote-token";

/// Protocol version this core speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// Errors from the remote connection.
#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    /// No server configured.
    #[error("no server configured")]
    NotConfigured,
    /// The server answered with an error.
    #[error(transparent)]
    Rpc(#[from] Status),
    /// Local storage failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The secret store failed.
    #[error(transparent)]
    Secrets(#[from] SecretsError),
    /// A server is configured but its token is gone from the secret store.
    #[error("server token missing from the secret store; connect again")]
    TokenMissing,
    /// The server's answer could not be understood.
    #[error("bad reply from server: {0}")]
    BadReply(&'static str),
    /// Attachment bytes failed on the way.
    #[error(transparent)]
    Blob(#[from] crate::blobs::BlobError),
    /// A record could not be understood.
    #[error(transparent)]
    Model(#[from] crate::model::ModelError),
}

struct Session {
    url: String,
    token: String,
    user_id: String,
    user_name: String,
    client: Client,
    workers: HashMap<SpaceId, JoinHandle<()>>,
    /// Re-dials when a connection without its own reconnect ends.
    supervisor: Option<JoinHandle<()>>,
    /// Submits Changes that span Spaces from the coordinator journal.
    coordinator: JoinHandle<()>,
}

/// The device's server link.
pub struct Remote {
    store: Arc<Store>,
    device_id: String,
    secrets: Secrets,
    net: Arc<Net>,
    session: Mutex<Option<Session>>,
}

/// Per-Space replication status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceStatus {
    /// Space.
    pub space_id: SpaceId,
    /// Highest server sequence installed locally.
    pub cursor: i64,
    /// `(received, target)` while the initial download is incomplete.
    pub downloading: Option<(i64, i64)>,
    /// Local Changes not yet accepted.
    pub pending: usize,
    /// Rejected Changes awaiting the user.
    pub rejected: usize,
}

/// Overall status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStatus {
    /// A server is configured.
    pub configured: bool,
    /// Its URL.
    pub url: String,
    /// User name on the server.
    pub user_name: String,
    /// A socket is currently up.
    pub connected: bool,
    /// Joined Spaces.
    pub spaces: Vec<SpaceStatus>,
}

fn authed<T>(token: &str, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {token}"))
}

impl Remote {
    /// Creates the link; nothing connects until [`Remote::resume`] or
    /// [`Remote::connect`].
    pub fn new(store: Arc<Store>, device_id: String, secrets: Secrets, net: Arc<Net>) -> Self {
        Self {
            store,
            device_id,
            secrets,
            net,
            session: Mutex::new(None),
        }
    }

    /// The stored configuration with its token filled in from the secret
    /// store.
    async fn config(&self) -> Result<Option<RemoteConfig>, RemoteError> {
        let Some(mut cfg) = self.store.remote().await? else {
            return Ok(None);
        };
        cfg.token = self
            .secrets
            .get(TOKEN_KEY)
            .await?
            .ok_or(RemoteError::TokenMissing)?;
        Ok(Some(cfg))
    }

    /// Reconnects to a previously configured server, if any. Failures are
    /// logged, not returned: the device works offline.
    pub async fn resume(self: &Arc<Self>) {
        let cfg = match self.config().await {
            Ok(Some(cfg)) => cfg,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, "cannot read remote configuration");
                return;
            }
        };
        if let Err(e) = self.open_session(cfg).await {
            tracing::warn!(error = %e, "remote resume failed; will retry on next connect");
        }
    }

    /// Verifies `url` and `token` with the server, stores them, and starts
    /// replicating every joined Space.
    pub async fn connect(
        self: &Arc<Self>,
        url: &str,
        token: &str,
    ) -> Result<RemoteConfig, RemoteError> {
        let (client, _) = dial(&self.net, url).await?;
        let hello = SyncServiceClient::new(client.clone())
            .hello(authed(
                token,
                HelloRequest {
                    device_id: self.device_id.clone(),
                    protocol_version: PROTOCOL_VERSION,
                },
            ))
            .await?
            .msg;
        let cfg = RemoteConfig {
            url: url.to_owned(),
            token: token.to_owned(),
            user_id: hello.user_id,
            user_name: hello.user_name,
        };
        self.secrets.set(TOKEN_KEY, token).await?;
        self.store.set_remote(&cfg).await?;
        self.close_session().await;
        self.install_session(cfg.clone(), client).await?;
        Ok(cfg)
    }

    /// Stops replicating without forgetting the server.
    pub async fn close(&self) {
        self.close_session().await;
    }

    /// Forgets the server and stops replicating. Spaces stay on the device.
    pub async fn disconnect(&self) -> Result<(), RemoteError> {
        self.close_session().await;
        self.store.clear_remote().await?;
        self.secrets.delete(TOKEN_KEY).await?;
        Ok(())
    }

    async fn open_session(self: &Arc<Self>, cfg: RemoteConfig) -> Result<(), RemoteError> {
        let (client, _) = dial(&self.net, &cfg.url).await?;
        self.install_session(cfg, client).await
    }

    /// Re-dials after a connection that cannot reconnect by itself (one
    /// tunnelled through a proxy) has ended, with backoff, until a
    /// session is up again or the session is closed.
    fn supervise(self: &Arc<Self>, client: Client, cfg: RemoteConfig) -> JoinHandle<()> {
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            client.closed_for_good().await;
            let mut wait = Duration::from_millis(500);
            loop {
                tokio::time::sleep(wait).await;
                let Some(me) = me.upgrade() else { return };
                match dial(&me.net, &cfg.url).await {
                    Ok((client, _)) => {
                        // Replace the session without aborting this task
                        // from inside itself.
                        let old = me.session.lock().await.take();
                        if let Some(s) = old {
                            for (_, w) in s.workers {
                                w.abort();
                            }
                            s.coordinator.abort();
                        }
                        if let Err(e) = me.install_session(cfg.clone(), client).await {
                            tracing::warn!(error = %e, "re-established link could not start");
                        }
                        return;
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "server still unreachable");
                        wait = (wait * 2).min(Duration::from_secs(30));
                    }
                }
            }
        })
    }

    async fn install_session(
        self: &Arc<Self>,
        cfg: RemoteConfig,
        client: Client,
    ) -> Result<(), RemoteError> {
        let coordinator =
            spawn_coordinator(Arc::clone(&self.store), client.clone(), cfg.token.clone());
        let supervisor = Some(self.supervise(client.clone(), cfg.clone()));
        let mut session = Session {
            url: cfg.url,
            token: cfg.token,
            user_id: cfg.user_id,
            user_name: cfg.user_name,
            client,
            workers: HashMap::new(),
            coordinator,
            supervisor,
        };
        for info in self.store.list_spaces().await? {
            if info.authority() == Authority::Server {
                let id = ObjectId::parse(&info.id).map_err(StoreError::from)?;
                let space = self.store.space(id).await?;
                session.workers.insert(
                    id,
                    spawn_worker(id, space, session.client.clone(), session.token.clone()),
                );
            }
        }
        *self.session.lock().await = Some(session);
        self.store.emit_remote();
        Ok(())
    }

    async fn close_session(&self) {
        if let Some(s) = self.session.lock().await.take() {
            for (_, w) in s.workers {
                w.abort();
            }
            s.coordinator.abort();
            if let Some(sup) = s.supervisor {
                sup.abort();
            }
            s.client.close();
            self.store.emit_remote();
        }
    }

    async fn with_session<R>(&self, f: impl FnOnce(&Session) -> R) -> Result<R, RemoteError> {
        let guard = self.session.lock().await;
        guard.as_ref().map(f).ok_or(RemoteError::NotConfigured)
    }

    /// Current status.
    pub async fn status(&self) -> Result<RemoteStatus, RemoteError> {
        let guard = self.session.lock().await;
        let mut spaces = Vec::new();
        for info in self.store.list_spaces().await? {
            if info.authority() == Authority::Server {
                let id = ObjectId::parse(&info.id).map_err(StoreError::from)?;
                let space = self.store.space(id).await?;
                let (pending, rejected) = space.pending_counts().await?;
                spaces.push(SpaceStatus {
                    space_id: id,
                    cursor: space.cursor().await?,
                    downloading: space.downloading().await?,
                    pending,
                    rejected,
                });
            }
        }
        if let Some(s) = guard.as_ref() {
            return Ok(RemoteStatus {
                configured: true,
                url: s.url.clone(),
                user_name: s.user_name.clone(),
                connected: s.client.is_connected(),
                spaces,
            });
        }
        let cfg = self.store.remote().await?;
        Ok(RemoteStatus {
            configured: cfg.is_some(),
            url: cfg.as_ref().map(|c| c.url.clone()).unwrap_or_default(),
            user_name: cfg.map(|c| c.user_name).unwrap_or_default(),
            connected: false,
            spaces,
        })
    }

    /// Spaces visible on the server, with whether each is joined here.
    pub async fn list_remote_spaces(&self) -> Result<Vec<(PbSpace, bool)>, RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let res = SyncServiceClient::new(client)
            .list_spaces(authed(&token, ListSpacesRequest {}))
            .await?;
        let local = self.store.list_spaces().await?;
        Ok(res
            .msg
            .spaces
            .into_iter()
            .map(|s| {
                let joined = local.iter().any(|l| l.id == s.id);
                (s, joined)
            })
            .collect())
    }

    /// Creates a Space on the server and joins it.
    pub async fn create_remote_space(self: &Arc<Self>, name: &str) -> Result<PbSpace, RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let space = SyncServiceClient::new(client)
            .create_space(authed(
                &token,
                CreateSpaceRequest {
                    name: name.to_owned(),
                },
            ))
            .await?
            .msg
            .space
            .ok_or(RemoteError::BadReply("no space"))?;
        self.join(&space).await?;
        Ok(space)
    }

    /// Replicates a server Space to this device.
    pub async fn join_space(self: &Arc<Self>, space_id: SpaceId) -> Result<PbSpace, RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let res = SyncServiceClient::new(client)
            .list_spaces(authed(&token, ListSpacesRequest {}))
            .await?;
        let space = res
            .msg
            .spaces
            .into_iter()
            .find(|s| s.id == space_id.to_string())
            .ok_or_else(|| Status::new(Code::NotFound, "space not visible on the server"))?;
        self.join(&space).await?;
        Ok(space)
    }

    async fn join(self: &Arc<Self>, space: &PbSpace) -> Result<(), RemoteError> {
        let id = ObjectId::parse(&space.id).map_err(|_| RemoteError::BadReply("space id"))?;
        if self.store.space(id).await.is_err() {
            self.store
                .register_space(id, &space.name, Authority::Server)
                .await?;
            // Read-only until everything the server had at join is here.
            self.store
                .space(id)
                .await?
                .set_initial_target(space.last_seq)
                .await?;
        }
        let store = self.store.space(id).await?;
        let mut guard = self.session.lock().await;
        let session = guard.as_mut().ok_or(RemoteError::NotConfigured)?;
        if !session.workers.contains_key(&id) {
            session.workers.insert(
                id,
                spawn_worker(id, store, session.client.clone(), session.token.clone()),
            );
        }
        Ok(())
    }

    /// Owner/Admin: adds a member to a server Space.
    pub async fn add_member(
        &self,
        space_id: SpaceId,
        user_name: &str,
        role: i32,
    ) -> Result<(), RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        SyncServiceClient::new(client)
            .add_member(authed(
                &token,
                AddMemberRequest {
                    space_id: space_id.to_string(),
                    user_name: user_name.to_owned(),
                    role,
                },
            ))
            .await?;
        Ok(())
    }

    /// Owner/Admin: asks the server to erase a deleted Object for good. The
    /// erasure reaches this device with the log like any other entry.
    pub async fn purge_object(
        &self,
        space_id: SpaceId,
        object_id: ObjectId,
    ) -> Result<i64, RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let res = SyncServiceClient::new(client)
            .purge_object(authed(
                &token,
                PurgeObjectRequest {
                    space_id: space_id.to_string(),
                    object_id: object_id.to_string(),
                },
            ))
            .await?;
        Ok(res.msg.seq)
    }

    /// Owner/Admin: asks the server to pin a publication. The pin reaches
    /// this device with the log.
    pub async fn pin_definition(
        &self,
        space_id: SpaceId,
        publication: ObjectId,
        expected_epoch: i64,
    ) -> Result<elri_proto::elri::v1::definition::Pin, RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let res = SyncServiceClient::new(client)
            .pin_definition(authed(
                &token,
                PinDefinitionRequest {
                    space_id: space_id.to_string(),
                    publication_id: publication.to_string(),
                    expected_epoch,
                },
            ))
            .await?;
        res.msg.pin.ok_or(RemoteError::BadReply("no pin"))
    }

    /// Fetches attachment bytes of a server Space into the local blob
    /// store, checking the digest and size on the way.
    pub async fn fetch_blob(
        &self,
        space_id: SpaceId,
        sha256: &str,
        size: i64,
    ) -> Result<(), RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let store = self.store.space(space_id).await?;
        fetch_blob(&client, &token, &store, sha256, size).await
    }

    /// Runs one replication round for `space_id` right now and waits for it
    /// to finish; for tests and explicit "sync now".
    pub async fn sync_now(&self, space_id: SpaceId) -> Result<(), RemoteError> {
        let (client, token) = self
            .with_session(|s| (s.client.clone(), s.token.clone()))
            .await?;
        let space = self.store.space(space_id).await?;
        replicate_once(space_id, &space, &client, &token).await
    }

    /// User id on the server, when connected.
    pub async fn user_id(&self) -> Option<String> {
        self.session
            .lock()
            .await
            .as_ref()
            .map(|s| s.user_id.clone())
    }
}

/// Dials the server: directly with ws-proto's own reconnect, or through a
/// tunnel when the device's proxy applies to the server's host. Returns
/// whether the connection is tunnelled.
async fn dial(net: &Net, url: &str) -> Result<(Client, bool), RemoteError> {
    let parsed = reqwest::Url::parse(url)
        .map_err(|_| RemoteError::Rpc(Status::new(Code::InvalidArgument, "server url")))?;
    let host = parsed.host_str().unwrap_or_default().to_owned();
    let tls = parsed.scheme() == "wss";
    let port = parsed.port().unwrap_or(if tls { 443 } else { 80 });
    let loopback = host == "127.0.0.1" || host == "localhost" || host == "::1";
    let Some(proxy) = (!loopback).then(|| net.tunnel_proxy(&host)).flatten() else {
        let cfg = ClientConfig {
            reconnect: Some(Reconnect {
                initial: Duration::from_millis(200),
                max: Duration::from_secs(30),
            }),
            ..ClientConfig::default()
        };
        return Ok((Client::dial(url, cfg).await?, false));
    };
    let unavailable = |e: String| RemoteError::Rpc(Status::new(Code::Unavailable, e));
    let stream = crate::net::tunnel(&proxy, &host, port)
        .await
        .map_err(|e| unavailable(format!("proxy {}: {e}", crate::net::redacted(&proxy))))?;
    let mut req = url
        .into_client_request()
        .map_err(|e| unavailable(format!("dial: {e}")))?;
    req.headers_mut().insert(
        "sec-websocket-protocol",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static(ws_proto::SUBPROTOCOL),
    );
    let (ws, resp) = tokio_tungstenite::client_async_tls_with_config(req, stream, None, None)
        .await
        .map_err(|e| unavailable(format!("dial through proxy: {e}")))?;
    let got = resp
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if got != ws_proto::SUBPROTOCOL {
        return Err(unavailable(format!(
            "server did not negotiate {} (got {got:?})",
            ws_proto::SUBPROTOCOL
        )));
    }
    Ok((
        Client::from_conn(
            ws_proto::conn::tungstenite::TungsteniteConn::new(ws),
            ClientConfig::default(),
        ),
        true,
    ))
}

/// Submits coordinated Changes, oldest first, and records their outcomes
/// in every Space they touched. Runs after a coordinated write and
/// periodically; a missing-predecessor answer waits for the Space workers
/// to push what the Change builds on.
fn spawn_coordinator(store: Arc<Store>, client: Client, token: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(e) = submit_coordinated(&store, &client, &token).await {
                tracing::warn!(error = %e, "coordinated submission failed");
            }
            tokio::select! {
                () = store.wait_dirty() => {}
                () = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
        }
    })
}

async fn submit_coordinated(
    store: &Store,
    client: &Client,
    token: &str,
) -> Result<(), RemoteError> {
    let sync = SyncServiceClient::new(client.clone());
    for env in store.coordinator_outbox().await? {
        for space_id in crate::sync::envelope_spaces(&env)? {
            let space = store.space(space_id).await?;
            upload_attachments(
                client,
                token,
                &space,
                &crate::sync::envelope_for(&env, space_id),
            )
            .await?;
        }
        let res = submit_envelope(&sync, token, &env).await?;
        match res.outcome {
            Some(Outcome::Rejected(r)) if r.reason() == Reason::MissingPredecessors => break,
            Some(outcome) => store.record_coordinated_outcome(&env, &outcome).await?,
            None => return Err(RemoteError::BadReply("submit outcome")),
        }
    }
    Ok(())
}

fn spawn_worker(
    space_id: SpaceId,
    space: Arc<SpaceStore>,
    client: Client,
    token: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let sync = SyncServiceClient::new(client.clone());
        let mut backoff = Duration::from_millis(200);
        // One subscription outlives the rounds, opened before the first
        // one: an announcement that lands during a round is buffered in
        // the stream, never missed.
        let mut announce: Option<SubscribeClientStream> = None;
        loop {
            if announce.is_none() {
                announce = sync
                    .subscribe(authed(
                        &token,
                        SubscribeRequest {
                            space_ids: vec![space_id.to_string()],
                        },
                    ))
                    .await
                    .ok();
            }
            match replicate_once(space_id, &space, &client, &token).await {
                Ok(()) => backoff = Duration::from_millis(200),
                Err(e) => {
                    tracing::warn!(space = %space_id, error = %e, "replication round failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
            }
            // Wait for a reason to run again: a local write, a server
            // announcement, or a periodic check.
            let announced = async {
                if let Some(stream) = &announce {
                    stream.recv().await.is_ok_and(|m| m.is_some())
                } else {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    false
                }
            };
            tokio::select! {
                () = space.wait_dirty() => {}
                alive = announced => {
                    if !alive {
                        // The stream ended or failed: open a fresh one next round.
                        announce = None;
                    }
                }
                () = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
        }
    })
}

async fn fetch_blob(
    client: &Client,
    token: &str,
    store: &SpaceStore,
    sha256: &str,
    size: i64,
) -> Result<(), RemoteError> {
    let stream = SyncServiceClient::new(client.clone())
        .download_blob(authed(
            token,
            DownloadBlobRequest {
                space_id: store.id().to_string(),
                sha256: sha256.to_owned(),
            },
        ))
        .await?;
    let mut writer = store.blobs().begin(store.id())?;
    while let Some(chunk) = stream.recv().await? {
        writer.write(&chunk.data)?;
    }
    writer.finish(Some(sha256), u64::try_from(size).ok())?;
    store.record_blob(sha256, size, true).await?;
    Ok(())
}

/// Sends attachment bytes the authority lacks before the Change naming
/// them goes out.
async fn upload_blob(
    client: &Client,
    token: &str,
    store: &SpaceStore,
    sha256: &str,
    size: i64,
) -> Result<(), RemoteError> {
    let sync = SyncServiceClient::new(client.clone());
    let stream = sync
        .upload_blob(CallOptions::new().with_header("authorization", format!("Bearer {token}")))
        .await?;
    stream
        .send(&UploadBlobRequest {
            chunk: Some(upload_blob_request::Chunk::Header(BlobHeader {
                space_id: store.id().to_string(),
                sha256: sha256.to_owned(),
                size,
            })),
        })
        .await?;
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    store.blobs().read_chunks(store.id(), sha256, |c| {
        chunks.push(c.to_vec());
        Ok(())
    })?;
    for data in chunks {
        stream
            .send(&UploadBlobRequest {
                chunk: Some(upload_blob_request::Chunk::Data(data.into())),
            })
            .await?;
    }
    stream.close_and_recv().await?;
    store.set_blob_uploaded(sha256).await?;
    Ok(())
}

/// Uploads the bytes of every attachment an envelope creates, unless the
/// authority already has them.
async fn upload_attachments(
    client: &Client,
    token: &str,
    space: &SpaceStore,
    env: &elri_proto::elri::v1::sync::Envelope,
) -> Result<(), RemoteError> {
    for part in &env.parts {
        if part.kind != ObjectKind::Attachment as i32 {
            continue;
        }
        let object = space.get(ObjectId::parse(&part.object_id)?).await?;
        let Some(sha) = crate::model::attachment_sha256(&object) else {
            continue;
        };
        let Some((size, _, uploaded)) = space.blob(sha).await? else {
            continue;
        };
        if !uploaded {
            upload_blob(client, token, space, sha, size).await?;
        }
    }
    Ok(())
}

/// Submits one envelope, in chunks when it is large.
async fn submit_envelope(
    sync: &SyncServiceClient,
    token: &str,
    env: &elri_proto::elri::v1::sync::Envelope,
) -> Result<SubmitResponse, RemoteError> {
    let size = prost::Message::encoded_len(env);
    if size <= crate::sync::LARGE_BYTES {
        return Ok(sync
            .submit(authed(
                token,
                SubmitRequest {
                    change: Some(env.clone()),
                },
            ))
            .await?
            .msg);
    }
    let bytes = prost::Message::encode_to_vec(env);
    let stream = sync
        .submit_chunked(CallOptions::new().with_header("authorization", format!("Bearer {token}")))
        .await?;
    stream
        .send(&SubmitChunkedRequest {
            chunk: Some(submit_chunked_request::Chunk::TotalBytes(
                i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            )),
        })
        .await?;
    for piece in bytes.chunks(crate::sync::CHUNK_BYTES) {
        stream
            .send(&SubmitChunkedRequest {
                chunk: Some(submit_chunked_request::Chunk::Data(piece.to_vec().into())),
            })
            .await?;
    }
    Ok(stream.close_and_recv().await?.msg)
}

/// Fetches an entry's envelope that Pull left out for its size.
async fn fetch_large_entry(
    sync: &SyncServiceClient,
    token: &str,
    space_id: SpaceId,
    seq: i64,
    expected: i64,
) -> Result<elri_proto::elri::v1::sync::Envelope, RemoteError> {
    let expected = usize::try_from(expected).unwrap_or(0);
    if expected > crate::sync::MAX_CHANGE_BYTES {
        return Err(RemoteError::BadReply("entry larger than the limit"));
    }
    let stream = sync
        .fetch_entry(authed(
            token,
            FetchEntryRequest {
                space_id: space_id.to_string(),
                seq,
            },
        ))
        .await?;
    let mut buf: Vec<u8> = Vec::with_capacity(expected);
    while let Some(chunk) = stream.recv().await? {
        if buf.len() + chunk.data.len() > crate::sync::MAX_CHANGE_BYTES {
            return Err(RemoteError::BadReply("entry larger than the limit"));
        }
        buf.extend_from_slice(&chunk.data);
    }
    <elri_proto::elri::v1::sync::Envelope as prost::Message>::decode(buf.as_slice())
        .map_err(|_| RemoteError::BadReply("entry envelope"))
}

/// Pushes the outbox, then pulls everything after the cursor.
async fn replicate_once(
    space_id: SpaceId,
    space: &SpaceStore,
    client: &Client,
    token: &str,
) -> Result<(), RemoteError> {
    let sync = SyncServiceClient::new(client.clone());

    for env in space.outbox(0).await? {
        upload_attachments(client, token, space, &env).await?;
        let res = submit_envelope(&sync, token, &env).await?;
        match res.outcome {
            Some(Outcome::Rejected(r)) if r.reason() == Reason::MissingPredecessors => {
                // Transient: the server lacks history we have not pushed yet.
                // Pull first and try again next round.
                break;
            }
            Some(outcome @ Outcome::Rejected(_)) => {
                // Dependents of a rejected Change are paused by the store;
                // start the next round from a fresh outbox so none of them
                // go out.
                space.record_outcome(&env.id, &outcome).await?;
                break;
            }
            Some(outcome) => space.record_outcome(&env.id, &outcome).await?,
            None => return Err(RemoteError::BadReply("submit outcome")),
        }
    }

    loop {
        let after = space.cursor().await?;
        let res = sync
            .pull(authed(
                token,
                PullRequest {
                    space_id: space_id.to_string(),
                    after_seq: after,
                    limit: 200,
                },
            ))
            .await?
            .msg;
        for entry in &res.entries {
            if let Some(env) = entry.change.as_ref() {
                space.apply_accepted(entry.seq, env).await?;
            } else if entry.large_bytes > 0 {
                let env =
                    fetch_large_entry(&sync, token, space_id, entry.seq, entry.large_bytes).await?;
                space.apply_accepted(entry.seq, &env).await?;
            } else if !entry.purged_object_id.is_empty() {
                let id = ObjectId::parse(&entry.purged_object_id)
                    .map_err(|_| RemoteError::BadReply("purged object id"))?;
                space.apply_purge(entry.seq, id).await?;
            } else if let Some(pin) = entry.pin.as_ref() {
                space.apply_pin(entry.seq, pin).await?;
            } else {
                return Err(RemoteError::BadReply("entry without change or purge"));
            }
        }
        if !res.has_more {
            break;
        }
    }
    Ok(())
}
