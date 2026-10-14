//! The device's connection to its server and the replication workers that
//! keep joined Spaces in step: push the outbox, pull after the cursor, and
//! wake up on local writes and server announcements.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use elri_proto::elri::v1::space::{Authority, Space as PbSpace};
use elri_proto::elri::v1::sync::sync_service_ws::SyncServiceClient;
use elri_proto::elri::v1::sync::{
    AddMemberRequest, CreateSpaceRequest, HelloRequest, ListSpacesRequest, PullRequest,
    PurgeObjectRequest, SubmitRequest, SubscribeRequest,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use ws_proto::{Client, ClientConfig, Code, Reconnect, Request, Status};

use crate::change::{Outcome, Reason};
use crate::model::{ObjectId, SpaceId};
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
}

struct Session {
    url: String,
    token: String,
    user_id: String,
    user_name: String,
    client: Client,
    workers: HashMap<SpaceId, JoinHandle<()>>,
}

/// The device's server link.
pub struct Remote {
    store: Arc<Store>,
    device_id: String,
    secrets: Secrets,
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
    pub fn new(store: Arc<Store>, device_id: String, secrets: Secrets) -> Self {
        Self {
            store,
            device_id,
            secrets,
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
        let client = dial(url).await?;
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
        let client = dial(&cfg.url).await?;
        self.install_session(cfg, client).await
    }

    async fn install_session(
        self: &Arc<Self>,
        cfg: RemoteConfig,
        client: Client,
    ) -> Result<(), RemoteError> {
        let mut session = Session {
            url: cfg.url,
            token: cfg.token,
            user_id: cfg.user_id,
            user_name: cfg.user_name,
            client,
            workers: HashMap::new(),
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

async fn dial(url: &str) -> Result<Client, RemoteError> {
    let cfg = ClientConfig {
        reconnect: Some(Reconnect {
            initial: Duration::from_millis(200),
            max: Duration::from_secs(30),
        }),
        ..ClientConfig::default()
    };
    Ok(Client::dial(url, cfg).await?)
}

fn spawn_worker(
    space_id: SpaceId,
    space: Arc<SpaceStore>,
    client: Client,
    token: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
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
            let announce = async {
                let sync = SyncServiceClient::new(client.clone());
                match sync
                    .subscribe(authed(
                        &token,
                        SubscribeRequest {
                            space_ids: vec![space_id.to_string()],
                        },
                    ))
                    .await
                {
                    Ok(stream) => {
                        let _ = stream.recv().await;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_secs(5)).await,
                }
            };
            tokio::select! {
                () = space.wait_dirty() => {}
                () = announce => {}
                () = tokio::time::sleep(Duration::from_secs(30)) => {}
            }
        }
    })
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
        let res = sync
            .submit(authed(
                token,
                SubmitRequest {
                    change: Some(env.clone()),
                },
            ))
            .await?
            .msg;
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
            } else if !entry.purged_object_id.is_empty() {
                let id = ObjectId::parse(&entry.purged_object_id)
                    .map_err(|_| RemoteError::BadReply("purged object id"))?;
                space.apply_purge(entry.seq, id).await?;
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
