//! Authoritative synchronization server. Users, memberships, and Spaces are
//! server records; Changes are admitted through the same store the local
//! core uses, so a Space file on the server has the same shape as on a
//! device.

pub mod service;
pub mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use elri_core::device::DeviceId;
use elri_core::store::Store;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use ws_proto::server::OriginPolicy;
use ws_proto::{Server as WsServer, ServerConfig as WsConfig};

use store::ServerStore;

/// Start-up configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory holding `server.db`, the Space files, and the server identity.
    pub data_dir: PathBuf,
    /// Address to listen on.
    pub bind: SocketAddr,
}

/// Errors raised while starting the server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Registry failure.
    #[error(transparent)]
    Registry(#[from] store::ServerStoreError),
    /// Space storage failure.
    #[error(transparent)]
    Spaces(#[from] elri_core::store::StoreError),
    /// Server identity failure.
    #[error(transparent)]
    Identity(#[from] elri_core::device::DeviceError),
    /// The listener could not be bound.
    #[error("bind {addr}: {source}")]
    Bind {
        /// Requested address.
        addr: SocketAddr,
        /// Underlying error.
        source: std::io::Error,
    },
}

/// Shared state behind every handler.
pub struct State {
    /// Users and memberships.
    pub registry: ServerStore,
    /// Spaces and their logs.
    pub spaces: Store,
    /// Announces `(space_id, seq)` after every acceptance.
    pub events: broadcast::Sender<(uuid::Uuid, i64)>,
    /// Attachment bytes, per Space.
    pub blobs: elri_core::blobs::Blobs,
}

/// A running server.
pub struct Server {
    addr: SocketAddr,
    task: JoinHandle<()>,
    retention: JoinHandle<()>,
    state: Arc<State>,
}

impl Server {
    /// Opens the stores, binds the listener, and starts serving.
    pub async fn start(config: Config) -> Result<Self, ServerError> {
        let registry = ServerStore::open(&config.data_dir).await?;
        let identity = DeviceId::load_or_create(&config.data_dir)?;
        let spaces = Store::open(&config.data_dir, identity).await?;
        let (events, _) = broadcast::channel(1024);
        let blobs = elri_core::blobs::Blobs::new(&config.data_dir);
        let state = Arc::new(State {
            registry,
            spaces,
            events,
            blobs,
        });

        let listener =
            TcpListener::bind(config.bind)
                .await
                .map_err(|source| ServerError::Bind {
                    addr: config.bind,
                    source,
                })?;
        let addr = listener.local_addr().map_err(|source| ServerError::Bind {
            addr: config.bind,
            source,
        })?;

        let ws = WsServer::new(WsConfig {
            origin: OriginPolicy::same_origin_only(),
            ..WsConfig::default()
        });
        service::register(&ws, Arc::clone(&state));
        let task = tokio::spawn(async move {
            if let Err(e) = ws.serve_listener(listener).await {
                tracing::error!(error = %e, "listener stopped");
            }
        });
        let retention = {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                loop {
                    match state.spaces.purge_expired(true).await {
                        Ok(0) => {}
                        Ok(n) => tracing::info!(purged = n, "retention purge"),
                        Err(e) => tracing::warn!(error = %e, "retention purge failed"),
                    }
                    tokio::time::sleep(std::time::Duration::from_hours(1)).await;
                }
            })
        };
        tracing::info!(%addr, "elri-server listening");
        Ok(Self {
            addr,
            task,
            retention,
            state,
        })
    }

    /// Where clients connect.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// WebSocket URL of this server.
    pub fn url(&self) -> String {
        format!("ws://{}/", self.addr)
    }

    /// Registry access, for administration.
    pub fn state(&self) -> &Arc<State> {
        &self.state
    }

    /// Stops serving.
    pub fn shutdown(self) {
        self.task.abort();
        self.retention.abort();
    }
}
