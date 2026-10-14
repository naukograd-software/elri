//! Lifecycle of a running core: configuration, start, endpoint publication,
//! shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::api;
use crate::device::{DeviceError, DeviceId, LaunchToken};
use crate::remote::Remote;
use crate::secrets::{Secrets, SecretsBackend, SecretsError};
use crate::store::{Store, StoreError};

/// Version information baked into the binary that embeds the core.
#[derive(Debug, Clone)]
pub struct BuildInfo {
    /// Semantic version.
    pub version: &'static str,
    /// Git commit, if known at build time.
    pub commit: &'static str,
}

/// Start-up configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory holding the device identity, databases, and the endpoint file.
    pub data_dir: PathBuf,
    /// Loopback address to listen on; port 0 picks a free port.
    pub bind: SocketAddr,
    /// Version information of the embedding binary.
    pub build: BuildInfo,
    /// Browser origins (host patterns) allowed to open the loopback socket,
    /// besides same-origin and non-browser clients. The Tauri `WebView`
    /// presents `tauri://localhost` or `http://tauri.localhost`; the Vite dev
    /// server presents `http://127.0.0.1:<port>`.
    pub allowed_origins: Vec<String>,
    /// Where the server token is kept.
    pub secrets: SecretsBackend,
}

impl Config {
    /// Default configuration for `data_dir`: loopback, random port, the
    /// shell's own origins allowed.
    pub fn new(data_dir: impl Into<PathBuf>, build: BuildInfo) -> Self {
        Self {
            data_dir: data_dir.into(),
            bind: SocketAddr::from(([127, 0, 0, 1], 0)),
            build,
            allowed_origins: vec![
                "localhost".to_owned(),
                "localhost:*".to_owned(),
                "tauri.localhost".to_owned(),
                "127.0.0.1:*".to_owned(),
            ],
            secrets: SecretsBackend::File,
        }
    }
}

/// The core's HTTP clients from the stored proxy settings, with the
/// password put back from the credential store. A stored proxy that no
/// longer parses must not keep the core from starting: it goes direct
/// until fixed.
async fn open_net(store: &Store, secrets: &Secrets) -> Result<Arc<crate::net::Net>, CoreError> {
    let settings = store.settings().await?;
    let mut proxy = settings.proxy.unwrap_or_default();
    if let Some(pw) = secrets.get(crate::net::PROXY_PASSWORD_KEY).await? {
        proxy.url = crate::net::with_password(&proxy.url, &pw);
    }
    let net = crate::net::Net::new(proxy).or_else(|e| {
        tracing::warn!(error = %e, "stored proxy unusable; connecting directly");
        crate::net::Net::new(elri_proto::elri::v1::settings::ProxySettings {
            mode: elri_proto::elri::v1::settings::ProxyMode::None.into(),
            ..Default::default()
        })
    })?;
    Ok(Arc::new(net))
}

/// How a client reaches a running core. Written to `data_dir/endpoint.json`
/// while the core runs so that the CLI and other local tools can find it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    /// WebSocket URL of the loopback listener.
    pub url: String,
    /// Per-launch token, presented as `authorization: Bearer <token>`.
    pub token: String,
    /// Process that owns the listener, so a stale file can be detected.
    pub pid: u32,
}

impl Endpoint {
    /// Reads the endpoint file of a running core. A file left behind by a
    /// process that is no longer alive is reported as stale rather than
    /// returned.
    pub fn read(data_dir: &std::path::Path) -> Result<Self, CoreError> {
        let path = data_dir.join("endpoint.json");
        let bytes = std::fs::read(&path).map_err(|source| CoreError::Io {
            path: path.clone(),
            source,
        })?;
        let endpoint: Self = serde_json::from_slice(&bytes).map_err(|source| CoreError::Json {
            path: path.clone(),
            source,
        })?;
        if !process_alive(endpoint.pid) {
            return Err(CoreError::Stale {
                path,
                pid: endpoint.pid,
            });
        }
        Ok(endpoint)
    }
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    pid != 0 && std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(not(unix))]
fn process_alive(_pid: u32) -> bool {
    true
}

/// Errors raised while starting or stopping a core.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// Device identity failure.
    #[error(transparent)]
    Device(#[from] DeviceError),
    /// Storage could not be opened.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The secret store could not be opened.
    #[error(transparent)]
    Secrets(#[from] SecretsError),
    /// The HTTP client could not be built.
    #[error(transparent)]
    Net(#[from] crate::net::NetError),
    /// File system failure.
    #[error("{path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// Malformed JSON file.
    #[error("{path}: {source}")]
    Json {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: serde_json::Error,
    },
    /// The endpoint file belongs to a process that no longer runs.
    #[error("{path}: core with pid {pid} is not running (stale endpoint file)")]
    Stale {
        /// Path involved.
        path: PathBuf,
        /// Recorded process id.
        pid: u32,
    },
    /// Another core already serves this data directory.
    #[error("another elri core (pid {pid}) is already running on {}", path.display())]
    AlreadyRunning {
        /// Data directory.
        path: PathBuf,
        /// Process holding it.
        pid: u32,
    },
    /// The loopback listener could not be bound.
    #[error("bind {addr}: {source}")]
    Bind {
        /// Requested address.
        addr: SocketAddr,
        /// Underlying error.
        source: std::io::Error,
    },
}

/// A running core. Dropping it stops the listener and removes the endpoint
/// file.
pub struct Core {
    inner: Arc<Inner>,
    listener_task: JoinHandle<()>,
}

struct Inner {
    config: Config,
    device_id: DeviceId,
    token: LaunchToken,
    endpoint: Endpoint,
    store: Arc<Store>,
    remote: Arc<Remote>,
    rules_task: JoinHandle<()>,
    retention_task: JoinHandle<()>,
}

impl Core {
    /// Loads the device identity, binds the loopback listener, registers the
    /// API, and publishes the endpoint file.
    pub async fn start(config: Config) -> Result<Self, CoreError> {
        // A live endpoint file whose socket still answers means another core
        // owns this directory; a file whose socket is gone is stale and gets
        // overwritten below.
        if let Ok(existing) = Endpoint::read(&config.data_dir) {
            if endpoint_answers(&existing).await {
                return Err(CoreError::AlreadyRunning {
                    path: config.data_dir.clone(),
                    pid: existing.pid,
                });
            }
        }
        let device_id = DeviceId::load_or_create(&config.data_dir)?;
        let token = LaunchToken::generate();
        let store = Arc::new(Store::open(&config.data_dir, device_id).await?);
        let secrets = Secrets::open(config.secrets, &config.data_dir, &device_id.as_string())?;
        let net = open_net(&store, &secrets).await?;
        let remote = Arc::new(Remote::new(
            Arc::clone(&store),
            device_id.as_string(),
            secrets.clone(),
            Arc::clone(&net),
        ));

        let listener = TcpListener::bind(config.bind)
            .await
            .map_err(|source| CoreError::Bind {
                addr: config.bind,
                source,
            })?;
        let local = listener.local_addr().map_err(|source| CoreError::Bind {
            addr: config.bind,
            source,
        })?;

        let endpoint = Endpoint {
            url: format!("ws://{local}/"),
            token: token.expose().to_owned(),
            pid: std::process::id(),
        };
        write_endpoint(&config.data_dir, &endpoint)?;

        let retention_task = spawn_retention(Arc::clone(&store));
        let modules = crate::extension::Modules::new(&config.data_dir);
        let scripts =
            crate::script::Host::new(Arc::clone(&store), device_id.as_string(), modules.clone());
        let saved = store
            .setting(
                crate::rules::MAX_CONCURRENT_SCRIPTS,
                i64::try_from(crate::script::DEFAULT_CONCURRENCY).unwrap_or(4),
            )
            .await?;
        scripts
            .set_concurrency(usize::try_from(saved).unwrap_or(crate::script::DEFAULT_CONCURRENCY));
        let rules_task =
            crate::rules::spawn_runner(Arc::clone(&store), scripts.clone(), device_id.as_string());
        if let Err(e) = crate::workflows::recover(&store, &scripts).await {
            tracing::warn!(error = %e, "workflow recovery failed");
        }
        let server = api::build_server(
            api::Services {
                build: config.build.clone(),
                device_id,
                token: token.clone(),
                store: Arc::clone(&store),
                remote: Arc::clone(&remote),
                scripts,
                modules,
                net: Arc::clone(&net),
                secrets,
            },
            config.allowed_origins.clone(),
        );
        let listener_task = tokio::spawn(async move {
            if let Err(e) = server.serve_listener(listener).await {
                tracing::error!(error = %e, "loopback listener stopped");
            }
        });
        tracing::info!(url = %endpoint.url, device = %device_id.as_string(), "core started");
        {
            let remote = Arc::clone(&remote);
            tokio::spawn(async move { remote.resume().await });
        }

        Ok(Self {
            inner: Arc::new(Inner {
                config,
                device_id,
                token,
                endpoint,
                store,
                remote,
                rules_task,
                retention_task,
            }),
            listener_task,
        })
    }

    /// Where clients connect.
    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    /// This device's identity.
    pub fn device_id(&self) -> DeviceId {
        self.inner.device_id
    }

    /// The per-launch token.
    pub fn token(&self) -> &LaunchToken {
        &self.inner.token
    }

    /// Storage of this device.
    pub fn store(&self) -> &Arc<Store> {
        &self.inner.store
    }

    /// Server link of this device.
    pub fn remote(&self) -> &Arc<Remote> {
        &self.inner.remote
    }

    /// Stops the listener and removes the endpoint file.
    pub fn shutdown(self) {
        let remote = Arc::clone(&self.inner.remote);
        tokio::spawn(async move { remote.close().await });
        self.listener_task.abort();
        self.inner.rules_task.abort();
        self.inner.retention_task.abort();
        remove_endpoint(&self.inner.config.data_dir);
        tracing::info!("core stopped");
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        self.listener_task.abort();
        self.inner.rules_task.abort();
        self.inner.retention_task.abort();
        remove_endpoint(&self.inner.config.data_dir);
    }
}

/// The endpoint file contains the launch token, so it is owner-only and
/// written atomically: a temporary file with restricted permissions, then a
/// rename over the final name.
/// Purges expired deleted Objects of local Spaces shortly after start and
/// then every hour.
pub fn spawn_retention(store: Arc<Store>) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        loop {
            match store.purge_expired(false).await {
                Ok(0) => {}
                Ok(n) => tracing::info!(purged = n, "retention purge"),
                Err(e) => tracing::warn!(error = %e, "retention purge failed"),
            }
            tokio::time::sleep(std::time::Duration::from_hours(1)).await;
        }
    })
}

/// Whether a core is running on `data_dir`: its endpoint file names a
/// socket that still answers.
pub async fn in_use(data_dir: &std::path::Path) -> bool {
    match Endpoint::read(data_dir) {
        Ok(existing) => endpoint_answers(&existing).await,
        Err(_) => false,
    }
}

async fn endpoint_answers(endpoint: &Endpoint) -> bool {
    let Some(addr) = endpoint
        .url
        .strip_prefix("ws://")
        .and_then(|rest| rest.split('/').next())
    else {
        return false;
    };
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}

fn write_endpoint(data_dir: &std::path::Path, endpoint: &Endpoint) -> Result<(), CoreError> {
    use std::io::Write as _;

    let path = data_dir.join("endpoint.json");
    let tmp = data_dir.join(".endpoint.json.tmp");
    let bytes = serde_json::to_vec_pretty(endpoint).map_err(|source| CoreError::Json {
        path: path.clone(),
        source,
    })?;
    let io = |p: &std::path::Path| {
        let p = p.to_path_buf();
        move |source| CoreError::Io { path: p, source }
    };

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(io(&tmp))?;
    file.write_all(&bytes).map_err(io(&tmp))?;
    file.sync_all().map_err(io(&tmp))?;
    drop(file);
    std::fs::rename(&tmp, &path).map_err(io(&path))
}

fn remove_endpoint(data_dir: &std::path::Path) {
    let _ = std::fs::remove_file(data_dir.join("endpoint.json"));
}
