//! Where the device keeps secrets it must present elsewhere, such as the
//! server token: the operating system's credential store when one is
//! available, otherwise an owner-only file in the data directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Which backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SecretsBackend {
    /// `data_dir/secrets.json`, mode 0600. The default: predictable, works
    /// headless, and keeps tests away from the user's credential store.
    #[default]
    File,
    /// The operating system's credential store; fails to open where none
    /// answers.
    Keyring,
    /// The credential store when it answers, the file otherwise.
    Auto,
}

/// Secret storage failures.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    /// The credential store refused or is absent.
    #[error("credential store: {0}")]
    Keyring(String),
    /// The secrets file could not be read or written.
    #[error("{path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// The secrets file is not valid JSON.
    #[error("{path}: {source}")]
    Json {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone)]
enum Backend {
    File(PathBuf),
    #[cfg(feature = "keyring")]
    Keyring {
        /// Distinguishes this data directory's entries from another's on
        /// the same account.
        scope: String,
    },
}

/// The device's secret store.
#[derive(Debug, Clone)]
pub struct Secrets {
    backend: Backend,
    lock: Arc<tokio::sync::Mutex<()>>,
}

const SERVICE: &str = "elri";

impl Secrets {
    /// Opens the store. `scope` tells one data directory's entries from
    /// another's in the credential store; the device id serves.
    pub fn open(
        backend: SecretsBackend,
        data_dir: &Path,
        scope: &str,
    ) -> Result<Self, SecretsError> {
        let file = Backend::File(data_dir.join("secrets.json"));
        let backend = match backend {
            SecretsBackend::File => file,
            SecretsBackend::Keyring => keyring_backend(scope)?,
            SecretsBackend::Auto => match keyring_backend(scope) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "no credential store; secrets go to the data directory");
                    file
                }
            },
        };
        Ok(Self {
            backend,
            lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Whether secrets live in the operating system's credential store.
    pub fn in_keyring(&self) -> bool {
        !matches!(self.backend, Backend::File(_))
    }

    /// Reads a secret.
    pub async fn get(&self, key: &str) -> Result<Option<String>, SecretsError> {
        let _guard = self.lock.lock().await;
        match &self.backend {
            Backend::File(path) => Ok(read_file(path)?.remove(key)),
            #[cfg(feature = "keyring")]
            Backend::Keyring { scope } => {
                let user = entry_name(scope, key);
                blocking(
                    move || match keyring::Entry::new(SERVICE, &user)?.get_password() {
                        Ok(v) => Ok(Some(v)),
                        Err(keyring::Error::NoEntry) => Ok(None),
                        Err(e) => Err(e),
                    },
                )
                .await
            }
        }
    }

    /// Stores a secret, replacing any previous value.
    pub async fn set(&self, key: &str, value: &str) -> Result<(), SecretsError> {
        let _guard = self.lock.lock().await;
        match &self.backend {
            Backend::File(path) => {
                let mut all = read_file(path)?;
                all.insert(key.to_owned(), value.to_owned());
                write_file(path, &all)
            }
            #[cfg(feature = "keyring")]
            Backend::Keyring { scope } => {
                let user = entry_name(scope, key);
                let value = value.to_owned();
                blocking(move || keyring::Entry::new(SERVICE, &user)?.set_password(&value)).await
            }
        }
    }

    /// Removes a secret; removing a missing one is fine.
    pub async fn delete(&self, key: &str) -> Result<(), SecretsError> {
        let _guard = self.lock.lock().await;
        match &self.backend {
            Backend::File(path) => {
                let mut all = read_file(path)?;
                if all.remove(key).is_some() {
                    write_file(path, &all)?;
                }
                Ok(())
            }
            #[cfg(feature = "keyring")]
            Backend::Keyring { scope } => {
                let user = entry_name(scope, key);
                blocking(
                    move || match keyring::Entry::new(SERVICE, &user)?.delete_credential() {
                        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
                        Err(e) => Err(e),
                    },
                )
                .await
            }
        }
    }
}

#[cfg(feature = "keyring")]
fn keyring_backend(scope: &str) -> Result<Backend, SecretsError> {
    // Probing a name that is never stored tells a working store (no such
    // entry) from an absent one (anything else).
    match keyring::Entry::new(SERVICE, &entry_name(scope, "probe")).and_then(|e| e.get_password()) {
        Ok(_) | Err(keyring::Error::NoEntry) => Ok(Backend::Keyring {
            scope: scope.to_owned(),
        }),
        Err(e) => Err(SecretsError::Keyring(e.to_string())),
    }
}

#[cfg(not(feature = "keyring"))]
fn keyring_backend(_scope: &str) -> Result<Backend, SecretsError> {
    Err(SecretsError::Keyring(
        "not built with credential store support".to_owned(),
    ))
}

#[cfg(feature = "keyring")]
fn entry_name(scope: &str, key: &str) -> String {
    format!("{key}@{scope}")
}

#[cfg(feature = "keyring")]
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> keyring::Result<T> + Send + 'static,
) -> Result<T, SecretsError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| SecretsError::Keyring(e.to_string()))?
        .map_err(|e| SecretsError::Keyring(e.to_string()))
}

fn read_file(path: &Path) -> Result<BTreeMap<String, String>, SecretsError> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|source| SecretsError::Json {
            path: path.to_path_buf(),
            source,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(source) => Err(SecretsError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn write_file(path: &Path, all: &BTreeMap<String, String>) -> Result<(), SecretsError> {
    use std::io::Write as _;

    let io = |p: &Path| {
        let p = p.to_path_buf();
        move |source| SecretsError::Io { path: p, source }
    };
    let bytes = serde_json::to_vec_pretty(all).map_err(|source| SecretsError::Json {
        path: path.to_path_buf(),
        source,
    })?;
    let tmp = path.with_extension("json.tmp");
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
    std::fs::rename(&tmp, path).map_err(io(path))
}
