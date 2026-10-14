//! Device identity: a stable `UUIDv7` created on first start and persisted in
//! the data directory, plus the per-launch loopback token.
//!
//! The token is random, lives only in memory and in the endpoint file of the
//! running core, and is required on every request.

use std::path::{Path, PathBuf};

use rand::RngCore;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Errors raised while reading or creating the device identity.
#[derive(Debug, thiserror::Error)]
pub enum DeviceError {
    /// The data directory could not be read or written.
    #[error("device identity I/O at {path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// The stored identity file is not valid.
    #[error("device identity file {path} is corrupt: {source}")]
    Corrupt {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: serde_json::Error,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct DeviceFile {
    device_id: Uuid,
}

/// Stable identity of this device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceId(Uuid);

impl DeviceId {
    /// Loads the identity from `data_dir/device.json`, creating it on first run.
    pub fn load_or_create(data_dir: &Path) -> Result<Self, DeviceError> {
        create_private_dir(data_dir).map_err(|source| DeviceError::Io {
            path: data_dir.to_path_buf(),
            source,
        })?;
        let path = data_dir.join("device.json");
        match std::fs::read(&path) {
            Ok(bytes) => {
                let file: DeviceFile =
                    serde_json::from_slice(&bytes).map_err(|source| DeviceError::Corrupt {
                        path: path.clone(),
                        source,
                    })?;
                Ok(Self(file.device_id))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let id = Uuid::now_v7();
                let bytes =
                    serde_json::to_vec_pretty(&DeviceFile { device_id: id }).map_err(|source| {
                        DeviceError::Corrupt {
                            path: path.clone(),
                            source,
                        }
                    })?;
                std::fs::write(&path, bytes).map_err(|source| DeviceError::Io {
                    path: path.clone(),
                    source,
                })?;
                Ok(Self(id))
            }
            Err(source) => Err(DeviceError::Io { path, source }),
        }
    }

    /// Canonical string form.
    pub fn as_string(&self) -> String {
        self.0.to_string()
    }
}

/// Makes sure the data directory exists and is readable by its owner only;
/// databases and the endpoint token live there. The shell may have created
/// the directory earlier with wider permissions, so they are tightened here
/// on every start.
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Per-launch secret that every loopback client must present.
#[derive(Clone, PartialEq, Eq)]
pub struct LaunchToken(String);

impl LaunchToken {
    /// Generates 32 random bytes, hex-encoded.
    pub fn generate() -> Self {
        use std::fmt::Write as _;
        let mut bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut bytes);
        let hex = bytes.iter().fold(String::with_capacity(64), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        });
        Self(hex)
    }

    /// The token as it travels in the `authorization` header: `Bearer <hex>`.
    pub fn bearer(&self) -> String {
        format!("Bearer {}", self.0)
    }

    /// Checks a presented `authorization` header value in constant time.
    pub fn matches_header(&self, header: &str) -> bool {
        let expected = self.bearer();
        let (a, b) = (expected.as_bytes(), header.as_bytes());
        if a.len() != b.len() {
            return false;
        }
        a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
    }

    /// Raw hex value, for the endpoint file only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for LaunchToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LaunchToken(***)")
    }
}
