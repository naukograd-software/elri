//! Local backups: a consistent copy of every database with a manifest that
//! says what is in it, and restore of such a copy into a data directory.
//!
//! A backup takes the write barrier (every database lock at once), has the
//! engine write a compacted copy of each file, releases the barrier, then
//! hashes the copies and reopens each one to read it back. Only then is
//! the manifest marked verified. Secrets are never copied; the manifest
//! lists every omission so a backup is never mistaken for more than it is.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::device::DeviceId;
use crate::model::now_ms;
use crate::store::{Store, StoreError};

/// Manifest format this code writes.
pub const FORMAT: u32 = 1;

/// Name of the manifest file inside a backup directory.
pub const MANIFEST: &str = "manifest.json";

/// What a backup contains.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest format.
    pub format: u32,
    /// When the barrier was taken.
    pub created_at_ms: i64,
    /// Device the backup was taken from.
    pub device_id: String,
    /// Spaces included.
    pub spaces: Vec<SpaceSummary>,
    /// Files included, relative to the backup directory.
    pub files: Vec<FileEntry>,
    /// What is not included.
    pub omissions: Vec<String>,
    /// Every file was reopened and read back after copying.
    pub verified: bool,
}

/// One Space in a backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceSummary {
    /// Space id.
    pub id: String,
    /// Name at backup time.
    pub name: String,
    /// `Authority` code.
    pub authority: i32,
    /// Highest server sequence installed; 0 for local Spaces.
    pub cursor: i64,
    /// Changes not yet accepted.
    pub pending: usize,
    /// Changes rejected or paused.
    pub rejected: usize,
}

/// One file in a backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path relative to the backup directory.
    pub path: String,
    /// Hex SHA-256 of the file.
    pub sha256: String,
    /// Size in bytes.
    pub bytes: u64,
}

/// Backup and restore failures.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// Storage failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// File system failure.
    #[error("{path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// The manifest could not be read or written.
    #[error("{path}: {source}")]
    Json {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: serde_json::Error,
    },
    /// A file does not match its manifest entry.
    #[error("{path}: checksum mismatch")]
    Checksum {
        /// File involved.
        path: PathBuf,
    },
    /// The manifest is of a format this code does not read.
    #[error("manifest format {0} is not supported")]
    Format(u32),
    /// The destination already holds data.
    #[error("{0} already holds a device; pass replace to move it aside")]
    Occupied(PathBuf),
    /// A core is running on the destination.
    #[error("a core is running on {0}; stop it first")]
    InUse(PathBuf),
    /// Device identity failure.
    #[error(transparent)]
    Device(#[from] crate::device::DeviceError),
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> BackupError + '_ {
    move |source| BackupError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Writes a backup of `store` into a new directory under `dest`, named by
/// the instant, and returns its manifest and directory.
pub async fn create(store: &Store, dest: &Path) -> Result<(Manifest, PathBuf), BackupError> {
    let created_at_ms = now_ms();
    let dir = dest.join(format!("elri-backup-{created_at_ms}"));
    std::fs::create_dir_all(dir.join("spaces")).map_err(io(&dir))?;
    restrict(&dir);

    let (device_id, spaces) = store.backup_into(&dir).await?;

    let mut files = Vec::with_capacity(spaces.len() + 1);
    let mut names = vec!["device.db".to_owned()];
    names.extend(spaces.iter().map(|s| format!("spaces/{}.db", s.id)));
    for name in names {
        let path = dir.join(&name);
        let (sha256, bytes) = hash_file(&path)?;
        files.push(FileEntry {
            path: name,
            sha256,
            bytes,
        });
    }
    let mut manifest = Manifest {
        format: FORMAT,
        created_at_ms,
        device_id: device_id.as_string(),
        spaces,
        files,
        omissions: vec![
            "secrets: the server token is not copied; connect again after restore".to_owned(),
            "attachments: none exist yet".to_owned(),
        ],
        verified: false,
    };
    write_manifest(&dir, &manifest)?;

    // Staging restore: every copy must open and read back.
    Store::verify_copies(&dir, &manifest.files).await?;
    manifest.verified = true;
    write_manifest(&dir, &manifest)?;
    Ok((manifest, dir))
}

/// Reads and checks a backup: manifest format, and every file's size and
/// checksum. Returns the manifest.
pub fn check(dir: &Path) -> Result<Manifest, BackupError> {
    let manifest = read_manifest(dir)?;
    if manifest.format != FORMAT {
        return Err(BackupError::Format(manifest.format));
    }
    for f in &manifest.files {
        let path = dir.join(&f.path);
        let (sha256, bytes) = hash_file(&path)?;
        if sha256 != f.sha256 || bytes != f.bytes {
            return Err(BackupError::Checksum { path });
        }
    }
    Ok(manifest)
}

/// How restore treats an occupied data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Occupied {
    /// Refuse.
    Refuse,
    /// Move the existing contents aside to a sibling directory first.
    MoveAside,
}

/// Restores a checked backup into `data_dir`. The restored device gets a
/// fresh identity unless `keep_identity`; it never gets the secrets, so
/// the server must be connected again. The core must not be running on
/// `data_dir`.
pub async fn restore(
    backup_dir: &Path,
    data_dir: &Path,
    occupied: Occupied,
    keep_identity: bool,
) -> Result<Manifest, BackupError> {
    let manifest = check(backup_dir)?;
    if crate::core::in_use(data_dir).await {
        return Err(BackupError::InUse(data_dir.to_path_buf()));
    }
    if data_dir.join("device.db").exists() {
        match occupied {
            Occupied::Refuse => return Err(BackupError::Occupied(data_dir.to_path_buf())),
            Occupied::MoveAside => {
                let aside = data_dir.with_file_name(format!(
                    "{}.before-restore-{}",
                    data_dir
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("elri"),
                    now_ms()
                ));
                std::fs::rename(data_dir, &aside).map_err(io(data_dir))?;
            }
        }
    }
    std::fs::create_dir_all(data_dir.join("spaces")).map_err(io(data_dir))?;
    restrict(data_dir);
    for f in &manifest.files {
        let from = backup_dir.join(&f.path);
        let to = data_dir.join(&f.path);
        std::fs::copy(&from, &to).map_err(io(&from))?;
        restrict(&to);
    }
    if keep_identity {
        DeviceId::write(data_dir, &manifest.device_id)?;
    } else {
        let _ = std::fs::remove_file(data_dir.join("device.json"));
        DeviceId::load_or_create(data_dir)?;
    }
    Ok(manifest)
}

fn read_manifest(dir: &Path) -> Result<Manifest, BackupError> {
    let path = dir.join(MANIFEST);
    let bytes = std::fs::read(&path).map_err(io(&path))?;
    serde_json::from_slice(&bytes).map_err(|source| BackupError::Json { path, source })
}

fn write_manifest(dir: &Path, manifest: &Manifest) -> Result<(), BackupError> {
    let path = dir.join(MANIFEST);
    let bytes = serde_json::to_vec_pretty(manifest).map_err(|source| BackupError::Json {
        path: path.clone(),
        source,
    })?;
    std::fs::write(&path, bytes).map_err(io(&path))?;
    restrict(&path);
    Ok(())
}

fn hash_file(path: &Path) -> Result<(String, u64), BackupError> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path).map_err(io(path))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf).map_err(io(path))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    let digest = hasher.finalize();
    let hex = digest.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    });
    Ok((hex, total))
}

fn restrict(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = if path.is_dir() { 0o700 } else { 0o600 };
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}
