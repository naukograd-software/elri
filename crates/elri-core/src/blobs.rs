//! Attachment bytes: immutable files named by their SHA-256, kept per
//! Space under `data_dir/blobs/<space>/<sha256>`. A file is written to a
//! temporary name while its hash is computed and moved into place only
//! when the hash and size check out, so a name is never a lie.

use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};

use crate::model::SpaceId;

/// Largest attachment accepted, in bytes.
pub const MAX_BYTES: u64 = 1 << 30;

/// Chunk size used when streaming bytes out.
pub const CHUNK: usize = 64 * 1024;

/// Blob store failures.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    /// File system failure.
    #[error("{path}: {source}")]
    Io {
        /// Path involved.
        path: PathBuf,
        /// Underlying error.
        source: std::io::Error,
    },
    /// The bytes do not hash to the announced digest.
    #[error("bytes hash to {actual}, not {expected}")]
    Hash {
        /// Announced.
        expected: String,
        /// Computed.
        actual: String,
    },
    /// The bytes are not the announced size.
    #[error("received {actual} bytes, not {expected}")]
    Size {
        /// Announced.
        expected: u64,
        /// Received.
        actual: u64,
    },
    /// Over the limit.
    #[error("attachment exceeds {MAX_BYTES} bytes")]
    TooLarge,
    /// Not a hex SHA-256.
    #[error("not a sha256 digest: {0}")]
    BadDigest(String),
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> BlobError + '_ {
    move |source| BlobError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Checks that `s` is a lowercase hex SHA-256.
pub fn check_digest(s: &str) -> Result<(), BlobError> {
    if s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        Ok(())
    } else {
        Err(BlobError::BadDigest(s.to_owned()))
    }
}

/// Hex digest of a finished hasher.
fn hex(digest: &[u8]) -> String {
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// The blob store of one data directory.
#[derive(Debug, Clone)]
pub struct Blobs {
    root: PathBuf,
}

impl Blobs {
    /// Blob store under `data_dir/blobs`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            root: data_dir.join("blobs"),
        }
    }

    /// Where a blob of a Space lives.
    pub fn path(&self, space: SpaceId, sha256: &str) -> PathBuf {
        self.root.join(space.to_string()).join(sha256)
    }

    /// Whether the bytes are here.
    pub fn exists(&self, space: SpaceId, sha256: &str) -> bool {
        check_digest(sha256).is_ok() && self.path(space, sha256).is_file()
    }

    /// Starts writing a blob of a Space. Nothing is visible until
    /// [`Writer::finish`].
    pub fn begin(&self, space: SpaceId) -> Result<Writer, BlobError> {
        let dir = self.root.join(space.to_string());
        std::fs::create_dir_all(&dir).map_err(io(&dir))?;
        restrict(&dir);
        let tmp = dir.join(format!(".part-{}", uuid::Uuid::now_v7()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let file = options.open(&tmp).map_err(io(&tmp))?;
        Ok(Writer {
            dir,
            tmp,
            file,
            hasher: Sha256::new(),
            size: 0,
        })
    }

    /// Reads a blob in chunks, calling `f` for each.
    pub fn read_chunks(
        &self,
        space: SpaceId,
        sha256: &str,
        mut f: impl FnMut(&[u8]) -> Result<(), BlobError>,
    ) -> Result<(), BlobError> {
        check_digest(sha256)?;
        let path = self.path(space, sha256);
        let mut file = std::fs::File::open(&path).map_err(io(&path))?;
        let mut buf = vec![0u8; CHUNK];
        loop {
            let n = file.read(&mut buf).map_err(io(&path))?;
            if n == 0 {
                return Ok(());
            }
            f(&buf[..n])?;
        }
    }

    /// Removes a blob; removing a missing one is fine.
    pub fn remove(&self, space: SpaceId, sha256: &str) -> Result<(), BlobError> {
        check_digest(sha256)?;
        let path = self.path(space, sha256);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(BlobError::Io { path, source }),
        }
    }

    /// Hex SHA-256 of a file on disk, with its size.
    pub fn digest_file(path: &Path) -> Result<(String, u64), BlobError> {
        let mut file = std::fs::File::open(path).map_err(io(path))?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; CHUNK];
        let mut size = 0u64;
        loop {
            let n = file.read(&mut buf).map_err(io(path))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            size += n as u64;
        }
        Ok((hex(&hasher.finalize()), size))
    }
}

/// A blob being written.
pub struct Writer {
    dir: PathBuf,
    tmp: PathBuf,
    file: std::fs::File,
    hasher: Sha256,
    size: u64,
}

impl Writer {
    /// Appends bytes.
    pub fn write(&mut self, chunk: &[u8]) -> Result<(), BlobError> {
        self.size += chunk.len() as u64;
        if self.size > MAX_BYTES {
            return Err(BlobError::TooLarge);
        }
        self.file.write_all(chunk).map_err(io(&self.tmp))?;
        self.hasher.update(chunk);
        Ok(())
    }

    /// Bytes written so far.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Checks the announced size and digest, if any, and moves the file
    /// into place. Returns the digest. Bytes already present under that
    /// name are kept; the duplicate is dropped.
    pub fn finish(
        mut self,
        expected_sha256: Option<&str>,
        expected_size: Option<u64>,
    ) -> Result<String, BlobError> {
        self.file.flush().map_err(io(&self.tmp))?;
        self.file.sync_all().map_err(io(&self.tmp))?;
        let actual = hex(&std::mem::take(&mut self.hasher).finalize());
        if let Some(expected) = expected_size {
            if expected != self.size {
                let _ = std::fs::remove_file(&self.tmp);
                return Err(BlobError::Size {
                    expected,
                    actual: self.size,
                });
            }
        }
        if let Some(expected) = expected_sha256 {
            if expected != actual {
                let _ = std::fs::remove_file(&self.tmp);
                return Err(BlobError::Hash {
                    expected: expected.to_owned(),
                    actual,
                });
            }
        }
        let path = self.dir.join(&actual);
        if path.is_file() {
            let _ = std::fs::remove_file(&self.tmp);
        } else {
            std::fs::rename(&self.tmp, &path).map_err(io(&path))?;
        }
        Ok(actual)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // An unfinished upload leaves nothing behind.
        let _ = std::fs::remove_file(&self.tmp);
    }
}

fn restrict(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}
