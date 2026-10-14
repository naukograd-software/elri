//! Server-side registry: users, their access tokens, and Space memberships.
//! Spaces themselves live in the shared core store.

use std::path::Path;

use sha2::{Digest, Sha256};
use toasty::Db;
use toasty_driver_turso::Turso;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Server registry failures.
#[derive(Debug, thiserror::Error)]
pub enum ServerStoreError {
    /// The database layer failed.
    #[error(transparent)]
    Db(#[from] toasty::Error),
    /// File system failure.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// A user with that name already exists.
    #[error("user {0} already exists")]
    UserExists(String),
    /// No such user.
    #[error("user {0} not found")]
    UserNotFound(String),
    /// A stored row does not have the expected shape.
    #[error("corrupt row: {0}")]
    Corrupt(&'static str),
}

/// Role of a user in a Space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// All rights.
    Owner,
    /// Manages members and settings.
    Admin,
    /// Writes content.
    Editor,
    /// Reads only.
    Viewer,
}

impl Role {
    /// Stable code, shared with the protocol enum.
    pub fn code(self) -> i64 {
        match self {
            Self::Owner => 1,
            Self::Admin => 2,
            Self::Editor => 3,
            Self::Viewer => 4,
        }
    }

    /// Inverse of [`Role::code`].
    pub fn from_code(code: i64) -> Option<Self> {
        match code {
            1 => Some(Self::Owner),
            2 => Some(Self::Admin),
            3 => Some(Self::Editor),
            4 => Some(Self::Viewer),
            _ => None,
        }
    }

    /// May submit Changes.
    pub fn can_write(self) -> bool {
        !matches!(self, Self::Viewer)
    }

    /// May manage members.
    pub fn can_manage(self) -> bool {
        matches!(self, Self::Owner | Self::Admin)
    }
}

/// A registered user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    /// Identity.
    pub id: Uuid,
    /// Unique name.
    pub name: String,
}

#[derive(Debug, toasty::Model)]
#[table = "users"]
struct UserRow {
    #[key]
    id: Uuid,
    #[unique]
    name: String,
    #[unique]
    token_hash: Vec<u8>,
    created_at: i64,
}

#[derive(Debug, toasty::Model)]
#[table = "memberships"]
#[index(user_id, space_id)]
struct MembershipRow {
    #[key]
    space_id: Uuid,
    #[key]
    user_id: Uuid,
    role: i64,
}

fn hash_token(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}

/// Users and memberships of one server.
pub struct ServerStore {
    db: Mutex<Db>,
}

impl ServerStore {
    /// Opens `data_dir/server.db`, creating it on first use.
    pub async fn open(data_dir: &Path) -> Result<Self, ServerStoreError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("server.db");
        let fresh = !path.exists();
        let db = Db::builder()
            .models(toasty::models!(UserRow, MembershipRow))
            .build(Turso::file(&path))
            .await?;
        if fresh {
            db.push_schema().await?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(Self { db: Mutex::new(db) })
    }

    /// Creates a user and returns the one-time plaintext token.
    pub async fn create_user(&self, name: &str) -> Result<(User, String), ServerStoreError> {
        let mut db = self.db.lock().await;
        if UserRow::filter_by_name(name.to_owned())
            .first()
            .exec(&mut *db)
            .await?
            .is_some()
        {
            return Err(ServerStoreError::UserExists(name.to_owned()));
        }
        let token = random_token();
        let id = Uuid::now_v7();
        toasty::create!(UserRow {
            id,
            name: name.to_owned(),
            token_hash: hash_token(&token),
            created_at: elri_core::model::now_ms(),
        })
        .exec(&mut *db)
        .await?;
        Ok((
            User {
                id,
                name: name.to_owned(),
            },
            token,
        ))
    }

    /// Resolves a presented token to its user.
    pub async fn authenticate(&self, token: &str) -> Result<Option<User>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let row = UserRow::filter_by_token_hash(hash_token(token))
            .first()
            .exec(&mut *db)
            .await?;
        Ok(row.map(|r| User {
            id: r.id,
            name: r.name,
        }))
    }

    /// Looks a user up by name.
    pub async fn user_by_name(&self, name: &str) -> Result<Option<User>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let row = UserRow::filter_by_name(name.to_owned())
            .first()
            .exec(&mut *db)
            .await?;
        Ok(row.map(|r| User {
            id: r.id,
            name: r.name,
        }))
    }

    /// Sets a user's role in a Space.
    pub async fn set_role(
        &self,
        space: Uuid,
        user: Uuid,
        role: Role,
    ) -> Result<(), ServerStoreError> {
        let mut db = self.db.lock().await;
        MembershipRow::filter(
            MembershipRow::fields()
                .space_id()
                .eq(space)
                .and(MembershipRow::fields().user_id().eq(user)),
        )
        .delete()
        .exec(&mut *db)
        .await?;
        toasty::create!(MembershipRow {
            space_id: space,
            user_id: user,
            role: role.code(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// A user's role in a Space, if a member.
    pub async fn role(&self, space: Uuid, user: Uuid) -> Result<Option<Role>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let row = MembershipRow::filter(
            MembershipRow::fields()
                .space_id()
                .eq(space)
                .and(MembershipRow::fields().user_id().eq(user)),
        )
        .first()
        .exec(&mut *db)
        .await?;
        row.map(|r| Role::from_code(r.role).ok_or(ServerStoreError::Corrupt("role")))
            .transpose()
    }

    /// Every Space the user belongs to, with the role.
    pub async fn memberships(&self, user: Uuid) -> Result<Vec<(Uuid, Role)>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let rows = MembershipRow::filter(MembershipRow::fields().user_id().eq(user))
            .exec(&mut *db)
            .await?;
        rows.into_iter()
            .map(|r| {
                Role::from_code(r.role)
                    .map(|role| (r.space_id, role))
                    .ok_or(ServerStoreError::Corrupt("role"))
            })
            .collect()
    }
}

fn random_token() -> String {
    use rand::RngCore;
    use std::fmt::Write as _;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}
