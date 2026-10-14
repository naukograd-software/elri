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
    /// No such token.
    #[error("token {0} not found")]
    TokenNotFound(String),
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
    /// May create users and manage anyone's tokens.
    pub admin: bool,
    /// Creation instant.
    pub created_at: i64,
}

/// An access token of a user, without its secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// Identity.
    pub id: Uuid,
    /// Owning user.
    pub user_id: Uuid,
    /// Free text chosen at issue.
    pub label: String,
    /// Issue instant.
    pub created_at: i64,
    /// Last authenticated use, if any.
    pub last_used_at: Option<i64>,
    /// The device that last said hello with it.
    pub last_device_id: String,
    /// Revocation instant, if revoked.
    pub revoked_at: Option<i64>,
}

/// An authenticated caller: the user and the token that was presented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    /// The user.
    pub user: User,
    /// The token used.
    pub token_id: Uuid,
}

#[derive(Debug, toasty::Model)]
#[table = "users"]
struct UserRow {
    #[key]
    id: Uuid,
    #[unique]
    name: String,
    admin: bool,
    created_at: i64,
}

#[derive(Debug, toasty::Model)]
#[table = "tokens"]
struct TokenRow {
    #[key]
    id: Uuid,
    #[unique]
    token_hash: Vec<u8>,
    #[index]
    user_id: Uuid,
    label: String,
    created_at: i64,
    last_used_at: Option<i64>,
    last_device_id: String,
    revoked_at: Option<i64>,
}

fn user_of(r: &UserRow) -> User {
    User {
        id: r.id,
        name: r.name.clone(),
        admin: r.admin,
        created_at: r.created_at,
    }
}

fn token_of(r: &TokenRow) -> Token {
    Token {
        id: r.id,
        user_id: r.user_id,
        label: r.label.clone(),
        created_at: r.created_at,
        last_used_at: r.last_used_at,
        last_device_id: r.last_device_id.clone(),
        revoked_at: r.revoked_at,
    }
}

/// `last_used_at` is refreshed at most this often, to keep authentication
/// read-mostly.
const USE_GRANULARITY_MS: i64 = 60_000;

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
            .models(toasty::models!(UserRow, TokenRow, MembershipRow))
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

    /// Creates a user with a first token and returns the token's one-time
    /// secret.
    pub async fn create_user(
        &self,
        name: &str,
        admin: bool,
        label: &str,
    ) -> Result<(User, Token, String), ServerStoreError> {
        let name = name.trim();
        let mut db = self.db.lock().await;
        if UserRow::filter_by_name(name.to_owned())
            .first()
            .exec(&mut *db)
            .await?
            .is_some()
        {
            return Err(ServerStoreError::UserExists(name.to_owned()));
        }
        let id = Uuid::now_v7();
        let created_at = elri_core::model::now_ms();
        toasty::create!(UserRow {
            id,
            name: name.to_owned(),
            admin,
            created_at,
        })
        .exec(&mut *db)
        .await?;
        let (token, secret) = issue(&mut db, id, label).await?;
        Ok((
            User {
                id,
                name: name.to_owned(),
                admin,
                created_at,
            },
            token,
            secret,
        ))
    }

    /// Issues one more token for a user; the secret is returned once.
    pub async fn issue_token(
        &self,
        user: Uuid,
        label: &str,
    ) -> Result<(Token, String), ServerStoreError> {
        let mut db = self.db.lock().await;
        if UserRow::filter_by_id(user)
            .first()
            .exec(&mut *db)
            .await?
            .is_none()
        {
            return Err(ServerStoreError::UserNotFound(user.to_string()));
        }
        issue(&mut db, user, label).await
    }

    /// Resolves a presented secret to its user and token. Revoked tokens do
    /// not authenticate.
    pub async fn authenticate(&self, secret: &str) -> Result<Option<Auth>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let Some(row) = TokenRow::filter_by_token_hash(hash_token(secret))
            .first()
            .exec(&mut *db)
            .await?
        else {
            return Ok(None);
        };
        if row.revoked_at.is_some() {
            return Ok(None);
        }
        let Some(user) = UserRow::filter_by_id(row.user_id)
            .first()
            .exec(&mut *db)
            .await?
        else {
            return Err(ServerStoreError::Corrupt("token without user"));
        };
        let now = elri_core::model::now_ms();
        if row
            .last_used_at
            .is_none_or(|t| now - t >= USE_GRANULARITY_MS)
        {
            toasty::update!(TokenRow::filter_by_id(row.id) {
                last_used_at: Some(now),
            })
            .exec(&mut *db)
            .await?;
        }
        Ok(Some(Auth {
            user: user_of(&user),
            token_id: row.id,
        }))
    }

    /// Records which device last presented a token.
    pub async fn note_device(&self, token: Uuid, device_id: &str) -> Result<(), ServerStoreError> {
        let mut db = self.db.lock().await;
        toasty::update!(TokenRow::filter_by_id(token) {
            last_device_id: device_id.to_owned(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Looks a token up by id.
    pub async fn token(&self, id: Uuid) -> Result<Token, ServerStoreError> {
        let mut db = self.db.lock().await;
        TokenRow::filter_by_id(id)
            .first()
            .exec(&mut *db)
            .await?
            .as_ref()
            .map(token_of)
            .ok_or_else(|| ServerStoreError::TokenNotFound(id.to_string()))
    }

    /// A user's tokens, newest first.
    pub async fn tokens(
        &self,
        user: Uuid,
        include_revoked: bool,
    ) -> Result<Vec<Token>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let rows = TokenRow::filter(TokenRow::fields().user_id().eq(user))
            .exec(&mut *db)
            .await?;
        let mut out: Vec<Token> = rows
            .iter()
            .filter(|r| include_revoked || r.revoked_at.is_none())
            .map(token_of)
            .collect();
        out.sort_by_key(|t| std::cmp::Reverse(t.created_at));
        Ok(out)
    }

    /// Revokes a token; it stops authenticating at once. Revoking twice is
    /// a no-op.
    pub async fn revoke_token(&self, id: Uuid) -> Result<Token, ServerStoreError> {
        let mut db = self.db.lock().await;
        let Some(row) = TokenRow::filter_by_id(id).first().exec(&mut *db).await? else {
            return Err(ServerStoreError::TokenNotFound(id.to_string()));
        };
        if row.revoked_at.is_none() {
            toasty::update!(TokenRow::filter_by_id(id) {
                revoked_at: Some(elri_core::model::now_ms()),
            })
            .exec(&mut *db)
            .await?;
        }
        self_token(&mut db, id).await
    }

    /// Looks a user up by name.
    pub async fn user_by_name(&self, name: &str) -> Result<Option<User>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let row = UserRow::filter_by_name(name.trim().to_owned())
            .first()
            .exec(&mut *db)
            .await?;
        Ok(row.as_ref().map(user_of))
    }

    /// Every user, oldest first.
    pub async fn users(&self) -> Result<Vec<User>, ServerStoreError> {
        let mut db = self.db.lock().await;
        let rows = UserRow::all()
            .order_by(UserRow::fields().created_at().asc())
            .exec(&mut *db)
            .await?;
        Ok(rows.iter().map(user_of).collect())
    }

    /// Grants or removes the admin flag.
    pub async fn set_admin(&self, user: Uuid, admin: bool) -> Result<User, ServerStoreError> {
        let mut db = self.db.lock().await;
        toasty::update!(UserRow::filter_by_id(user) { admin })
            .exec(&mut *db)
            .await?;
        UserRow::filter_by_id(user)
            .first()
            .exec(&mut *db)
            .await?
            .as_ref()
            .map(user_of)
            .ok_or_else(|| ServerStoreError::UserNotFound(user.to_string()))
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

async fn self_token(db: &mut Db, id: Uuid) -> Result<Token, ServerStoreError> {
    TokenRow::filter_by_id(id)
        .first()
        .exec(&mut *db)
        .await?
        .as_ref()
        .map(token_of)
        .ok_or_else(|| ServerStoreError::TokenNotFound(id.to_string()))
}

async fn issue(db: &mut Db, user: Uuid, label: &str) -> Result<(Token, String), ServerStoreError> {
    let secret = random_token();
    let token = Token {
        id: Uuid::now_v7(),
        user_id: user,
        label: label.trim().to_owned(),
        created_at: elri_core::model::now_ms(),
        last_used_at: None,
        last_device_id: String::new(),
        revoked_at: None,
    };
    toasty::create!(TokenRow {
        id: token.id,
        token_hash: hash_token(&secret),
        user_id: user,
        label: token.label.clone(),
        created_at: token.created_at,
        last_used_at: None,
        last_device_id: String::new(),
        revoked_at: None,
    })
    .exec(&mut *db)
    .await?;
    Ok((token, secret))
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
