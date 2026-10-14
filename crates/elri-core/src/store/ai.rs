//! Model provider accounts and small string preferences in the device
//! database.

use elri_proto::elri::v1::ai::{Account, AccountKind};
use uuid::Uuid;

use super::models::{AiAccountRow, PrefRow};
use super::{uuid_arg, Store, StoreError};

fn account_of(r: &AiAccountRow) -> Account {
    Account {
        id: r.id.to_string(),
        kind: i32::try_from(r.kind).unwrap_or(0),
        label: r.label.clone(),
        email: r.email.clone(),
        subject: r.subject.clone(),
        client_id: r.client_id.clone(),
        scopes: r.scopes.split_whitespace().map(str::to_owned).collect(),
        needs_sign_in: r.needs_sign_in,
        created_at_ms: r.created_at,
    }
}

impl Store {
    /// Every account, oldest first.
    pub async fn ai_accounts(&self) -> Result<Vec<Account>, StoreError> {
        let mut db = self.device.lock().await;
        let mut rows = AiAccountRow::all().exec(&mut *db).await?;
        rows.sort_by_key(|r| (r.created_at, r.id));
        Ok(rows.iter().map(account_of).collect())
    }

    /// One account and its access token expiry.
    pub async fn ai_account(&self, id: &str) -> Result<Option<(Account, i64)>, StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        Ok(AiAccountRow::filter_by_id(id)
            .first()
            .exec(&mut *db)
            .await?
            .map(|r| (account_of(&r), r.expires_at)))
    }

    /// Writes an account, replacing the one with its id.
    pub async fn ai_account_put(&self, a: &Account, expires_at: i64) -> Result<(), StoreError> {
        let id = uuid_arg(&a.id)?;
        let mut db = self.device.lock().await;
        AiAccountRow::filter_by_id(id)
            .delete()
            .exec(&mut *db)
            .await?;
        toasty::create!(AiAccountRow {
            id,
            kind: i64::from(a.kind),
            label: a.label.clone(),
            email: a.email.clone(),
            subject: a.subject.clone(),
            client_id: a.client_id.clone(),
            scopes: a.scopes.join(" "),
            expires_at,
            needs_sign_in: a.needs_sign_in,
            created_at: a.created_at_ms,
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Removes an account record.
    pub async fn ai_account_delete(&self, id: &str) -> Result<(), StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        AiAccountRow::filter_by_id(id)
            .delete()
            .exec(&mut *db)
            .await?;
        Ok(())
    }

    /// A string preference, if set.
    pub async fn pref_string(&self, key: &str) -> Result<Option<String>, StoreError> {
        let mut db = self.device.lock().await;
        Ok(PrefRow::filter_by_key(key.to_owned())
            .first()
            .exec(&mut *db)
            .await?
            .map(|r| String::from_utf8_lossy(&r.value).into_owned()))
    }

    /// Sets a string preference.
    pub async fn set_pref_string(&self, key: &str, value: &str) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        PrefRow::filter_by_key(key.to_owned())
            .delete()
            .exec(&mut *db)
            .await?;
        toasty::create!(PrefRow {
            key: key.to_owned(),
            value: value.as_bytes().to_vec(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }
}

/// A fresh random account id.
pub fn new_account_id() -> Uuid {
    Uuid::now_v7()
}

/// Kind of an account.
pub fn kind_of(a: &Account) -> AccountKind {
    a.kind()
}
