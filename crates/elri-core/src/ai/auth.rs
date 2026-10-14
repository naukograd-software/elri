//! Signing in with ChatGPT: authorization code with PKCE through the
//! system browser, a loopback callback on 127.0.0.1, registration of this
//! application as a client on first sign-in, ID token checks against the
//! provider's keys, refresh before expiry, and revocation on sign-out.
//! API keys are the other way to pay.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use elri_proto::elri::v1::ai::{Account, AccountKind, SignInState};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::{Ai, AiError};
use crate::model::now_ms;

/// Name this application registers under; the same everywhere.
pub const AGENT_NAME: &str = "elri";
/// Client id of a first registration.
const DYNAMIC_CLIENT: &str = "dynamic_agent_client";
/// Scopes asked for: identity, refresh, and use of the ChatGPT plan.
pub const SCOPES: &str =
    "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct";
/// The scope without which the plan cannot be used.
pub const PLAN_SCOPE: &str = "chatgpt.tokens.use.direct";
/// The API the tokens are for.
const RESOURCE: &str = "https://api.openai.com/v1";
/// Preference key of this host's opaque id.
const HOST_ID: &str = "ai_host_id";
/// How long a sign-in waits for the browser.
const SIGN_IN_TIMEOUT: Duration = Duration::from_secs(600);
/// Refresh this long before expiry.
const REFRESH_EARLY_MS: i64 = 5 * 60 * 1000;
/// Refresh errors after which the refresh token is useless.
const TERMINAL_REFRESH: &[&str] = &[
    "invalid_grant",
    "invalid_refresh_token",
    "token_expired",
    "refresh_token_expired",
    "refresh_token_invalidated",
    "refresh_token_reused",
];

/// One sign-in in progress or finished.
#[derive(Debug, Clone)]
pub struct Attempt {
    /// Where it stands.
    pub state: SignInState,
    /// The account, when done.
    pub account: Option<Account>,
    /// Why it failed.
    pub error: String,
}

fn key(account: &str, what: &str) -> String {
    format!("ai/{account}/{what}")
}

/// Base64url without padding.
pub fn b64url(input: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(char::from(A[(n >> 18) as usize & 63]));
        out.push(char::from(A[(n >> 12) as usize & 63]));
        if chunk.len() > 1 {
            out.push(char::from(A[(n >> 6) as usize & 63]));
        }
        if chunk.len() > 2 {
            out.push(char::from(A[n as usize & 63]));
        }
    }
    out
}

fn random_token() -> String {
    use rand::RngCore as _;
    let mut b = [0u8; 32];
    rand::rng().fill_bytes(&mut b);
    b64url(&b)
}

/// The S256 challenge of a PKCE verifier.
pub fn challenge(verifier: &str) -> String {
    b64url(&Sha256::digest(verifier.as_bytes()))
}

/// Endpoints from the issuer's discovery document.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Discovery {
    /// Issuer, compared exactly with the ID token's.
    pub issuer: String,
    /// Where the browser goes.
    pub authorization_endpoint: String,
    /// Code and refresh exchanges.
    pub token_endpoint: String,
    /// Signing keys.
    pub jwks_uri: String,
    /// Sign-out.
    #[serde(default)]
    pub revocation_endpoint: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    nonce: Option<String>,
}

fn transport(e: reqwest::Error) -> AiError {
    AiError::Transport(e.without_url().to_string())
}

/// The error code of an OAuth error answer.
async fn oauth_error(resp: reqwest::Response) -> AiError {
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    let code = body
        .get("error")
        .and_then(|e| {
            e.as_str()
                .map(str::to_owned)
                .or_else(|| e.get("code").and_then(|c| c.as_str()).map(str::to_owned))
        })
        .unwrap_or_else(|| status.as_u16().to_string());
    let message = body
        .get("error_description")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_owned();
    AiError::Provider { code, message }
}

impl Ai {
    async fn discovery(&self) -> Result<Discovery, AiError> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.endpoints.issuer.trim_end_matches('/')
        );
        let resp = self
            .net
            .client()
            .get(&url)
            .send()
            .await
            .map_err(transport)?;
        if !resp.status().is_success() {
            return Err(AiError::Provider {
                code: resp.status().as_u16().to_string(),
                message: "discovery".to_owned(),
            });
        }
        resp.json().await.map_err(transport)
    }

    async fn host_id(&self) -> Result<String, AiError> {
        if let Some(h) = self.store.pref_string(HOST_ID).await? {
            return Ok(h);
        }
        let mut bytes = [0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        let id = format!(
            "urn:uuid:{}",
            uuid::Builder::from_random_bytes(bytes).into_uuid()
        );
        self.store.set_pref_string(HOST_ID, &id).await?;
        Ok(id)
    }

    async fn label_for(&self, existing: Option<&Account>, label: &str) -> Result<String, AiError> {
        if let Some(a) = existing {
            return Ok(a.label.clone());
        }
        let l = label.trim();
        if l.is_empty() {
            return Err(AiError::Invalid("a label is required".to_owned()));
        }
        if self.store.ai_accounts().await?.iter().any(|a| a.label == l) {
            return Err(AiError::Invalid(format!("label {l} is taken")));
        }
        Ok(l.to_owned())
    }

    /// Client parameters: the issued client with hints for a sign-in
    /// again, or a first registration under this application's name.
    async fn client_params(
        &self,
        existing: Option<&Account>,
    ) -> Result<Vec<(&'static str, String)>, AiError> {
        let Some(a) = existing else {
            return Ok(vec![
                ("client_id", DYNAMIC_CLIENT.to_owned()),
                ("agent_name_hint", AGENT_NAME.to_owned()),
            ]);
        };
        let mut out = vec![("client_id", a.client_id.clone())];
        if let Some(idt) = self.secrets.get(&key(&a.id, "id")).await? {
            out.push(("id_token_hint", idt));
        }
        if !a.email.is_empty() {
            out.push(("login_hint", a.email.clone()));
        }
        Ok(out)
    }

    /// Starts a sign-in: listens on the loopback, and returns the attempt
    /// id and the URL to open in the browser.
    pub async fn start_sign_in(
        self: &Arc<Self>,
        account_id: Option<&str>,
        label: &str,
    ) -> Result<(String, String), AiError> {
        let existing = match account_id {
            Some(id) => Some(
                self.store
                    .ai_account(id)
                    .await?
                    .ok_or_else(|| AiError::NoAccount(format!("no account {id}")))?
                    .0,
            ),
            None => None,
        };
        if existing
            .as_ref()
            .is_some_and(|a| a.kind() != AccountKind::Chatgpt)
        {
            return Err(AiError::Invalid("only ChatGPT accounts sign in".to_owned()));
        }
        let label = self.label_for(existing.as_ref(), label).await?;
        let disc = self.discovery().await?;
        let host = self.host_id().await?;
        let listener = bind_callback(self.endpoints.callback_port).await?;
        let port = listener
            .local_addr()
            .map_err(|e| AiError::Transport(e.to_string()))?
            .port();
        let redirect = format!("http://127.0.0.1:{port}/auth/callback");
        let state = random_token();
        let nonce = random_token();
        let verifier = random_token();
        let mut params: Vec<(&str, String)> = vec![
            ("response_type", "code".to_owned()),
            ("redirect_uri", redirect.clone()),
            ("scope", SCOPES.to_owned()),
            ("resource", RESOURCE.to_owned()),
            ("state", state.clone()),
            ("nonce", nonce.clone()),
            ("code_challenge_method", "S256".to_owned()),
            ("code_challenge", challenge(&verifier)),
            ("ext_agent_host_id", host),
        ];
        params.extend(self.client_params(existing.as_ref()).await?);
        let url = reqwest::Url::parse_with_params(&disc.authorization_endpoint, &params)
            .map_err(|e| AiError::Invalid(format!("authorization endpoint: {e}")))?;
        let attempt = uuid::Uuid::now_v7().to_string();
        self.attempts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                attempt.clone(),
                Attempt {
                    state: SignInState::Pending,
                    account: None,
                    error: String::new(),
                },
            );
        let me = Arc::clone(self);
        let flow = Flow {
            disc,
            existing,
            label,
            redirect,
            state,
            nonce,
            verifier,
        };
        let attempt_id = attempt.clone();
        tokio::spawn(async move {
            let outcome = tokio::time::timeout(SIGN_IN_TIMEOUT, me.await_callback(listener, flow))
                .await
                .unwrap_or_else(|_| {
                    Err(AiError::SignIn(
                        "the browser did not answer in time".to_owned(),
                    ))
                });
            let finished = match outcome {
                Ok(account) => Attempt {
                    state: SignInState::Done,
                    account: Some(account),
                    error: String::new(),
                },
                Err(e) => Attempt {
                    state: SignInState::Failed,
                    account: None,
                    error: e.to_string(),
                },
            };
            me.attempts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(attempt_id, finished);
        });
        Ok((attempt, url.to_string()))
    }

    /// Where a sign-in stands.
    pub fn sign_in_state(&self, attempt: &str) -> Option<Attempt> {
        self.attempts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(attempt)
            .cloned()
    }

    async fn await_callback(
        &self,
        listener: tokio::net::TcpListener,
        flow: Flow,
    ) -> Result<Account, AiError> {
        loop {
            let (mut sock, _) = listener
                .accept()
                .await
                .map_err(|e| AiError::Transport(e.to_string()))?;
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]).into_owned();
            let target = head
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("")
                .to_owned();
            if !target.starts_with("/auth/callback") {
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                continue;
            }
            let query: HashMap<String, String> =
                reqwest::Url::parse(&format!("http://127.0.0.1{target}"))
                    .map(|u| u.query_pairs().into_owned().collect())
                    .unwrap_or_default();
            let result = self.finish(&flow, &query).await;
            let page = match &result {
                Ok(_) => {
                    "<h1>Signed in</h1><p>You can close this tab and return to elri.</p>".to_owned()
                }
                Err(e) => format!(
                    "<h1>Sign-in failed</h1><p>{}</p>",
                    e.to_string().replace('<', "&lt;")
                ),
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
                page.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            return result;
        }
    }

    async fn finish(&self, flow: &Flow, q: &HashMap<String, String>) -> Result<Account, AiError> {
        if q.get("state") != Some(&flow.state) {
            return Err(AiError::SignIn(
                "the answer does not match this sign-in".to_owned(),
            ));
        }
        if let Some(e) = q.get("error") {
            return Err(AiError::SignIn(if e == "access_denied" {
                "access was denied".to_owned()
            } else {
                e.clone()
            }));
        }
        let code = q
            .get("code")
            .ok_or_else(|| AiError::SignIn("no authorization code".to_owned()))?;
        let client_id = match (&flow.existing, q.get("client_id")) {
            (Some(a), Some(c)) if c != &a.client_id => {
                return Err(AiError::SignIn(
                    "the provider named a different client".to_owned(),
                ))
            }
            (Some(a), _) => a.client_id.clone(),
            (None, Some(c)) if !c.is_empty() => c.clone(),
            (None, _) => {
                return Err(AiError::SignIn(
                    "registration did not complete: no client id".to_owned(),
                ))
            }
        };
        let resp = self
            .net
            .client()
            .post(&flow.disc.token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", client_id.as_str()),
                ("code", code.as_str()),
                ("code_verifier", flow.verifier.as_str()),
                ("redirect_uri", flow.redirect.as_str()),
                ("resource", RESOURCE),
            ])
            .send()
            .await
            .map_err(transport)?;
        if !resp.status().is_success() {
            return Err(oauth_error(resp).await);
        }
        let tokens: TokenResponse = resp.json().await.map_err(transport)?;
        let id_token = tokens
            .id_token
            .clone()
            .ok_or_else(|| AiError::SignIn("no ID token".to_owned()))?;
        let claims = self
            .verify_id_token(&flow.disc, &id_token, &client_id, Some(&flow.nonce))
            .await?;
        let scopes: Vec<String> = tokens
            .scope
            .clone()
            .or_else(|| q.get("scope").cloned())
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if !scopes.iter().any(|s| s == PLAN_SCOPE) {
            return Err(AiError::SignIn(
                "ChatGPT plan usage was not granted; use an API key instead".to_owned(),
            ));
        }
        if let Some(a) = &flow.existing {
            if !a.subject.is_empty() && a.subject != claims.sub {
                return Err(AiError::SignIn(
                    "a different ChatGPT account signed in; add it as a new account".to_owned(),
                ));
            }
        }
        let account = Account {
            id: flow.existing.as_ref().map_or_else(
                || crate::store::ai::new_account_id().to_string(),
                |a| a.id.clone(),
            ),
            kind: AccountKind::Chatgpt.into(),
            label: flow.label.clone(),
            email: claims.email.unwrap_or_default(),
            subject: claims.sub,
            client_id,
            scopes,
            needs_sign_in: false,
            created_at_ms: flow
                .existing
                .as_ref()
                .map_or_else(now_ms, |a| a.created_at_ms),
        };
        self.save_tokens(&account.id, &tokens, Some(&id_token))
            .await?;
        let expires = now_ms() + tokens.expires_in.unwrap_or(3600) * 1000;
        self.store.ai_account_put(&account, expires).await?;
        Ok(account)
    }

    async fn save_tokens(
        &self,
        id: &str,
        t: &TokenResponse,
        id_token: Option<&str>,
    ) -> Result<(), AiError> {
        self.secrets
            .set(&key(id, "access"), &t.access_token)
            .await?;
        if let Some(r) = &t.refresh_token {
            self.secrets.set(&key(id, "refresh"), r).await?;
        }
        if let Some(i) = id_token {
            self.secrets.set(&key(id, "id"), i).await?;
        }
        Ok(())
    }

    async fn verify_id_token(
        &self,
        disc: &Discovery,
        token: &str,
        client_id: &str,
        nonce: Option<&str>,
    ) -> Result<Claims, AiError> {
        let header = jsonwebtoken::decode_header(token)
            .map_err(|e| AiError::SignIn(format!("ID token: {e}")))?;
        let jwks: jsonwebtoken::jwk::JwkSet = self
            .net
            .client()
            .get(&disc.jwks_uri)
            .send()
            .await
            .map_err(transport)?
            .json()
            .await
            .map_err(transport)?;
        let jwk = match header.kid.as_deref() {
            Some(kid) => jwks.find(kid),
            None => jwks.keys.first(),
        }
        .ok_or_else(|| AiError::SignIn("ID token signed with an unknown key".to_owned()))?;
        let key = jsonwebtoken::DecodingKey::from_jwk(jwk)
            .map_err(|e| AiError::SignIn(format!("signing key: {e}")))?;
        let mut v = jsonwebtoken::Validation::new(header.alg);
        v.set_issuer(&[disc.issuer.as_str()]);
        v.set_audience(&[client_id]);
        let data = jsonwebtoken::decode::<Claims>(token, &key, &v)
            .map_err(|e| AiError::SignIn(format!("ID token: {e}")))?;
        if let Some(n) = nonce {
            if data.claims.nonce.as_deref() != Some(n) {
                return Err(AiError::SignIn("ID token nonce does not match".to_owned()));
            }
        }
        Ok(data.claims)
    }

    /// Adds an account that pays with an OpenAI API key.
    pub async fn add_api_key(&self, label: &str, api_key: &str) -> Result<Account, AiError> {
        let label = label.trim();
        if label.is_empty() || api_key.trim().is_empty() {
            return Err(AiError::Invalid(
                "a label and a key are required".to_owned(),
            ));
        }
        if self
            .store
            .ai_accounts()
            .await?
            .iter()
            .any(|a| a.label == label)
        {
            return Err(AiError::Invalid(format!("label {label} is taken")));
        }
        let account = Account {
            id: crate::store::ai::new_account_id().to_string(),
            kind: AccountKind::ApiKey.into(),
            label: label.to_owned(),
            created_at_ms: now_ms(),
            ..Account::default()
        };
        self.secrets
            .set(&key(&account.id, "key"), api_key.trim())
            .await?;
        self.store.ai_account_put(&account, i64::MAX).await?;
        Ok(account)
    }

    async fn lock_for(&self, id: &str) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.refresh
                .lock()
                .await
                .entry(id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// A bearer credential for the account, refreshed when near expiry.
    pub async fn access_token(&self, id: &str) -> Result<String, AiError> {
        let (account, expires) = self
            .store
            .ai_account(id)
            .await?
            .ok_or_else(|| AiError::NoAccount(format!("no account {id}")))?;
        if account.kind() == AccountKind::ApiKey {
            return self
                .secrets
                .get(&key(id, "key"))
                .await?
                .ok_or_else(|| AiError::SignIn("the API key is gone; add it again".to_owned()));
        }
        if account.needs_sign_in {
            return Err(AiError::SignIn(format!(
                "account {} is signed out",
                account.label
            )));
        }
        if expires - now_ms() > REFRESH_EARLY_MS {
            if let Some(t) = self.secrets.get(&key(id, "access")).await? {
                return Ok(t);
            }
        }
        self.refresh(id, false).await
    }

    /// Refreshes the account's tokens, one refresh at a time per account.
    /// `forced` refreshes even when the stored token still looks valid.
    pub async fn refresh(&self, id: &str, forced: bool) -> Result<String, AiError> {
        let lock = self.lock_for(id).await;
        let _held = lock.lock().await;
        let (mut account, expires) = self
            .store
            .ai_account(id)
            .await?
            .ok_or_else(|| AiError::NoAccount(format!("no account {id}")))?;
        // Another caller may have refreshed while this one waited.
        if !forced && expires - now_ms() > REFRESH_EARLY_MS {
            if let Some(t) = self.secrets.get(&key(id, "access")).await? {
                return Ok(t);
            }
        }
        let Some(refresh) = self.secrets.get(&key(id, "refresh")).await? else {
            return Err(self.signed_out(&mut account, "no refresh token").await);
        };
        let disc = self.discovery().await?;
        let resp = self
            .net
            .client()
            .post(&disc.token_endpoint)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", account.client_id.as_str()),
                ("refresh_token", refresh.as_str()),
                ("resource", RESOURCE),
            ])
            .send()
            .await
            .map_err(transport)?;
        if !resp.status().is_success() {
            let err = oauth_error(resp).await;
            if let AiError::Provider { code, .. } = &err {
                if TERMINAL_REFRESH.contains(&code.as_str()) {
                    return Err(self.signed_out(&mut account, code).await);
                }
            }
            return Err(err);
        }
        let tokens: TokenResponse = resp.json().await.map_err(transport)?;
        self.save_tokens(id, &tokens, tokens.id_token.as_deref())
            .await?;
        if let Some(s) = &tokens.scope {
            account.scopes = s.split_whitespace().map(str::to_owned).collect();
        }
        let expires = now_ms() + tokens.expires_in.unwrap_or(3600) * 1000;
        self.store.ai_account_put(&account, expires).await?;
        Ok(tokens.access_token)
    }

    async fn signed_out(&self, account: &mut Account, why: &str) -> AiError {
        for what in ["access", "refresh"] {
            let _ = self.secrets.delete(&key(&account.id, what)).await;
        }
        account.needs_sign_in = true;
        let _ = self.store.ai_account_put(account, 0).await;
        AiError::SignIn(format!("{}: {why}", account.label))
    }

    /// Signs out: revokes the refresh token where the provider allows and
    /// clears the credentials. Returns whether the revocation was confirmed.
    pub async fn sign_out(&self, id: &str) -> Result<bool, AiError> {
        let (mut account, _) = self
            .store
            .ai_account(id)
            .await?
            .ok_or_else(|| AiError::NoAccount(format!("no account {id}")))?;
        if account.kind() == AccountKind::ApiKey {
            self.secrets.delete(&key(id, "key")).await?;
            self.store.ai_account_delete(id).await?;
            return Ok(true);
        }
        let lock = self.lock_for(id).await;
        let _held = lock.lock().await;
        let mut revoked = false;
        if let Some(refresh) = self.secrets.get(&key(id, "refresh")).await? {
            if let Ok(disc) = self.discovery().await {
                if let Some(endpoint) = &disc.revocation_endpoint {
                    for attempt in 0..3u32 {
                        let sent = self
                            .net
                            .client()
                            .post(endpoint)
                            .form(&[
                                ("token", refresh.as_str()),
                                ("token_type_hint", "refresh_token"),
                                ("client_id", account.client_id.as_str()),
                            ])
                            .send()
                            .await;
                        match sent {
                            Ok(r) if r.status().is_success() => {
                                revoked = true;
                                break;
                            }
                            Ok(r) if r.status().is_client_error() => break,
                            _ => tokio::time::sleep(Duration::from_millis(500 << attempt)).await,
                        }
                    }
                }
            }
        }
        for what in ["access", "refresh", "id"] {
            self.secrets.delete(&key(id, what)).await?;
        }
        account.needs_sign_in = true;
        self.store.ai_account_put(&account, 0).await?;
        Ok(revoked)
    }
}

/// The loopback listener of the callback: the preferred port, or any.
async fn bind_callback(port: u16) -> Result<tokio::net::TcpListener, AiError> {
    if let Ok(l) = tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
        return Ok(l);
    }
    tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|e| AiError::Transport(e.to_string()))
}

struct Flow {
    disc: Discovery,
    existing: Option<Account>,
    label: String,
    redirect: String,
    state: String,
    nonce: String,
    verifier: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_matches_the_rfc_example() {
        // RFC 7636 appendix B.
        assert_eq!(
            challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        assert_eq!(b64url(b"\xfb\xff"), "-_8");
    }
}
