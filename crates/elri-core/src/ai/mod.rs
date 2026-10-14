//! Model providers and agents: accounts signed in with ChatGPT or holding
//! an API key, the models they may use, and the harness that runs an
//! agent's turns against the Space through the same API the application
//! uses.

pub mod auth;
pub mod call;
pub mod harness;
pub mod openai;
pub mod skills;

use std::collections::HashMap;
use std::sync::Arc;

use crate::net::Net;
use crate::secrets::Secrets;
use crate::store::Store;

/// Where the provider lives; tests point these at a local stand-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    /// OAuth issuer; its discovery document names the other endpoints.
    pub issuer: String,
    /// Responses API base.
    pub api_base: String,
    /// Preferred loopback port for the sign-in callback; another free one
    /// is used when it is taken.
    pub callback_port: u16,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            issuer: "https://auth.openai.com".to_owned(),
            api_base: "https://api.openai.com/v1".to_owned(),
            callback_port: 1455,
        }
    }
}

/// Provider and harness failures.
#[derive(Debug, thiserror::Error)]
pub enum AiError {
    /// Storage failed.
    #[error(transparent)]
    Store(#[from] crate::store::StoreError),
    /// The credential store failed.
    #[error(transparent)]
    Secrets(#[from] crate::secrets::SecretsError),
    /// No such account, or none chosen.
    #[error("{0}")]
    NoAccount(String),
    /// The account must sign in again.
    #[error("sign in again: {0}")]
    SignIn(String),
    /// The provider refused or failed.
    #[error("{code}: {message}")]
    Provider {
        /// Provider error code, or the HTTP status.
        code: String,
        /// What it said.
        message: String,
    },
    /// The network failed.
    #[error("network: {0}")]
    Transport(String),
    /// The request does not make sense.
    #[error("{0}")]
    Invalid(String),
}

/// The device's AI hub.
pub struct Ai {
    /// Storage.
    pub store: Arc<Store>,
    /// Outgoing HTTP.
    pub net: Arc<Net>,
    /// Credential store.
    pub secrets: Secrets,
    /// Provider endpoints.
    pub endpoints: Endpoints,
    /// The agent's loopback API access.
    pub caller: call::Caller,
    attempts: std::sync::Mutex<HashMap<String, auth::Attempt>>,
    refresh: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl Ai {
    /// The hub; `loopback` and `agent_token` reach the core's own API.
    pub fn new(
        store: Arc<Store>,
        net: Arc<Net>,
        secrets: Secrets,
        endpoints: Endpoints,
        loopback: String,
        agent_token: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            net,
            secrets,
            endpoints,
            caller: call::Caller::new(loopback, agent_token),
            attempts: std::sync::Mutex::new(HashMap::new()),
            refresh: tokio::sync::Mutex::new(HashMap::new()),
        })
    }
}
