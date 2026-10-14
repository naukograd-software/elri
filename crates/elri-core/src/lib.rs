//! elri core: the local core embedded in every application and the library
//! behind the server. It owns the graph, storage, synchronization,
//! authorization, and script execution. It knows nothing about any shell.
//!
//! | module   | responsibility |
//! |----------|----------------|
//! | `api`    | ws-proto handlers and the single authorization dispatcher |
//! | `device` | device identity and the per-launch endpoint token |
//! | `model`  | Objects, Loro containers, definitions, publications |
//! | `store`  | Turso per Space, device database, projections |
//! | `change` | Change, dependency DAG, outbox, commit coordinator |
//! | `sync`   | wire conversions of Changes |
//! | `remote` | server link and per-Space replication workers |
//! | `search` | lexical and semantic indexes |
//! | `script` | Luau runtime, WASM host, native extensions |

pub mod api;
pub mod change;
pub mod device;
pub mod model;
pub mod remote;
pub mod script;
pub mod search;
pub mod secrets;
pub mod store;
pub mod sync;

mod core;

pub use crate::core::{BuildInfo, Config, Core, Endpoint};
