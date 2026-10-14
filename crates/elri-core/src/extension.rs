//! Extensions installed on this device: WebAssembly modules run under
//! wasmtime with fuel and a memory limit, and, on desktop, native dynamic
//! libraries with a small C ABI. Both expose functions that take and
//! return JSON, and both run only under a grant for their exact bytes.
//!
//! WASM ABI. The module exports a linear `memory`, `alloc(len: i32) ->
//! i32` for buffers the host hands it, and one function per declared
//! name with the signature `(ptr: i32, len: i32) -> i64`: the arguments
//! are JSON in memory, the result is the pointer and length of JSON in
//! memory packed as `(ptr << 32) | len`. The module may import
//! `elri.call(ptr: i32, len: i32) -> i64` to run a core operation (see
//! `hostops`): the request is JSON, the reply is JSON placed by `alloc`.
//!
//! Native ABI. The library exports `elri_ext_call(const uint8_t *req,
//! size_t req_len, uint8_t **out, size_t *out_len) -> int32_t` where the
//! request is JSON `{"function", "args", "space"}`, the reply JSON is
//! written to a buffer the library owns, and `elri_ext_free(uint8_t *,
//! size_t)` releases it. A native extension gets no host callback: it is
//! a client of the loopback API like any other process.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::time::Instant;
use tokio::sync::Mutex;

use elri_proto::elri::v1::extension::{Extension, ExtensionKind};
use elri_proto::elri::v1::script::{Manifest, RunResult, Script};
use serde_json::{json, Value as Json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::hostops::Session;
use crate::model::{ObjectId, SpaceId};
use crate::script;
use crate::store::{ExtensionRecord, Store, StoreError};

/// Fuel a call may burn: roughly instructions.
const FUEL: u64 = 50_000_000;
/// Linear memory a module may grow to.
const MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Largest module accepted.
pub const MAX_MODULE_BYTES: usize = 64 * 1024 * 1024;
/// Host calls one invocation may make.
const CALL_LIMIT: u32 = 10_000;

/// Extension failures.
#[derive(Debug, thiserror::Error)]
pub enum ExtensionError {
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
    /// The module is not something this device can run.
    #[error("module: {0}")]
    Module(String),
    /// No such extension or function.
    #[error("{0}")]
    NotFound(String),
    /// Running it failed.
    #[error("{0}")]
    Run(String),
    /// Native code is not loaded on this platform.
    #[error("native extensions run on desktop only")]
    NativeUnsupported,
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> ExtensionError + '_ {
    move |source| ExtensionError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Hex SHA-256 of module bytes.
pub fn version_of(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// The grant identity of an extension: a stable id derived from its name,
/// in no Space.
pub fn grant_identity(name: &str) -> ObjectId {
    ObjectId::from_uuid(Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes()))
}

/// The script-shaped identity grants are checked against.
pub fn as_script(rec: &ExtensionRecord) -> Script {
    let manifest = script::manifest_from_json(&rec.manifest).unwrap_or_default();
    let benign = rec.kind == ExtensionKind::Wasm as i64
        && manifest.reads.is_empty()
        && manifest.writes.is_empty()
        && !manifest.http
        && !manifest.ai
        && manifest.extensions.is_empty();
    Script {
        id: grant_identity(&rec.name).to_string(),
        space_id: Uuid::nil().to_string(),
        name: rec.name.clone(),
        source: String::new(),
        manifest: Some(manifest),
        version: rec.version.clone(),
        benign,
        created_at_ms: rec.installed_at,
        updated_at_ms: rec.installed_at,
        deleted_at_ms: None,
    }
}

/// Where this device keeps extension modules.
#[derive(Debug, Clone)]
pub struct Modules {
    root: PathBuf,
}

impl Modules {
    /// Under `data_dir/extensions`.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            root: data_dir.join("extensions"),
        }
    }

    /// Path of a module by name and version.
    pub fn path(&self, name: &str, version: &str, kind: ExtensionKind) -> PathBuf {
        let ext = match kind {
            ExtensionKind::Native => native_extension(),
            _ => "wasm",
        };
        self.root.join(name).join(format!("{version}.{ext}"))
    }

    /// Writes module bytes into place.
    pub fn store(
        &self,
        name: &str,
        kind: ExtensionKind,
        bytes: &[u8],
    ) -> Result<String, ExtensionError> {
        let version = version_of(bytes);
        let path = self.path(name, &version, kind);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(io(parent))?;
        }
        std::fs::write(&path, bytes).map_err(io(&path))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(version)
    }

    /// Removes every module of an extension.
    pub fn remove(&self, name: &str) -> Result<(), ExtensionError> {
        let dir = self.root.join(name);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(ExtensionError::Io { path: dir, source }),
        }
    }
}

fn native_extension() -> &'static str {
    if cfg!(target_os = "windows") {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    }
}

/// A name is a short identifier.
pub fn check_name(name: &str) -> Result<(), ExtensionError> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(ExtensionError::Module(
            "name must be 1-64 of [A-Za-z0-9_-]".to_owned(),
        ))
    }
}

/// Checks a WASM module compiles and exports what it claims.
pub fn check_wasm(bytes: &[u8], functions: &[String]) -> Result<(), ExtensionError> {
    let engine = engine()?;
    let module =
        wasmtime::Module::new(&engine, bytes).map_err(|e| ExtensionError::Module(e.to_string()))?;
    let exports: Vec<String> = module.exports().map(|e| e.name().to_owned()).collect();
    for required in ["memory", "alloc"] {
        if !exports.iter().any(|e| e == required) {
            return Err(ExtensionError::Module(format!(
                "module does not export {required}"
            )));
        }
    }
    for f in functions {
        if !exports.iter().any(|e| e == f) {
            return Err(ExtensionError::Module(format!(
                "module does not export {f}"
            )));
        }
    }
    Ok(())
}

fn engine() -> Result<wasmtime::Engine, ExtensionError> {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);
    wasmtime::Engine::new(&config).map_err(|e| ExtensionError::Module(e.to_string()))
}

/// Per-call state behind the wasmtime store.
struct Env {
    session: Arc<Mutex<Session>>,
    limits: wasmtime::StoreLimits,
    handle: tokio::runtime::Handle,
}

/// The protocol view of an installed extension.
pub fn extension_of(rec: &ExtensionRecord, granted: bool) -> Extension {
    let functions: Vec<String> = serde_json::from_str(&rec.functions).unwrap_or_default();
    let s = as_script(rec);
    Extension {
        name: rec.name.clone(),
        kind: i32::try_from(rec.kind).unwrap_or_default(),
        version: rec.version.clone(),
        functions,
        manifest: s.manifest,
        benign: s.benign,
        granted,
        installed_at_ms: rec.installed_at,
    }
}

/// Runs one function of an installed extension.
pub async fn call(
    store: Arc<Store>,
    modules: &Modules,
    rec: &ExtensionRecord,
    function: &str,
    space: SpaceId,
    args: Json,
    grant_id: String,
) -> RunResult {
    let started = Instant::now();
    let functions: Vec<String> = serde_json::from_str(&rec.functions).unwrap_or_default();
    let manifest = script::manifest_from_json(&rec.manifest).unwrap_or_default();
    let session = Arc::new(Mutex::new(Session {
        store: Arc::clone(&store),
        own: space,
        manifest,
        actor: format!("extension:{}", rec.name),
        labels: std::collections::BTreeSet::default(),
        logs: Vec::new(),
        calls: 0,
        call_limit: CALL_LIMIT,
    }));
    let outcome: Result<Json, ExtensionError> = if !functions.iter().any(|f| f == function) {
        Err(ExtensionError::NotFound(format!(
            "extension {} has no function {function}",
            rec.name
        )))
    } else if rec.kind == ExtensionKind::Native as i64 {
        call_native(
            modules.path(&rec.name, &rec.version, ExtensionKind::Native),
            function,
            space,
            args,
        )
        .await
    } else {
        let path = modules.path(&rec.name, &rec.version, ExtensionKind::Wasm);
        let session = Arc::clone(&session);
        let function = function.to_owned();
        tokio::task::spawn_blocking(move || call_wasm(&path, &function, &args, session))
            .await
            .map_err(|e| ExtensionError::Run(e.to_string()))
            .and_then(|r| r)
    };
    let guard = session.lock().await;
    let (output_json, error) = match outcome {
        Ok(Json::Null) => (String::new(), String::new()),
        Ok(v) => (v.to_string(), String::new()),
        Err(e) => (String::new(), e.to_string()),
    };
    RunResult {
        output_json,
        logs: guard.logs.clone(),
        duration_ms: i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
        error,
        labels: guard.labels.iter().cloned().collect(),
        grant_id,
    }
}

#[allow(clippy::too_many_lines)]
fn call_wasm(
    path: &Path,
    function: &str,
    args: &Json,
    session: Arc<Mutex<Session>>,
) -> Result<Json, ExtensionError> {
    let engine = engine()?;
    let bytes = std::fs::read(path).map_err(io(path))?;
    let module = wasmtime::Module::new(&engine, &bytes)
        .map_err(|e| ExtensionError::Module(e.to_string()))?;
    let handle = tokio::runtime::Handle::current();
    let env = Env {
        session,
        limits: wasmtime::StoreLimitsBuilder::new()
            .memory_size(MEMORY_LIMIT)
            .instances(1)
            .build(),
        handle,
    };
    let mut store = wasmtime::Store::new(&engine, env);
    store.limiter(|env| &mut env.limits);
    store
        .set_fuel(FUEL)
        .map_err(|e| ExtensionError::Run(e.to_string()))?;

    let mut linker = wasmtime::Linker::new(&engine);
    linker
        .func_wrap(
            "elri",
            "call",
            |mut caller: wasmtime::Caller<'_, Env>, ptr: i32, len: i32| -> wasmtime::Result<i64> {
                let memory = caller
                    .get_export("memory")
                    .and_then(wasmtime::Extern::into_memory)
                    .ok_or_else(|| wasmtime::Error::msg("no memory export"))?;
                let alloc = caller
                    .get_export("alloc")
                    .and_then(wasmtime::Extern::into_func)
                    .ok_or_else(|| wasmtime::Error::msg("no alloc export"))?
                    .typed::<i32, i32>(&caller)?;
                let start = usize::try_from(ptr).map_err(wasmtime::Error::msg)?;
                let n = usize::try_from(len).map_err(wasmtime::Error::msg)?;
                let data = memory.data(&caller);
                let req: Json = serde_json::from_slice(
                    data.get(start..start + n)
                        .ok_or_else(|| wasmtime::Error::msg("request out of bounds"))?,
                )
                .map_err(wasmtime::Error::msg)?;
                let session = Arc::clone(&caller.data().session);
                let handle = caller.data().handle.clone();
                // This runs on a blocking thread, so driving the operation
                // to completion here is allowed.
                let reply = handle.block_on(async {
                    let mut s = session.lock().await;
                    s.call(&req).await
                });
                let out = reply.to_string().into_bytes();
                let out_len = i32::try_from(out.len()).map_err(wasmtime::Error::msg)?;
                let out_ptr = alloc.call(&mut caller, out_len)?;
                let memory_mut = memory.data_mut(&mut caller);
                let o = usize::try_from(out_ptr).map_err(wasmtime::Error::msg)?;
                memory_mut
                    .get_mut(o..o + out.len())
                    .ok_or_else(|| wasmtime::Error::msg("reply out of bounds"))?
                    .copy_from_slice(&out);
                Ok((i64::from(out_ptr) << 32) | i64::from(out_len))
            },
        )
        .map_err(|e| ExtensionError::Module(e.to_string()))?;
    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| ExtensionError::Module(e.to_string()))?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| ExtensionError::Module("no memory export".to_owned()))?;
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .map_err(|e| ExtensionError::Module(e.to_string()))?;
    let func = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, function)
        .map_err(|e| ExtensionError::Module(e.to_string()))?;

    let input = args.to_string().into_bytes();
    let in_len =
        i32::try_from(input.len()).map_err(|_| ExtensionError::Run("args too large".into()))?;
    let in_ptr = alloc
        .call(&mut store, in_len)
        .map_err(|e| ExtensionError::Run(e.to_string()))?;
    let start = usize::try_from(in_ptr).map_err(|_| ExtensionError::Run("bad pointer".into()))?;
    memory
        .data_mut(&mut store)
        .get_mut(start..start + input.len())
        .ok_or_else(|| ExtensionError::Run("args out of bounds".to_owned()))?
        .copy_from_slice(&input);
    let packed = func
        .call(&mut store, (in_ptr, in_len))
        .map_err(|e| ExtensionError::Run(trap_text(&e)))?;
    let out_ptr =
        usize::try_from(packed >> 32).map_err(|_| ExtensionError::Run("bad result".into()))?;
    let out_len = usize::try_from(packed & 0xffff_ffff)
        .map_err(|_| ExtensionError::Run("bad result".into()))?;
    let data = memory.data(&store);
    let out = data
        .get(out_ptr..out_ptr + out_len)
        .ok_or_else(|| ExtensionError::Run("result out of bounds".to_owned()))?;
    if out.is_empty() {
        return Ok(Json::Null);
    }
    serde_json::from_slice(out).map_err(|e| ExtensionError::Run(format!("result is not JSON: {e}")))
}

fn trap_text(e: &wasmtime::Error) -> String {
    if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
        if *trap == wasmtime::Trap::OutOfFuel {
            return "limit exceeded: fuel".to_owned();
        }
    }
    e.to_string()
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
async fn call_native(
    path: PathBuf,
    function: &str,
    space: SpaceId,
    args: Json,
) -> Result<Json, ExtensionError> {
    let req = json!({ "function": function, "args": args, "space": space.to_string() })
        .to_string()
        .into_bytes();
    tokio::task::spawn_blocking(move || native_call(&path, &req))
        .await
        .map_err(|e| ExtensionError::Run(e.to_string()))?
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
fn native_call(path: &Path, req: &[u8]) -> Result<Json, ExtensionError> {
    type Call = unsafe extern "C" fn(*const u8, usize, *mut *mut u8, *mut usize) -> i32;
    type Free = unsafe extern "C" fn(*mut u8, usize);
    // Loading and calling native code is the user's accepted risk; the
    // grant for this exact file is checked before we get here. The ABI
    // contract is: the library writes a reply buffer it owns and frees it
    // on request; the request bytes are only read for the duration of the
    // call.
    #[allow(unsafe_code)]
    let out = unsafe {
        let lib =
            libloading::Library::new(path).map_err(|e| ExtensionError::Module(e.to_string()))?;
        let call: libloading::Symbol<Call> = lib
            .get(b"elri_ext_call\0")
            .map_err(|e| ExtensionError::Module(e.to_string()))?;
        let free: libloading::Symbol<Free> = lib
            .get(b"elri_ext_free\0")
            .map_err(|e| ExtensionError::Module(e.to_string()))?;
        let mut out_ptr: *mut u8 = std::ptr::null_mut();
        let mut out_len: usize = 0;
        let code = call(req.as_ptr(), req.len(), &raw mut out_ptr, &raw mut out_len);
        let bytes = if out_ptr.is_null() {
            Vec::new()
        } else {
            let v = std::slice::from_raw_parts(out_ptr, out_len).to_vec();
            free(out_ptr, out_len);
            v
        };
        if code != 0 {
            return Err(ExtensionError::Run(format!(
                "native extension returned {code}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        bytes
    };
    if out.is_empty() {
        return Ok(Json::Null);
    }
    serde_json::from_slice(&out)
        .map_err(|e| ExtensionError::Run(format!("result is not JSON: {e}")))
}

#[cfg(any(target_os = "android", target_os = "ios"))]
async fn call_native(
    _path: PathBuf,
    _function: &str,
    _space: SpaceId,
    _args: Json,
) -> Result<Json, ExtensionError> {
    Err(ExtensionError::NativeUnsupported)
}

/// Whether native extensions can run here.
pub fn native_supported() -> bool {
    cfg!(not(any(target_os = "android", target_os = "ios")))
}

/// Keeps the manifest type in the public signature set.
pub fn manifest_of(rec: &ExtensionRecord) -> Manifest {
    script::manifest_from_json(&rec.manifest).unwrap_or_default()
}
