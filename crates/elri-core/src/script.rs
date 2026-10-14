//! Luau scripts: source kept in a Space, run on this device as a client of
//! the core API with a manifest of what it may touch.
//!
//! A run gets a fresh sandboxed VM with bounded memory, a deadline, and a
//! host-call budget. The `elri` table is its whole world: its own Space,
//! its arguments, and a handful of data functions. Every read is checked
//! against the manifest and recorded as a label; every write is checked
//! against the manifest too. Nothing else is reachable: no I/O, no
//! `require`, no clock beyond what the host hands over.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use elri_proto::elri::v1::events::{event, NoticeEvent};
use elri_proto::elri::v1::object::{value, Object, ObjectKind, Value};
use elri_proto::elri::v1::script::{Manifest, RunResult, Script};
use mlua::{Lua, LuaSerdeExt as _, Table, Value as LuaValue, VmState};
use sha2::{Digest as _, Sha256};

use crate::model::{
    int_field, now_ms, reserved, string_field, text_value, ObjectDoc, ObjectId, SpaceId,
};
use crate::store::{ObjectFilter, Store, StoreError};

/// Reserved field: the Luau source.
pub const SOURCE: &str = "$source";
/// Reserved field: the manifest as JSON.
pub const MANIFEST: &str = "$manifest";

/// Memory a run may use.
const MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// Wall-clock a run may take.
const TIME_LIMIT: Duration = Duration::from_secs(5);
/// Host calls a run may make.
const HOST_CALL_LIMIT: u32 = 10_000;
/// Longest output kept, in bytes.
const OUTPUT_LIMIT: usize = 1 << 20;

/// Run failures that are not storage failures.
#[derive(Debug, thiserror::Error)]
pub enum ScriptError {
    /// Storage failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The source does not compile or the run raised.
    #[error("{0}")]
    Lua(String),
    /// The manifest is not valid JSON of the expected shape.
    #[error("manifest: {0}")]
    Manifest(String),
    /// The script asked for something outside its manifest.
    #[error("capability denied: {0}")]
    Denied(String),
    /// The run exceeded a limit.
    #[error("limit exceeded: {0}")]
    Limit(&'static str),
}

/// Hex SHA-256 over source and manifest: what a grant is bound to.
pub fn version_of(source: &str, manifest: &Manifest) -> String {
    let mut h = Sha256::new();
    h.update(source.as_bytes());
    h.update(b"\0");
    h.update(manifest_json(manifest).as_bytes());
    h.finalize()
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// The manifest's canonical JSON form.
pub fn manifest_json(m: &Manifest) -> String {
    let mut reads = m.reads.clone();
    reads.sort();
    reads.dedup();
    let mut writes = m.writes.clone();
    writes.sort();
    writes.dedup();
    let mut extensions = m.extensions.clone();
    extensions.sort();
    extensions.dedup();
    serde_json::json!({
        "reads": reads,
        "writes": writes,
        "http": m.http,
        "ai": m.ai,
        "extensions": extensions,
    })
    .to_string()
}

/// Parses a manifest from its JSON form.
pub fn manifest_from_json(s: &str) -> Result<Manifest, ScriptError> {
    if s.trim().is_empty() {
        return Ok(Manifest::default());
    }
    let v: serde_json::Value =
        serde_json::from_str(s).map_err(|e| ScriptError::Manifest(e.to_string()))?;
    let list = |key: &str| -> Result<Vec<String>, ScriptError> {
        match v.get(key) {
            None | Some(serde_json::Value::Null) => Ok(Vec::new()),
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|i| {
                    i.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| ScriptError::Manifest(format!("{key}: not a string")))
                })
                .collect(),
            Some(_) => Err(ScriptError::Manifest(format!("{key}: not a list"))),
        }
    };
    Ok(Manifest {
        reads: list("reads")?,
        writes: list("writes")?,
        http: v
            .get("http")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        ai: v
            .get("ai")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        extensions: list("extensions")?,
    })
}

/// Whether every capability stays inside the script's own Space and data:
/// such a script is granted without asking under the device policy.
pub fn is_benign(space: SpaceId, m: &Manifest) -> bool {
    let own = space.to_string();
    m.reads.iter().all(|s| *s == own)
        && m.writes.iter().all(|s| *s == own)
        && !m.http
        && !m.ai
        && m.extensions.is_empty()
}

/// The protocol view of a script Object.
pub fn script_of(o: &Object) -> Result<Script, ScriptError> {
    let source = match o.fields.get(SOURCE).and_then(|v| v.value.as_ref()) {
        Some(value::Value::Text(t)) => t.clone(),
        Some(value::Value::String(s)) => s.clone(),
        _ => String::new(),
    };
    let manifest = manifest_from_json(string_field(o, MANIFEST).unwrap_or_default())?;
    let space = ObjectId::parse(&o.space_id).map_err(StoreError::from)?;
    Ok(Script {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        version: version_of(&source, &manifest),
        benign: is_benign(space, &manifest),
        source,
        manifest: Some(manifest),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    })
}

/// Writes source and manifest onto a script document.
pub fn write_script(
    doc: &ObjectDoc,
    source: Option<&str>,
    manifest: Option<&Manifest>,
) -> Result<(), crate::model::ModelError> {
    if let Some(s) = source {
        doc.set_field(SOURCE, &text_value(s))?;
    }
    if let Some(m) = manifest {
        doc.set_field(MANIFEST, &crate::model::string_value(manifest_json(m)))?;
    }
    Ok(())
}

/// Compiles without running; the diagnostics are empty when it compiles.
pub fn check(source: &str) -> Vec<String> {
    let lua = Lua::new();
    match lua.load(source).set_name("script").into_function() {
        Ok(_) => Vec::new(),
        Err(e) => vec![e.to_string()],
    }
}

/// What a run may touch, and what it has touched.
struct Access {
    own: SpaceId,
    manifest: Manifest,
    /// Spaces read so far: the labels every write carries.
    labels: BTreeSet<String>,
    logs: Vec<String>,
    calls: u32,
    bytes_out: usize,
}

impl Access {
    fn may_read(&mut self, space: SpaceId) -> Result<(), ScriptError> {
        if space != self.own && !self.manifest.reads.contains(&space.to_string()) {
            return Err(ScriptError::Denied(format!("read space {space}")));
        }
        self.labels.insert(space.to_string());
        Ok(())
    }

    fn may_write(&self, space: SpaceId) -> Result<(), ScriptError> {
        if space != self.own && !self.manifest.writes.contains(&space.to_string()) {
            return Err(ScriptError::Denied(format!("write space {space}")));
        }
        Ok(())
    }

    fn charge(&mut self) -> Result<(), ScriptError> {
        self.calls += 1;
        if self.calls > HOST_CALL_LIMIT {
            return Err(ScriptError::Limit("host calls"));
        }
        Ok(())
    }
}

/// The host the `elri` table talks to.
#[derive(Clone)]
pub struct Host {
    store: Arc<Store>,
    device_id: String,
    modules: crate::extension::Modules,
}

/// One run's identity for attribution.
#[derive(Debug, Clone)]
pub struct RunIdentity {
    /// The script.
    pub script_id: String,
    /// The rule that fired it, if any.
    pub rule_id: Option<String>,
    /// The grant the run used.
    pub grant_id: String,
}

impl RunIdentity {
    /// The actor string written on every Change the run makes.
    pub fn actor(&self) -> String {
        match &self.rule_id {
            Some(rule) => format!("rule:{rule}/script:{}", self.script_id),
            None => format!("script:{}", self.script_id),
        }
    }
}

impl Host {
    /// A host over this device's store.
    pub fn new(store: Arc<Store>, device_id: String, modules: crate::extension::Modules) -> Self {
        Self {
            store,
            device_id,
            modules,
        }
    }

    /// Runs a script with JSON `args` as `elri.args`.
    pub async fn run(
        &self,
        space: SpaceId,
        script: &Script,
        args: serde_json::Value,
        identity: RunIdentity,
    ) -> RunResult {
        let started = Instant::now();
        let manifest = script.manifest.clone().unwrap_or_default();
        let access = Arc::new(Mutex::new(Access {
            own: space,
            manifest,
            labels: BTreeSet::new(),
            logs: Vec::new(),
            calls: 0,
            bytes_out: 0,
        }));
        let outcome = self
            .run_inner(space, script, args, &identity, Arc::clone(&access), started)
            .await;
        let guard = access
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (output_json, error) = match outcome {
            Ok(v) => (v, String::new()),
            Err(e) => (String::new(), e.to_string()),
        };
        RunResult {
            output_json,
            logs: guard.logs.clone(),
            duration_ms: i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
            error,
            labels: guard.labels.iter().cloned().collect(),
            grant_id: identity.grant_id,
        }
    }

    /// Evaluates Luau expressions in one read-only VM: `elri` offers reads
    /// of the interface's Space only, `state` is the given object. Each
    /// result is JSON or the expression's error.
    pub async fn evaluate(
        &self,
        space: SpaceId,
        exprs: &[(String, String, String)],
        state: &serde_json::Value,
    ) -> Vec<Result<serde_json::Value, String>> {
        let started = Instant::now();
        let access = Arc::new(Mutex::new(Access {
            own: space,
            manifest: Manifest::default(),
            labels: BTreeSet::new(),
            logs: Vec::new(),
            calls: 0,
            bytes_out: 0,
        }));
        let identity = RunIdentity {
            script_id: String::new(),
            rule_id: None,
            grant_id: String::new(),
        };
        let lua = match self.vm(space, &identity, &access, started, true) {
            Ok(lua) => lua,
            Err(e) => return exprs.iter().map(|_| Err(e.to_string())).collect(),
        };
        if let Err(e) = lua
            .to_value(state)
            .and_then(|v| lua.globals().set("state", v))
        {
            return exprs.iter().map(|_| Err(e.to_string())).collect();
        }
        let mut out = Vec::with_capacity(exprs.len());
        for (_, _, expr) in exprs {
            let r = async {
                let f = lua
                    .load(format!("return {expr}"))
                    .set_name("binding")
                    .into_function()
                    .map_err(|e| e.to_string())?;
                let v: LuaValue = f.call_async(()).await.map_err(|e| e.to_string())?;
                lua.from_value::<serde_json::Value>(v)
                    .map_err(|e| e.to_string())
            }
            .await;
            out.push(r);
        }
        out
    }

    /// Runs handler source with a mutable `state` table and an `event`
    /// payload; returns the state afterwards, the logs, and the error.
    pub async fn handle(
        &self,
        space: SpaceId,
        source: &str,
        state: serde_json::Value,
        payload: serde_json::Value,
        identity: RunIdentity,
    ) -> (serde_json::Value, Vec<String>, String) {
        let started = Instant::now();
        let access = Arc::new(Mutex::new(Access {
            own: space,
            manifest: Manifest::default(),
            labels: BTreeSet::new(),
            logs: Vec::new(),
            calls: 0,
            bytes_out: 0,
        }));
        let result: Result<serde_json::Value, ScriptError> = async {
            let lua = self.vm(space, &identity, &access, started, false)?;
            lua.globals()
                .set("state", lua.to_value(&state).map_err(|e| lua_err(&e))?)
                .map_err(|e| lua_err(&e))?;
            lua.globals()
                .set("event", lua.to_value(&payload).map_err(|e| lua_err(&e))?)
                .map_err(|e| lua_err(&e))?;
            let f = lua
                .load(source)
                .set_name("handler")
                .into_function()
                .map_err(|e| lua_err(&e))?;
            f.call_async::<LuaValue>(())
                .await
                .map_err(|e| lua_err(&e))?;
            let after: LuaValue = lua.globals().get("state").map_err(|e| lua_err(&e))?;
            lua.from_value(after).map_err(|e| lua_err(&e))
        }
        .await;
        let guard = access
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match result {
            Ok(new_state) => (new_state, guard.logs.clone(), String::new()),
            Err(e) => (state, guard.logs.clone(), e.to_string()),
        }
    }

    /// A fresh sandboxed VM with the `elri` table installed; `read_only`
    /// leaves out every writing function.
    fn vm(
        &self,
        space: SpaceId,
        identity: &RunIdentity,
        access: &Arc<Mutex<Access>>,
        started: Instant,
        read_only: bool,
    ) -> Result<Lua, ScriptError> {
        let lua = Lua::new();
        lua.sandbox(true).map_err(|e| lua_err(&e))?;
        lua.set_memory_limit(MEMORY_LIMIT)
            .map_err(|e| lua_err(&e))?;
        let deadline = started + TIME_LIMIT;
        lua.set_interrupt(move |_| {
            if Instant::now() > deadline {
                Err(mlua::Error::runtime("time limit exceeded"))
            } else {
                Ok(VmState::Continue)
            }
        });
        let elri = lua.create_table().map_err(|e| lua_err(&e))?;
        elri.set("space", space.to_string())
            .map_err(|e| lua_err(&e))?;
        elri.set("device", self.device_id.clone())
            .map_err(|e| lua_err(&e))?;
        elri.set("now", now_ms()).map_err(|e| lua_err(&e))?;
        self.install(&lua, &elri, space, identity, access)?;
        if read_only {
            for name in ["create", "update", "delete", "notify"] {
                elri.set(name, LuaValue::Nil).map_err(|e| lua_err(&e))?;
            }
        }
        lua.globals().set("elri", elri).map_err(|e| lua_err(&e))?;
        Ok(lua)
    }

    async fn run_inner(
        &self,
        space: SpaceId,
        script: &Script,
        args: serde_json::Value,
        identity: &RunIdentity,
        access: Arc<Mutex<Access>>,
        started: Instant,
    ) -> Result<String, ScriptError> {
        let lua = Lua::new();
        lua.sandbox(true).map_err(|e| lua_err(&e))?;
        lua.set_memory_limit(MEMORY_LIMIT)
            .map_err(|e| lua_err(&e))?;
        let deadline = started + TIME_LIMIT;
        lua.set_interrupt(move |_| {
            if Instant::now() > deadline {
                Err(mlua::Error::runtime("time limit exceeded"))
            } else {
                Ok(VmState::Continue)
            }
        });

        let elri = lua.create_table().map_err(|e| lua_err(&e))?;
        elri.set("space", space.to_string())
            .map_err(|e| lua_err(&e))?;
        elri.set("device", self.device_id.clone())
            .map_err(|e| lua_err(&e))?;
        elri.set("args", lua.to_value(&args).map_err(|e| lua_err(&e))?)
            .map_err(|e| lua_err(&e))?;
        elri.set("now", now_ms()).map_err(|e| lua_err(&e))?;
        self.install(&lua, &elri, space, identity, &access)?;
        lua.globals().set("elri", elri).map_err(|e| lua_err(&e))?;

        let func = lua
            .load(script.source.as_str())
            .set_name(script.name.as_str())
            .into_function()
            .map_err(|e| lua_err(&e))?;
        let result: LuaValue = func.call_async(()).await.map_err(|e| lua_err(&e))?;
        let json: serde_json::Value = lua.from_value(result).map_err(|e| lua_err(&e))?;
        let out = if json.is_null() {
            String::new()
        } else {
            json.to_string()
        };
        if out.len() > OUTPUT_LIMIT {
            return Err(ScriptError::Limit("output size"));
        }
        Ok(out)
    }

    #[allow(clippy::too_many_lines)]
    fn install(
        &self,
        lua: &Lua,
        elri: &Table,
        own: SpaceId,
        identity: &RunIdentity,
        access: &Arc<Mutex<Access>>,
    ) -> Result<(), ScriptError> {
        let actor = identity.actor();

        // elri.log(...)
        let a = Arc::clone(access);
        let log = lua
            .create_function(move |_, items: mlua::Variadic<LuaValue>| {
                let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                g.charge().map_err(to_lua)?;
                let line = items
                    .iter()
                    .map(|v| match v {
                        LuaValue::String(s) => s.to_string_lossy(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                g.bytes_out += line.len();
                if g.bytes_out > OUTPUT_LIMIT {
                    return Err(to_lua(ScriptError::Limit("log size")));
                }
                g.logs.push(line);
                Ok(())
            })
            .map_err(|e| lua_err(&e))?;
        elri.set("log", log).map_err(|e| lua_err(&e))?;

        // elri.notify(text)
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let source = actor.clone();
        let notify = lua
            .create_function(move |_, text: String| {
                a.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .charge()
                    .map_err(to_lua)?;
                store.emit(event::Event::Notice(NoticeEvent {
                    space_id: own.to_string(),
                    source: source.clone(),
                    text,
                }));
                Ok(())
            })
            .map_err(|e| lua_err(&e))?;
        elri.set("notify", notify).map_err(|e| lua_err(&e))?;

        // elri.get(id [, space])
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let get = lua
            .create_async_function(move |lua, (id, space): (String, Option<String>)| {
                let a = Arc::clone(&a);
                let store = Arc::clone(&store);
                async move {
                    let space = space_arg(space.as_deref(), own)?;
                    {
                        let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        g.charge().map_err(to_lua)?;
                        g.may_read(space).map_err(to_lua)?;
                    }
                    let id = id_arg(&id)?;
                    let o = store
                        .space(space)
                        .await
                        .map_err(to_lua)?
                        .get(id)
                        .await
                        .map_err(to_lua)?;
                    object_to_lua(&lua, &o)
                }
            })
            .map_err(|e| lua_err(&e))?;
        elri.set("get", get).map_err(|e| lua_err(&e))?;

        // elri.list({kind=, type=, limit=, include_deleted=} [, space])
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let list = lua
            .create_async_function(
                move |lua, (filter, space): (Option<Table>, Option<String>)| {
                    let a = Arc::clone(&a);
                    let store = Arc::clone(&store);
                    async move {
                        let space = space_arg(space.as_deref(), own)?;
                        {
                            let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            g.charge().map_err(to_lua)?;
                            g.may_read(space).map_err(to_lua)?;
                        }
                        let f = filter_from_lua(filter.as_ref())?;
                        let objects = store
                            .space(space)
                            .await
                            .map_err(to_lua)?
                            .list(&f)
                            .await
                            .map_err(to_lua)?;
                        let out = lua.create_table()?;
                        for (i, o) in objects.iter().enumerate() {
                            out.set(i + 1, object_to_lua(&lua, o)?)?;
                        }
                        Ok(out)
                    }
                },
            )
            .map_err(|e| lua_err(&e))?;
        elri.set("list", list).map_err(|e| lua_err(&e))?;

        // elri.search(query [, space])
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let search = lua
            .create_async_function(move |lua, (query, space): (String, Option<String>)| {
                let a = Arc::clone(&a);
                let store = Arc::clone(&store);
                async move {
                    let space = space_arg(space.as_deref(), own)?;
                    {
                        let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        g.charge().map_err(to_lua)?;
                        g.may_read(space).map_err(to_lua)?;
                    }
                    let hits = store
                        .space(space)
                        .await
                        .map_err(to_lua)?
                        .search(&query, None, 50)
                        .await
                        .map_err(to_lua)?;
                    let out = lua.create_table()?;
                    for (i, (o, score, fields)) in hits.iter().enumerate() {
                        let hit = lua.create_table()?;
                        hit.set("object", object_to_lua(&lua, o)?)?;
                        hit.set("score", *score)?;
                        hit.set("fields", fields.clone())?;
                        out.set(i + 1, hit)?;
                    }
                    Ok(out)
                }
            })
            .map_err(|e| lua_err(&e))?;
        elri.set("search", search).map_err(|e| lua_err(&e))?;

        // elri.create({kind=, types=, fields=, target=, participants=} [, space])
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let actor_c = actor.clone();
        let create = lua
            .create_async_function(move |lua, (spec, space): (Table, Option<String>)| {
                let a = Arc::clone(&a);
                let store = Arc::clone(&store);
                let actor = actor_c.clone();
                async move {
                    let space = space_arg(space.as_deref(), own)?;
                    {
                        let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        g.charge().map_err(to_lua)?;
                        g.may_write(space).map_err(to_lua)?;
                    }
                    let doc = doc_from_lua(&spec, space)?;
                    let rec = store
                        .space(space)
                        .await
                        .map_err(to_lua)?
                        .insert_as(&doc, &actor)
                        .await
                        .map_err(to_lua)?;
                    object_to_lua(&lua, &rec)
                }
            })
            .map_err(|e| lua_err(&e))?;
        elri.set("create", create).map_err(|e| lua_err(&e))?;

        // elri.update(id, {fields=, remove_fields=, add_types=, remove_types=} [, space])
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let actor_u = actor.clone();
        let update = lua
            .create_async_function(
                move |lua, (id, spec, space): (String, Table, Option<String>)| {
                    let a = Arc::clone(&a);
                    let store = Arc::clone(&store);
                    let actor = actor_u.clone();
                    async move {
                        let space = space_arg(space.as_deref(), own)?;
                        {
                            let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            g.charge().map_err(to_lua)?;
                            g.may_write(space).map_err(to_lua)?;
                        }
                        let id = id_arg(&id)?;
                        let edit = edit_from_lua(&spec)?;
                        let rec = store
                            .space(space)
                            .await
                            .map_err(to_lua)?
                            .update_as(id, edit, &actor)
                            .await
                            .map_err(to_lua)?;
                        object_to_lua(&lua, &rec)
                    }
                },
            )
            .map_err(|e| lua_err(&e))?;
        elri.set("update", update).map_err(|e| lua_err(&e))?;

        // elri.delete(id [, space])
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let actor_d = actor;
        let delete = lua
            .create_async_function(move |_, (id, space): (String, Option<String>)| {
                let a = Arc::clone(&a);
                let store = Arc::clone(&store);
                let actor = actor_d.clone();
                async move {
                    let space = space_arg(space.as_deref(), own)?;
                    {
                        let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        g.charge().map_err(to_lua)?;
                        g.may_write(space).map_err(to_lua)?;
                    }
                    let id = id_arg(&id)?;
                    store
                        .space(space)
                        .await
                        .map_err(to_lua)?
                        .update_as(id, |d| d.mark_deleted(now_ms()), &actor)
                        .await
                        .map_err(to_lua)?;
                    Ok(true)
                }
            })
            .map_err(|e| lua_err(&e))?;
        elri.set("delete", delete).map_err(|e| lua_err(&e))?;

        // elri.ext(name, func, args): an installed, granted extension the
        // script's manifest names.
        let a = Arc::clone(access);
        let store = Arc::clone(&self.store);
        let modules = self.modules.clone();
        let ext = lua
            .create_async_function(
                move |lua, (name, func, args): (String, String, Option<LuaValue>)| {
                    let a = Arc::clone(&a);
                    let store = Arc::clone(&store);
                    let modules = modules.clone();
                    async move {
                        {
                            let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            g.charge().map_err(to_lua)?;
                            if !g.manifest.extensions.contains(&name) {
                                return Err(to_lua(ScriptError::Denied(format!(
                                    "extension {name}"
                                ))));
                            }
                        }
                        let rec =
                            store
                                .extension(&name)
                                .await
                                .map_err(to_lua)?
                                .ok_or_else(|| {
                                    to_lua(ScriptError::Denied(format!(
                                        "extension {name} is not installed"
                                    )))
                                })?;
                        let grant = store
                            .grant_for(crate::extension::grant_identity(&name), &rec.version)
                            .await
                            .map_err(to_lua)?
                            .ok_or_else(|| {
                                to_lua(ScriptError::Denied(format!(
                                    "extension {name} has no grant on this device"
                                )))
                            })?;
                        let args: serde_json::Value = match args {
                            Some(v) => lua.from_value(v)?,
                            None => serde_json::Value::Null,
                        };
                        let result = crate::extension::call(
                            Arc::clone(&store),
                            &modules,
                            &rec,
                            &func,
                            own,
                            args,
                            grant.id,
                        )
                        .await;
                        {
                            let mut g = a.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            g.logs.extend(result.logs.iter().cloned());
                            g.labels.extend(result.labels.iter().cloned());
                        }
                        if !result.error.is_empty() {
                            return Err(mlua::Error::runtime(result.error));
                        }
                        let out: serde_json::Value = if result.output_json.is_empty() {
                            serde_json::Value::Null
                        } else {
                            serde_json::from_str(&result.output_json)
                                .unwrap_or(serde_json::Value::Null)
                        };
                        lua.to_value(&out)
                    }
                },
            )
            .map_err(|e| lua_err(&e))?;
        elri.set("ext", ext).map_err(|e| lua_err(&e))?;
        Ok(())
    }
}

fn lua_err(e: &mlua::Error) -> ScriptError {
    ScriptError::Lua(e.to_string())
}

fn to_lua(e: impl Into<ScriptError>) -> mlua::Error {
    mlua::Error::external(e.into())
}

fn id_arg(s: &str) -> mlua::Result<ObjectId> {
    ObjectId::parse(s).map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))
}

fn space_arg(space: Option<&str>, own: SpaceId) -> mlua::Result<SpaceId> {
    match space {
        None => Ok(own),
        Some(s) => ObjectId::parse(s).map_err(|e| to_lua(ScriptError::from(StoreError::from(e)))),
    }
}

fn value_to_lua(lua: &Lua, v: &Value) -> mlua::Result<LuaValue> {
    Ok(match &v.value {
        Some(value::Value::Bool(b)) => LuaValue::Boolean(*b),
        Some(value::Value::Int(i)) => LuaValue::Integer(*i),
        Some(value::Value::Double(d)) => LuaValue::Number(*d),
        Some(value::Value::String(s) | value::Value::Text(s)) => {
            LuaValue::String(lua.create_string(s)?)
        }
        Some(value::Value::TimeMs(t)) => {
            let t_table = lua.create_table()?;
            t_table.set("time", *t)?;
            LuaValue::Table(t_table)
        }
        Some(value::Value::Ref(r)) => {
            let r_table = lua.create_table()?;
            r_table.set("ref", r.clone())?;
            LuaValue::Table(r_table)
        }
        Some(value::Value::List(l)) => {
            let items = lua.create_table()?;
            for (i, it) in l.items.iter().enumerate() {
                items.set(i + 1, value_to_lua(lua, it)?)?;
            }
            LuaValue::Table(items)
        }
        Some(value::Value::Map(m)) => {
            let entries = lua.create_table()?;
            for (k, v) in &m.entries {
                entries.set(k.clone(), value_to_lua(lua, v)?)?;
            }
            LuaValue::Table(entries)
        }
        Some(value::Value::Rich(r)) => {
            let t = lua.create_table()?;
            t.set("text", r.text.clone())?;
            let marks = lua.create_table()?;
            for (i, m) in r.marks.iter().enumerate() {
                let mt = lua.create_table()?;
                mt.set("start", m.start)?;
                mt.set("end", m.end)?;
                mt.set("key", m.key.clone())?;
                mt.set("value", m.value_json.clone())?;
                marks.set(i + 1, mt)?;
            }
            t.set("marks", marks)?;
            LuaValue::Table(t)
        }
        None => LuaValue::Nil,
    })
}

fn value_from_lua(v: LuaValue) -> mlua::Result<Option<Value>> {
    let inner = match v {
        LuaValue::Nil => return Ok(None),
        LuaValue::Boolean(b) => value::Value::Bool(b),
        LuaValue::Integer(i) => value::Value::Int(i),
        LuaValue::Number(n) => value::Value::Double(n),
        LuaValue::String(s) => value::Value::String(s.to_str()?.to_owned()),
        LuaValue::Table(t) => {
            if let Some(r) = t.get::<Option<String>>("ref")? {
                value::Value::Ref(r)
            } else if let Some(ms) = t.get::<Option<i64>>("time")? {
                value::Value::TimeMs(ms)
            } else if let Some(text) = t.get::<Option<String>>("text")? {
                match t.get::<Option<Table>>("marks")? {
                    Some(marks) => {
                        let mut out = Vec::new();
                        for m in marks.sequence_values::<Table>() {
                            let m = m?;
                            out.push(elri_proto::elri::v1::object::Mark {
                                start: m.get("start")?,
                                end: m.get("end")?,
                                key: m.get("key")?,
                                value_json: m
                                    .get::<Option<String>>("value")?
                                    .unwrap_or_else(|| "true".to_owned()),
                            });
                        }
                        value::Value::Rich(elri_proto::elri::v1::object::RichText {
                            text,
                            marks: out,
                        })
                    }
                    None => value::Value::Text(text),
                }
            } else {
                // A table with any string key is a map; otherwise a list.
                let mut entries = HashMap::new();
                let mut keyed = false;
                for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                    let (k, v) = pair?;
                    if let LuaValue::String(k) = k {
                        keyed = true;
                        if let Some(v) = value_from_lua(v)? {
                            entries.insert(k.to_str()?.to_owned(), v);
                        }
                    }
                }
                if keyed {
                    value::Value::Map(elri_proto::elri::v1::object::ValueMap { entries })
                } else {
                    let mut items = Vec::new();
                    for item in t.sequence_values::<LuaValue>() {
                        if let Some(v) = value_from_lua(item?)? {
                            items.push(v);
                        }
                    }
                    value::Value::List(elri_proto::elri::v1::object::ValueList { items })
                }
            }
        }
        other => {
            return Err(mlua::Error::runtime(format!(
                "unsupported field value: {other:?}"
            )))
        }
    };
    Ok(Some(Value { value: Some(inner) }))
}

fn object_to_lua(lua: &Lua, o: &Object) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.set("id", o.id.clone())?;
    t.set("space", o.space_id.clone())?;
    t.set("kind", o.kind().as_str_name().to_lowercase())?;
    t.set("types", o.types.clone())?;
    let fields = lua.create_table()?;
    for (k, v) in &o.fields {
        fields.set(k.clone(), value_to_lua(lua, v)?)?;
    }
    t.set("fields", fields)?;
    t.set("created_at", o.created_at_ms)?;
    t.set("updated_at", o.updated_at_ms)?;
    if let Some(d) = o.deleted_at_ms {
        t.set("deleted_at", d)?;
    }
    if let Some(target) = &o.target_id {
        t.set("target", target.clone())?;
    }
    if !o.participants.is_empty() {
        let ps = lua.create_table()?;
        for p in &o.participants {
            let pt = lua.create_table()?;
            pt.set("object", p.object_id.clone())?;
            pt.set("role", p.role_id.clone())?;
            ps.set(p.slot.clone(), pt)?;
        }
        t.set("participants", ps)?;
    }
    if !o.placements.is_empty() {
        let pl = lua.create_table()?;
        for (i, p) in o.placements.iter().enumerate() {
            let pt = lua.create_table()?;
            pt.set("id", p.id.clone())?;
            pt.set("parent", p.parent_id.clone())?;
            pt.set("index", p.index)?;
            pt.set("block", p.block_id.clone())?;
            pl.set(i + 1, pt)?;
        }
        t.set("placements", pl)?;
    }
    Ok(t)
}

fn kind_from_lua(s: Option<&str>) -> mlua::Result<ObjectKind> {
    match s.map(str::to_ascii_lowercase).as_deref() {
        None | Some("entity") => Ok(ObjectKind::Entity),
        Some("relation") => Ok(ObjectKind::Relation),
        Some("layer") => Ok(ObjectKind::Layer),
        Some(other) => Err(mlua::Error::runtime(format!(
            "kind {other} cannot be created here"
        ))),
    }
}

fn filter_from_lua(t: Option<&Table>) -> mlua::Result<ObjectFilter> {
    let Some(t) = t else {
        return Ok(ObjectFilter {
            limit: 100,
            ..ObjectFilter::default()
        });
    };
    let kind = match t.get::<Option<String>>("kind")? {
        None => None,
        Some(k) => Some(match k.to_ascii_lowercase().as_str() {
            "entity" => ObjectKind::Entity,
            "relation" => ObjectKind::Relation,
            "layer" => ObjectKind::Layer,
            "attachment" => ObjectKind::Attachment,
            "script" => ObjectKind::Script,
            "rule" => ObjectKind::Rule,
            other => return Err(mlua::Error::runtime(format!("unknown kind {other}"))),
        }),
    };
    let parse = |key: &str| -> mlua::Result<Option<ObjectId>> {
        match t.get::<Option<String>>(key)? {
            None => Ok(None),
            Some(s) => ObjectId::parse(&s)
                .map(Some)
                .map_err(|e| to_lua(ScriptError::from(StoreError::from(e)))),
        }
    };
    Ok(ObjectFilter {
        kind,
        type_id: t.get("type")?,
        include_deleted: t.get::<Option<bool>>("include_deleted")?.unwrap_or(false),
        limit: usize::try_from(t.get::<Option<i64>>("limit")?.unwrap_or(100)).unwrap_or(100),
        participant: parse("participant")?,
        target: parse("target")?,
        placed_block: parse("placed_block")?,
    })
}

fn fields_from_lua(t: Option<Table>) -> mlua::Result<HashMap<String, Value>> {
    let mut out = HashMap::new();
    if let Some(t) = t {
        for pair in t.pairs::<String, LuaValue>() {
            let (k, v) = pair?;
            if let Some(v) = value_from_lua(v)? {
                out.insert(k, v);
            }
        }
    }
    Ok(out)
}

fn doc_from_lua(spec: &Table, space: SpaceId) -> mlua::Result<ObjectDoc> {
    let kind = kind_from_lua(spec.get::<Option<String>>("kind")?.as_deref())?;
    let now = now_ms();
    let doc = match (kind, spec.get::<Option<String>>("target")?) {
        (ObjectKind::Layer, Some(target)) => {
            let target = ObjectId::parse(&target)
                .map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))?;
            ObjectDoc::create_layer(ObjectId::new(), space, target, now)
        }
        (ObjectKind::Layer, None) => {
            return Err(mlua::Error::runtime("a layer needs a target"));
        }
        (kind, _) => ObjectDoc::create(ObjectId::new(), space, kind, now),
    }
    .map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))?;
    for t in spec
        .get::<Option<Vec<String>>>("types")?
        .unwrap_or_default()
    {
        doc.add_type(&t)
            .map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))?;
    }
    for (k, v) in fields_from_lua(spec.get("fields")?)? {
        doc.set_field(&k, &v)
            .map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))?;
    }
    if let Some(ps) = spec.get::<Option<Table>>("participants")? {
        for pair in ps.pairs::<String, Table>() {
            let (slot, pt) = pair?;
            doc.set_participant(&elri_proto::elri::v1::object::Participant {
                slot,
                object_id: pt.get("object")?,
                role_id: pt.get::<Option<String>>("role")?.unwrap_or_default(),
            })
            .map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))?;
        }
    }
    doc.touch(now)
        .map_err(|e| to_lua(ScriptError::from(StoreError::from(e))))?;
    Ok(doc)
}

fn edit_from_lua(
    spec: &Table,
) -> mlua::Result<impl FnOnce(&ObjectDoc) -> Result<(), crate::model::ModelError>> {
    let set = fields_from_lua(spec.get("fields")?)?;
    let remove: Vec<String> = spec
        .get::<Option<Vec<String>>>("remove_fields")?
        .unwrap_or_default();
    let add_types: Vec<String> = spec
        .get::<Option<Vec<String>>>("add_types")?
        .unwrap_or_default();
    let remove_types: Vec<String> = spec
        .get::<Option<Vec<String>>>("remove_types")?
        .unwrap_or_default();
    Ok(move |doc: &ObjectDoc| {
        for (k, v) in &set {
            doc.set_field(k, v)?;
        }
        for k in &remove {
            doc.remove_field(k)?;
        }
        for t in &add_types {
            doc.add_type(t)?;
        }
        for t in &remove_types {
            doc.remove_type(t)?;
        }
        Ok(())
    })
}

/// Keeps the helper reachable for rule triggers that read object kinds.
pub fn kind_matches(kinds: &[i32], kind: i32) -> bool {
    kinds.is_empty() || kinds.contains(&kind)
}

/// Unused-import guard for helpers reached only from other modules.
#[allow(dead_code)]
fn _reach(o: &Object) -> Option<i64> {
    int_field(o, reserved::SIZE)
}
