//! The agent's access to the core API: any unary method, by name, with a
//! JSON request. Requests go through the same loopback dispatcher as the
//! renderer and the CLI, so every service the core offers is within reach
//! and every write is checked by the same handlers; an in-process token
//! attributes the writes to the agent.
//!
//! A gate decides first: methods that hand out authority or reach beyond
//! the device's data are refused, every Space a request names must be
//! within the agent's reach, every Space an answer names must be too, and
//! data read from one Space is copied into another only as its transfer
//! policy allows.

use std::collections::{BTreeSet, HashMap};
use std::sync::LazyLock;

use bytes::{Buf, BufMut};
use prost::encoding::{decode_varint, encode_key, encode_varint, DecodeContext, WireType};
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MessageDescriptor, MethodDescriptor, ServiceDescriptor,
    Value,
};
use ws_proto::{Client, ClientConfig, MethodSpec, Request, StreamKind};

/// The compiled protocol definitions, comments included.
pub static POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(&include_bytes!(concat!(env!("OUT_DIR"), "/api.bin"))[..])
        .unwrap_or_else(|_| DescriptorPool::new())
});

/// Routes of every unary method, made static once for the dispatcher.
static ROUTES: LazyLock<HashMap<String, &'static str>> = LazyLock::new(|| {
    let mut out = HashMap::new();
    for svc in POOL.services() {
        for m in svc.methods() {
            if m.is_client_streaming() || m.is_server_streaming() {
                continue;
            }
            let full = format!("{}/{}", svc.full_name(), m.name());
            let route: &'static str = Box::leak(format!("/{full}").into_boxed_str());
            out.insert(full, route);
        }
    }
    out
});

/// Services the core serves on its loopback socket. The server's own
/// services are not here.
const SERVED: &[&str] = &[
    "elri.v1.core.CoreService",
    "elri.v1.space.SpaceService",
    "elri.v1.object.ObjectService",
    "elri.v1.change.ChangeService",
    "elri.v1.search.SearchService",
    "elri.v1.definition.DefinitionService",
    "elri.v1.attachment.AttachmentService",
    "elri.v1.script.ScriptService",
    "elri.v1.script.RuleService",
    "elri.v1.ui.InterfaceService",
    "elri.v1.transfer.TransferService",
    "elri.v1.extension.ExtensionService",
    "elri.v1.context.ContextService",
    "elri.v1.document.DocumentService",
    "elri.v1.workflow.WorkflowService",
    "elri.v1.ai.AgentService",
];

/// Methods an agent may call. Anything not listed is refused, so a
/// method added to the API stays out of reach until it is added here.
/// Left out on purpose: methods that hand out authority, change the
/// device, erase for good, run code under its own grant (scripts, rules,
/// extensions, workflows), stand for a person's review or approval, or
/// name Spaces the agent was not given.
const ALLOWED: &[&str] = &[
    "elri.v1.ai.AgentService/CreateAgent",
    "elri.v1.ai.AgentService/CreatePrompt",
    "elri.v1.ai.AgentService/CreateSkill",
    "elri.v1.ai.AgentService/GetConversation",
    "elri.v1.ai.AgentService/ListAgents",
    "elri.v1.ai.AgentService/ListConversations",
    "elri.v1.ai.AgentService/ListPrompts",
    "elri.v1.ai.AgentService/ListSkills",
    "elri.v1.ai.AgentService/UpdateAgent",
    "elri.v1.ai.AgentService/UpdatePrompt",
    "elri.v1.ai.AgentService/UpdateSkill",
    "elri.v1.attachment.AttachmentService/GetAttachment",
    "elri.v1.attachment.AttachmentService/ListAttachments",
    "elri.v1.attachment.AttachmentService/Pin",
    "elri.v1.change.ChangeService/ListChanges",
    "elri.v1.context.ContextService/CreateContext",
    "elri.v1.context.ContextService/HideInContext",
    "elri.v1.context.ContextService/ListContexts",
    "elri.v1.context.ContextService/ListMappings",
    "elri.v1.context.ContextService/ProposeMapping",
    "elri.v1.context.ContextService/ResolveContext",
    "elri.v1.context.ContextService/UpdateContext",
    "elri.v1.core.CoreService/GetInfo",
    "elri.v1.definition.DefinitionService/CreateConstraint",
    "elri.v1.definition.DefinitionService/CreateFieldDefinition",
    "elri.v1.definition.DefinitionService/CreateRoleDefinition",
    "elri.v1.definition.DefinitionService/CreateTypeDefinition",
    "elri.v1.definition.DefinitionService/GetSchema",
    "elri.v1.definition.DefinitionService/ListConstraints",
    "elri.v1.definition.DefinitionService/ListFieldDefinitions",
    "elri.v1.definition.DefinitionService/ListPublications",
    "elri.v1.definition.DefinitionService/ListRoleDefinitions",
    "elri.v1.definition.DefinitionService/ListTypeDefinitions",
    "elri.v1.definition.DefinitionService/ListViolations",
    "elri.v1.definition.DefinitionService/PinDefinition",
    "elri.v1.definition.DefinitionService/PublishDefinition",
    "elri.v1.definition.DefinitionService/UpdateConstraint",
    "elri.v1.definition.DefinitionService/UpdateFieldDefinition",
    "elri.v1.definition.DefinitionService/UpdateTypeDefinition",
    "elri.v1.document.DocumentService/CaptureFragment",
    "elri.v1.document.DocumentService/MergeBlocks",
    "elri.v1.document.DocumentService/ResolveFragment",
    "elri.v1.document.DocumentService/SplitBlock",
    "elri.v1.extension.ExtensionService/ListExtensions",
    "elri.v1.object.ObjectService/Commit",
    "elri.v1.object.ObjectService/CreateObject",
    "elri.v1.object.ObjectService/DeleteObject",
    "elri.v1.object.ObjectService/GetObject",
    "elri.v1.object.ObjectService/ListHypotheses",
    "elri.v1.object.ObjectService/ListObjects",
    "elri.v1.object.ObjectService/UpdateObject",
    "elri.v1.script.RuleService/ListRuleRuns",
    "elri.v1.script.RuleService/ListRules",
    "elri.v1.script.ScriptService/CheckScript",
    "elri.v1.script.ScriptService/CreateScript",
    "elri.v1.script.ScriptService/GetScript",
    "elri.v1.script.ScriptService/GetSdk",
    "elri.v1.script.ScriptService/ListScripts",
    "elri.v1.script.ScriptService/UpdateScript",
    "elri.v1.search.SearchService/GetSemanticStatus",
    "elri.v1.search.SearchService/Search",
    "elri.v1.space.SpaceService/GetTransferPolicy",
    "elri.v1.transfer.TransferService/ExportObjects",
    "elri.v1.transfer.TransferService/ImportObjects",
    "elri.v1.transfer.TransferService/MoveObjects",
    "elri.v1.ui.InterfaceService/CreateInterface",
    "elri.v1.ui.InterfaceService/Dispatch",
    "elri.v1.ui.InterfaceService/Evaluate",
    "elri.v1.ui.InterfaceService/GetInterface",
    "elri.v1.ui.InterfaceService/ListInterfaces",
    "elri.v1.ui.InterfaceService/PublishInterface",
    "elri.v1.ui.InterfaceService/SelectVersion",
    "elri.v1.ui.InterfaceService/UpdateInterface",
    "elri.v1.workflow.WorkflowService/GetWorkflowRun",
    "elri.v1.workflow.WorkflowService/ListWorkflowRuns",
];

/// Method name prefixes that only read.
const READ_PREFIXES: &[&str] = &[
    "Get", "List", "Search", "Resolve", "Check", "Evaluate", "Export", "Capture",
];

/// Field names that name a Space.
const SPACE_FIELDS: &[&str] = &[
    "space_id",
    "space_ids",
    "source_space_id",
    "dest_space_id",
    "authorized_space_ids",
];

/// Whether an agent may call `method` (`package.Service/Method`).
pub fn allowed(method: &str) -> bool {
    let Some((svc, _)) = method.split_once('/') else {
        return false;
    };
    SERVED.contains(&svc) && ALLOWED.contains(&method) && ROUTES.contains_key(method)
}

/// Whether `method` only reads.
pub fn reads_only(method: &str) -> bool {
    method
        .rsplit_once('/')
        .is_some_and(|(_, m)| READ_PREFIXES.iter().any(|p| m.starts_with(p)))
}

fn method_of(full: &str) -> Option<MethodDescriptor> {
    let (svc, name) = full.split_once('/')?;
    POOL.get_service_by_name(svc)?
        .methods()
        .find(|m| m.name() == name)
}

/// Every Space id a message names, at any depth.
pub fn spaces_in(msg: &DynamicMessage, out: &mut BTreeSet<String>) {
    for (field, value) in msg.fields() {
        let named = SPACE_FIELDS.contains(&field.name());
        walk(value, named, out);
    }
}

fn walk(value: &Value, named: bool, out: &mut BTreeSet<String>) {
    match value {
        Value::String(s) if named && !s.is_empty() => {
            out.insert(s.clone());
        }
        Value::Message(m) => spaces_in(m, out),
        Value::List(items) => {
            for v in items {
                walk(v, named, out);
            }
        }
        Value::Map(m) => {
            for v in m.values() {
                walk(v, false, out);
            }
        }
        _ => {}
    }
}

/// A message passed through without decoding: the bytes of a dynamic
/// request one way and of the answer the other.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Raw(pub Vec<u8>);

#[allow(deprecated)]
impl prost::Message for Raw {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        buf.put_slice(&self.0);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        encode_key(tag, wire_type, &mut self.0);
        match wire_type {
            WireType::Varint => {
                let v = decode_varint(buf)?;
                encode_varint(v, &mut self.0);
            }
            WireType::SixtyFourBit => self.copy(buf, 8)?,
            WireType::ThirtyTwoBit => self.copy(buf, 4)?,
            WireType::LengthDelimited => {
                let len = decode_varint(buf)?;
                encode_varint(len, &mut self.0);
                self.copy(buf, usize::try_from(len).unwrap_or(usize::MAX))?;
            }
            WireType::StartGroup | WireType::EndGroup => {
                return Err(prost::DecodeError::new("groups are not supported"))
            }
        }
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        self.0.len()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

#[allow(deprecated)]
impl Raw {
    fn copy(&mut self, buf: &mut impl Buf, n: usize) -> Result<(), prost::DecodeError> {
        if buf.remaining() < n {
            return Err(prost::DecodeError::new("buffer underflow"));
        }
        let mut chunk = vec![0u8; n];
        buf.copy_to_slice(&mut chunk);
        self.0.extend_from_slice(&chunk);
        Ok(())
    }
}

/// What the agent may touch in one turn.
#[derive(Debug, Clone)]
pub struct Reach {
    /// Spaces it may read.
    pub read: BTreeSet<String>,
    /// Spaces it may write.
    pub write: BTreeSet<String>,
    /// Extensions it may call.
    pub extensions: BTreeSet<String>,
    /// Who the writes are attributed to: `agent:<id>`.
    pub actor: String,
}

/// Decides whether data from the labelled Spaces may be written to a
/// Space; an error says why not.
pub type CopyCheck = dyn Fn(&BTreeSet<String>, &str) -> futures_util::future::BoxFuture<'static, Result<(), String>>
    + Sync;

/// The loopback connection the agent's calls travel on.
pub struct Caller {
    url: String,
    token: String,
    client: tokio::sync::Mutex<Option<Client>>,
}

/// Longest answer handed back to the model.
pub const ANSWER_LIMIT: usize = 24 * 1024;

impl Caller {
    /// A caller for the core at `url`, authenticating with the in-process
    /// agent token.
    pub fn new(url: String, token: String) -> Self {
        Self {
            url,
            token,
            client: tokio::sync::Mutex::new(None),
        }
    }

    async fn client(&self) -> Result<Client, String> {
        let mut g = self.client.lock().await;
        if let Some(c) = g.as_ref().filter(|c| c.is_connected()) {
            return Ok(c.clone());
        }
        let c = Client::dial(&self.url, ClientConfig::default())
            .await
            .map_err(|e| e.to_string())?;
        *g = Some(c.clone());
        Ok(c)
    }

    /// Calls `method` with `request` as the agent, after the gate. Returns
    /// the answer as JSON, or why it was refused. `labels` collects the
    /// Spaces read so far in the turn; `copy_check` decides whether data
    /// from them may be written to a Space.
    pub async fn call(
        &self,
        reach: &Reach,
        method: &str,
        request: &serde_json::Value,
        labels: &mut BTreeSet<String>,
        copy_check: &CopyCheck,
    ) -> Result<String, String> {
        let method = method.trim().trim_start_matches('/');
        if !allowed(method) {
            return Err(format!(
                "{method} is not available to agents; use describe to see what is"
            ));
        }
        let desc = method_of(method).ok_or_else(|| format!("no method {method}"))?;
        let input = desc.input();
        let msg = DynamicMessage::deserialize(input.clone(), request)
            .map_err(|e| format!("request does not fit {}: {e}", input.full_name()))?;
        let mut named = BTreeSet::new();
        spaces_in(&msg, &mut named);
        let writes = !reads_only(method);
        // A write must say where it lands; one naming no Space is refused
        // rather than trusted.
        if writes && named.is_empty() {
            return Err(format!(
                "{method} names no space; a write must name the space it changes"
            ));
        }
        for s in &named {
            let ok = if writes {
                reach.write.contains(s)
            } else {
                reach.read.contains(s)
            };
            if !ok {
                return Err(format!(
                    "space {s} is outside this agent's reach for {}",
                    if writes { "writing" } else { "reading" }
                ));
            }
        }
        if writes {
            for dest in &named {
                copy_check(labels, dest).await?;
            }
        } else {
            labels.extend(named.iter().cloned());
        }

        let route = ROUTES
            .get(method)
            .copied()
            .ok_or_else(|| format!("no method {method}"))?;
        let client = self.client().await?;
        let req = Request::new(Raw(prost::Message::encode_to_vec(&msg)))
            .with_header("authorization", format!("Bearer {}", self.token))
            .with_header(crate::api::ACTOR_HEADER, reach.actor.clone());
        let resp = client
            .invoke_unary::<Raw, Raw>(
                MethodSpec {
                    route,
                    kind: StreamKind::Unary,
                },
                req,
            )
            .await
            .map_err(|e| format!("{:?}: {}", e.code, e.message))?;
        let out = DynamicMessage::decode(desc.output(), resp.msg.0.as_slice())
            .map_err(|e| format!("answer: {e}"))?;
        let mut seen = BTreeSet::new();
        spaces_in(&out, &mut seen);
        if let Some(s) = seen.iter().find(|s| !reach.read.contains(*s)) {
            return Err(format!(
                "the answer names space {s}, outside this agent's reach; it is withheld"
            ));
        }
        labels.extend(seen);
        let mut text = serde_json::to_string(&out).map_err(|e| e.to_string())?;
        if text.len() > ANSWER_LIMIT {
            let mut cut = ANSWER_LIMIT;
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            text.truncate(cut);
            text.push_str("…(truncated; ask for less, e.g. a smaller limit)");
        }
        Ok(text)
    }
}

fn comment(file: &prost_reflect::FileDescriptor, path: &[i32]) -> String {
    file.file_descriptor_proto()
        .source_code_info
        .as_ref()
        .and_then(|info| info.location.iter().find(|l| l.path == path))
        .and_then(|l| {
            l.leading_comments
                .clone()
                .or_else(|| l.trailing_comments.clone())
        })
        .map(|c| {
            c.lines()
                .map(|l| l.trim().trim_start_matches('*').trim())
                .filter(|l| !l.is_empty())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
}

fn kind_name(k: &Kind) -> String {
    match k {
        Kind::Message(m) => m.full_name().to_owned(),
        Kind::Enum(e) => {
            let values: Vec<String> = e.values().map(|v| v.name().to_owned()).collect();
            format!("enum {} ({})", e.name(), values.join("|"))
        }
        other => format!("{other:?}").to_lowercase(),
    }
}

fn describe_message(
    m: &MessageDescriptor,
    depth: usize,
    seen: &mut BTreeSet<String>,
    out: &mut String,
) {
    use std::fmt::Write as _;
    if !seen.insert(m.full_name().to_owned()) {
        return;
    }
    let pad = "  ".repeat(depth);
    for f in m.fields() {
        let kind = f.kind();
        let mut ty = kind_name(&kind);
        if f.is_map() {
            ty = format!("map {ty}");
        } else if f.is_list() {
            ty = format!("repeated {ty}");
        }
        let c = comment(&m.parent_file(), f.path());
        let _ = writeln!(
            out,
            "{pad}- {}: {ty}{}",
            f.name(),
            if c.is_empty() {
                String::new()
            } else {
                format!(" — {c}")
            }
        );
        if let Kind::Message(inner) = kind {
            if depth < 3 && !f.is_map() {
                describe_message(&inner, depth + 1, seen, out);
            }
        }
    }
}

fn describe_service(svc: &ServiceDescriptor, out: &mut String) {
    use std::fmt::Write as _;
    let c = comment(&svc.parent_file(), svc.path());
    let _ = writeln!(
        out,
        "{}{}",
        svc.full_name(),
        if c.is_empty() {
            String::new()
        } else {
            format!(": {c}")
        }
    );
    for m in svc.methods() {
        let full = format!("{}/{}", svc.full_name(), m.name());
        if !allowed(&full) {
            continue;
        }
        let c = comment(&m.parent_file(), m.path());
        let _ = writeln!(
            out,
            "  {full} ({}){}",
            if reads_only(&full) { "read" } else { "write" },
            if c.is_empty() {
                String::new()
            } else {
                format!(" — {c}")
            }
        );
    }
}

/// What the agent can call: every service with no target, a service's
/// methods, or a method's request and answer fields.
pub fn describe(target: &str) -> String {
    use std::fmt::Write as _;
    let target = target.trim().trim_start_matches('/');
    let mut out = String::new();
    if target.is_empty() {
        for svc in SERVED {
            if let Some(s) = POOL.get_service_by_name(svc) {
                let c = comment(&s.parent_file(), s.path());
                let first = c.split(". ").next().unwrap_or("").to_owned();
                let _ = writeln!(out, "{svc} — {first}");
            }
        }
        out.push_str("\nDescribe a service for its methods, a method for its fields.");
        return out;
    }
    if let Some(svc) = POOL
        .get_service_by_name(target)
        .filter(|s| SERVED.contains(&s.full_name()))
    {
        describe_service(&svc, &mut out);
        return out;
    }
    if let Some(m) = method_of(target).filter(|_| allowed(target)) {
        let c = comment(&m.parent_file(), m.path());
        let _ = writeln!(
            out,
            "{target}{}",
            if c.is_empty() {
                String::new()
            } else {
                format!(" — {c}")
            }
        );
        let _ = writeln!(out, "request {}:", m.input().full_name());
        describe_message(&m.input(), 1, &mut BTreeSet::new(), &mut out);
        let _ = writeln!(out, "answer {}:", m.output().full_name());
        describe_message(&m.output(), 1, &mut BTreeSet::new(), &mut out);
        return out;
    }
    if let Some(msg) = POOL.get_message_by_name(target) {
        let _ = writeln!(out, "{target}:");
        describe_message(&msg, 1, &mut BTreeSet::new(), &mut out);
        return out;
    }
    format!("nothing named {target}; describe with an empty target lists the services")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    /// Methods an agent must never reach.
    const NEVER: &[&str] = &[
        "elri.v1.space.SpaceService/ListSpaces",
        "elri.v1.space.SpaceService/CreateSpace",
        "elri.v1.space.SpaceService/SetRetention",
        "elri.v1.space.SpaceService/SetTransferPolicy",
        "elri.v1.object.ObjectService/PurgeObject",
        "elri.v1.change.ChangeService/RetryChange",
        "elri.v1.change.ChangeService/DiscardChange",
        "elri.v1.search.SearchService/PrepareSemantic",
        "elri.v1.script.ScriptService/RunScript",
        "elri.v1.extension.ExtensionService/RemoveExtension",
        "elri.v1.extension.ExtensionService/GrantExtension",
        "elri.v1.extension.ExtensionService/CallExtension",
        "elri.v1.script.RuleService/CreateRule",
        "elri.v1.script.RuleService/UpdateRule",
        "elri.v1.workflow.WorkflowService/StartWorkflow",
        "elri.v1.workflow.WorkflowService/ResumeWorkflowRun",
        "elri.v1.workflow.WorkflowService/ResolveWorkflowStep",
        "elri.v1.workflow.WorkflowService/CancelWorkflowRun",
        "elri.v1.ai.AgentService/GrantAgent",
        "elri.v1.ai.AgentService/Send",
        "elri.v1.ai.AgentService/StartConversation",
        "elri.v1.context.ContextService/ApproveMapping",
        "elri.v1.object.ObjectService/ReviewHypothesis",
        "elri.v1.script.RuleService/RunRule",
    ];

    #[test]
    fn the_gate_keeps_authority_out_of_reach() {
        for m in NEVER {
            assert!(!allowed(m), "{m} is open to agents");
        }
        for m in ALLOWED {
            assert!(ROUTES.contains_key(*m), "{m} is not a method of the API");
        }
    }

    #[test]
    fn the_gate_knows_the_api() {
        assert!(allowed("elri.v1.ui.InterfaceService/UpdateInterface"));
        assert!(allowed("elri.v1.script.ScriptService/CreateScript"));
        assert!(!allowed("elri.v1.script.ScriptService/RunScript"));
        assert!(!allowed("elri.v1.script.GrantService/GrantScript"));
        assert!(!allowed("elri.v1.settings.SettingsService/UpdateSettings"));
        assert!(!allowed("elri.v1.ai.AiService/ListAccounts"));
        assert!(!allowed("elri.v1.events.EventService/Watch"));
        assert!(!allowed("elri.v1.extension.ExtensionService/CallExtension"));
        assert!(!allowed("elri.v1.script.RuleService/CreateRule"));
        assert!(!allowed("nonsense"));
        assert!(reads_only("elri.v1.object.ObjectService/ListObjects"));
        assert!(!reads_only("elri.v1.object.ObjectService/CreateObject"));
        let all = describe("");
        assert!(all.contains("elri.v1.ui.InterfaceService"), "{all}");
        let m = describe("elri.v1.ui.InterfaceService/UpdateInterface");
        assert!(m.contains("set_nodes"), "{m}");
        assert!(m.contains("— "), "comments come along: {m}");
    }

    #[test]
    fn raw_messages_pass_bytes_through_and_spaces_are_found() {
        use prost::Message as _;
        let req = elri_proto::elri::v1::object::ListObjectsRequest {
            space_id: "s1".into(),
            limit: 5,
            ..Default::default()
        };
        let bytes = req.encode_to_vec();
        let raw = Raw::decode(bytes.as_slice()).expect("decode");
        assert_eq!(raw.0, bytes);
        let desc = POOL
            .get_message_by_name("elri.v1.object.ListObjectsRequest")
            .expect("desc");
        let dynm = DynamicMessage::decode(desc, bytes.as_slice()).expect("dyn");
        let mut s = BTreeSet::new();
        spaces_in(&dynm, &mut s);
        assert_eq!(s.into_iter().collect::<Vec<_>>(), vec!["s1".to_owned()]);
    }

    fn names_space(m: &MessageDescriptor, depth: usize) -> bool {
        depth < 8
            && m.fields().any(|f| {
                SPACE_FIELDS.contains(&f.name())
                    || matches!(f.kind(), Kind::Message(ref inner) if names_space(inner, depth + 1))
            })
    }

    /// Every write an agent may call can name the Space it touches, so
    /// the gate's refusal of writes naming none never blocks a real one.
    #[test]
    fn every_agent_write_names_a_space() {
        for svc in POOL.services() {
            for m in svc.methods() {
                let full = format!("{}/{}", svc.full_name(), m.name());
                if allowed(&full) && !reads_only(&full) {
                    assert!(names_space(&m.input(), 0), "{full} names no space");
                }
            }
        }
    }
}
