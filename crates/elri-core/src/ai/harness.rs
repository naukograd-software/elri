//! Agents, skills, prompts and conversations as Objects of their Space,
//! and the turn loop: the model answers and calls the core API through
//! the gate, until it stops calling or the step limit is reached. Every
//! item the turn produces is saved to the conversation as it arrives, so
//! an interrupted turn keeps what was done.

use std::collections::BTreeSet;
use std::sync::Arc;

use elri_proto::elri::v1::ai::{
    turn_event, Agent, Conversation, Message, MessageRole, Prompt, Skill, ToolCall, ToolResult,
    TurnError, TurnEvent,
};
use elri_proto::elri::v1::object::{value, Object, ObjectKind};
use elri_proto::elri::v1::script::{Manifest, Script};
use elri_proto::elri::v1::space::TransferOp;
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;

use super::call::{Reach, ANSWER_LIMIT};
use super::{Ai, AiError};
use crate::model::{
    list_value, reserved, string_field, string_value, text_value, ModelError, ObjectDoc, ObjectId,
    SpaceId,
};
use crate::script;

/// Reserved fields of the AI Objects.
pub mod field {
    /// Agents: model slug.
    pub const MODEL: &str = "$model";
    /// Agents: reasoning effort.
    pub const EFFORT: &str = "$effort";
    /// Agents: own instructions.
    pub const INSTRUCTIONS: &str = "$instructions";
    /// Agents: prompt ids as JSON.
    pub const PROMPTS: &str = "$prompts";
    /// Agents: skill references as JSON.
    pub const SKILLS: &str = "$skills";
    /// Agents: manifest as JSON.
    pub const MANIFEST: &str = "$manifest";
    /// Skills: when to use it.
    pub const DESCRIPTION: &str = "$description";
    /// Skills and prompts: the text.
    pub const BODY: &str = "$body";
    /// Conversations: the agent.
    pub const AGENT: &str = "$agent";
    /// Conversations: Responses API items, one JSON string each.
    pub const ITEMS: &str = "$items";
}

/// Tool calls one turn may make.
pub const MAX_STEPS: usize = 32;
/// Destination name of the provider in transfer policies.
pub const PROVIDER_DESTINATION: &str = "ai:openai";

fn text_of(o: &Object, id: &str) -> String {
    match o.fields.get(id).and_then(|v| v.value.as_ref()) {
        Some(value::Value::String(s) | value::Value::Text(s)) => s.clone(),
        Some(value::Value::Rich(r)) => r.text.clone(),
        _ => String::new(),
    }
}

fn json_list(o: &Object, id: &str) -> Vec<String> {
    serde_json::from_str(string_field(o, id).unwrap_or("[]")).unwrap_or_default()
}

/// Hex SHA-256 over what an agent is allowed to do and told: a device
/// grant is bound to it.
pub fn agent_version(a: &Agent) -> String {
    let mut h = Sha256::new();
    for part in [
        a.instructions.as_str(),
        &serde_json::to_string(&a.prompt_ids).unwrap_or_default(),
        &serde_json::to_string(&a.skills).unwrap_or_default(),
        &script::manifest_json(&a.manifest.clone().unwrap_or_default()),
    ] {
        h.update(part.as_bytes());
        h.update(b"\0");
    }
    h.finalize()
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// The protocol view of an agent Object.
pub fn agent_of(o: &Object) -> Agent {
    let manifest = script::manifest_from_json(string_field(o, field::MANIFEST).unwrap_or("{}"))
        .unwrap_or_default();
    let mut a = Agent {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        model: string_field(o, field::MODEL).unwrap_or_default().to_owned(),
        effort: string_field(o, field::EFFORT)
            .unwrap_or_default()
            .to_owned(),
        instructions: text_of(o, field::INSTRUCTIONS),
        prompt_ids: json_list(o, field::PROMPTS),
        skills: json_list(o, field::SKILLS),
        // Never benign: every turn sends the Space to the provider, and an
        // agent may rewrite its own instructions, so each version waits
        // for the user's grant.
        benign: false,
        manifest: Some(manifest),
        version: String::new(),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    };
    a.version = agent_version(&a);
    a
}

/// Writes an agent's editable fields onto its document.
pub fn write_agent(doc: &ObjectDoc, a: &Agent) -> Result<(), ModelError> {
    doc.set_field(reserved::NAME, &string_value(a.name.trim()))?;
    doc.set_field(field::MODEL, &string_value(a.model.trim()))?;
    doc.set_field(field::EFFORT, &string_value(a.effort.trim()))?;
    doc.set_field(field::INSTRUCTIONS, &text_value(&a.instructions))?;
    doc.set_field(
        field::PROMPTS,
        &string_value(serde_json::to_string(&a.prompt_ids).unwrap_or_default()),
    )?;
    doc.set_field(
        field::SKILLS,
        &string_value(serde_json::to_string(&a.skills).unwrap_or_default()),
    )?;
    doc.set_field(
        field::MANIFEST,
        &string_value(script::manifest_json(
            &a.manifest.clone().unwrap_or_default(),
        )),
    )?;
    Ok(())
}

/// Checks an agent before it is written.
pub fn check_agent(a: &Agent) -> Result<(), String> {
    if a.name.trim().is_empty() {
        return Err("an agent needs a name".to_owned());
    }
    if !matches!(
        a.effort.trim(),
        "" | "none" | "minimal" | "low" | "medium" | "high" | "xhigh"
    ) {
        return Err("effort: none, minimal, low, medium, high or xhigh".to_owned());
    }
    for p in &a.prompt_ids {
        ObjectId::parse(p).map_err(|_| format!("prompt {p} is not an id"))?;
    }
    for s in &a.skills {
        if !s.starts_with(super::skills::BUILTIN_PREFIX) {
            ObjectId::parse(s).map_err(|_| format!("skill {s} is not an id or builtin:<name>"))?;
        }
    }
    if let Some(m) = &a.manifest {
        for s in m.reads.iter().chain(&m.writes) {
            ObjectId::parse(s).map_err(|_| format!("manifest space {s} is not an id"))?;
        }
    }
    Ok(())
}

/// The script-shaped identity an agent's grant is recorded under.
pub fn as_script(a: &Agent) -> Script {
    Script {
        id: a.id.clone(),
        space_id: a.space_id.clone(),
        name: a.name.clone(),
        source: String::new(),
        manifest: a.manifest.clone(),
        version: a.version.clone(),
        benign: a.benign,
        created_at_ms: a.created_at_ms,
        updated_at_ms: a.updated_at_ms,
        deleted_at_ms: a.deleted_at_ms,
    }
}

/// The protocol view of a skill Object.
pub fn skill_of(o: &Object) -> Skill {
    Skill {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        description: string_field(o, field::DESCRIPTION)
            .unwrap_or_default()
            .to_owned(),
        body: text_of(o, field::BODY),
        builtin: false,
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

/// The protocol view of a prompt Object.
pub fn prompt_of(o: &Object) -> Prompt {
    Prompt {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        body: text_of(o, field::BODY),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

/// The items of a conversation, as the provider takes them.
pub fn items_of(o: &Object) -> Vec<serde_json::Value> {
    match o.fields.get(field::ITEMS).and_then(|v| v.value.as_ref()) {
        Some(value::Value::List(l)) => l
            .items
            .iter()
            .filter_map(|v| match &v.value {
                Some(value::Value::String(s)) => serde_json::from_str(s).ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn item_text(item: &serde_json::Value) -> String {
    item.get("content")
        .and_then(|c| c.as_array())
        .map(|parts| {
            parts
                .iter()
                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn tool_name(item: &serde_json::Value) -> String {
    let name = item.get("name").and_then(|n| n.as_str()).unwrap_or("");
    name.rsplit('.').next().unwrap_or(name).to_owned()
}

/// Readable messages of a conversation's items.
pub fn messages_of(items: &[serde_json::Value]) -> Vec<Message> {
    let mut out: Vec<Message> = Vec::new();
    for item in items {
        let kind = item
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("message");
        let at = item
            .get("at_ms")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        match kind {
            "message" => {
                let role = match item.get("role").and_then(|r| r.as_str()) {
                    Some("user") => MessageRole::User,
                    _ => MessageRole::Assistant,
                };
                out.push(Message {
                    role: role.into(),
                    text: item_text(item),
                    model: item
                        .get("elri_model")
                        .and_then(|m| m.as_str())
                        .unwrap_or("")
                        .to_owned(),
                    at_ms: at,
                    ..Message::default()
                });
            }
            "function_call" => out.push(Message {
                role: MessageRole::Tool.into(),
                tool: tool_name(item),
                arguments_json: item
                    .get("arguments")
                    .and_then(|a| a.as_str())
                    .unwrap_or("")
                    .to_owned(),
                at_ms: at,
                ..Message::default()
            }),
            "function_call_output" => {
                let output = item.get("output").and_then(|o| o.as_str()).unwrap_or("");
                let failed =
                    item.get("elri_failed").and_then(serde_json::Value::as_bool) == Some(true);
                if let Some(m) = out
                    .iter_mut()
                    .rev()
                    .find(|m| m.role() == MessageRole::Tool && m.result.is_empty())
                {
                    output.clone_into(&mut m.result);
                    m.failed = failed;
                }
            }
            _ => {}
        }
    }
    out
}

/// The protocol view of a conversation Object, with its messages.
pub fn conversation_of(o: &Object, with_messages: bool) -> Conversation {
    Conversation {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        agent_id: string_field(o, field::AGENT).unwrap_or_default().to_owned(),
        title: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        messages: if with_messages {
            messages_of(&items_of(o))
        } else {
            Vec::new()
        },
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

/// Fields the core keeps on items for display; removed before they go to
/// the provider, as are the ids of items the provider did not keep.
const LOCAL_KEYS: &[&str] = &["at_ms", "elri_model", "elri_failed", "id", "status"];

fn for_provider(items: &[serde_json::Value]) -> Vec<serde_json::Value> {
    items
        .iter()
        .map(|i| {
            let mut i = i.clone();
            if let Some(o) = i.as_object_mut() {
                for k in LOCAL_KEYS {
                    o.remove(*k);
                }
            }
            i
        })
        .collect()
}

fn tools() -> serde_json::Value {
    serde_json::json!([{
        "type": "namespace",
        "name": "elri",
        "description": "The elri API of this device, and the agent's skills.",
        "tools": [
            {
                "type": "function",
                "name": "describe",
                "description": "Explains the API: empty target lists services; a service lists its methods; a method lists request and answer fields with their meaning.",
                "parameters": {
                    "type": "object",
                    "properties": {"target": {"type": "string", "description": "Empty, a service, a method (package.Service/Method) or a message name."}},
                    "required": ["target"],
                    "additionalProperties": false
                },
                "strict": true
            },
            {
                "type": "function",
                "name": "call",
                "description": "Calls an API method as this agent and returns the answer as JSON.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "method": {"type": "string", "description": "Full method name, e.g. elri.v1.object.ObjectService/ListObjects."},
                        "request": {"type": "string", "description": "The request as a JSON object, encoded as a string."}
                    },
                    "required": ["method", "request"],
                    "additionalProperties": false
                },
                "strict": true
            },
            {
                "type": "function",
                "name": "load_skill",
                "description": "Loads the full instructions of one of the agent's skills.",
                "parameters": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                    "additionalProperties": false
                },
                "strict": true
            }
        ]
    }])
}

struct Prepared {
    account: String,
    model: String,
    effort: String,
    instructions: String,
    skills: Vec<Skill>,
    reach: Reach,
}

/// Why a turn could not start or stopped: a code and a message.
type Refusal = (String, String);

fn refusal(code: &str, m: impl Into<String>) -> Refusal {
    (code.to_owned(), m.into())
}

fn store_refusal(e: impl std::fmt::Display) -> Refusal {
    refusal("store", e.to_string())
}

impl Ai {
    /// The agent with its version covering what it is told as well as what
    /// it may do: the texts of its prompts and skills are hashed in, so
    /// editing one of them needs a new grant like editing the agent.
    pub async fn with_effective_version(
        &self,
        space: SpaceId,
        mut a: Agent,
    ) -> Result<Agent, AiError> {
        let store = self.store.space(space).await?;
        let mut h = Sha256::new();
        h.update(a.version.as_bytes());
        for p in &a.prompt_ids {
            h.update(b"\0p");
            if let Ok(id) = ObjectId::parse(p) {
                if let Ok(o) = store.get(id).await {
                    if o.kind() == ObjectKind::Prompt && o.deleted_at_ms.is_none() {
                        h.update(prompt_of(&o).body.as_bytes());
                    }
                }
            }
        }
        for s in self.skills_for(space, &a).await? {
            h.update(b"\0s");
            h.update(s.id.as_bytes());
            h.update(b"\0");
            h.update(s.description.as_bytes());
            h.update(b"\0");
            h.update(s.body.as_bytes());
        }
        a.version = h
            .finalize()
            .iter()
            .fold(String::with_capacity(64), |mut out, b| {
                use std::fmt::Write as _;
                let _ = write!(out, "{b:02x}");
                out
            });
        Ok(a)
    }

    /// Skills an agent may load: the ones it names, or every built-in one
    /// when it names none.
    async fn skills_for(&self, space: SpaceId, a: &Agent) -> Result<Vec<Skill>, AiError> {
        let builtin = super::skills::builtin();
        if a.skills.is_empty() {
            return Ok(builtin);
        }
        let store = self.store.space(space).await?;
        let mut out = Vec::new();
        for s in &a.skills {
            if let Some(b) = builtin.iter().find(|b| &b.id == s) {
                out.push(b.clone());
            } else if let Ok(id) = ObjectId::parse(s) {
                if let Ok(o) = store.get(id).await {
                    if o.kind() == ObjectKind::Skill && o.deleted_at_ms.is_none() {
                        out.push(skill_of(&o));
                    }
                }
            }
        }
        Ok(out)
    }

    /// The Spaces the agent may read and write in this turn: its own and
    /// its manifest's, each only if the user allowed it to be sent to the
    /// provider and its transfer policy lets data go there.
    async fn reach_of(
        &self,
        own: &str,
        agent: &Agent,
        authorized: &BTreeSet<String>,
    ) -> Result<Reach, Refusal> {
        if !authorized.contains(own) {
            return Err(refusal(
                "consent",
                format!("space {own} is not allowed to be sent to the AI provider; allow it in the settings"),
            ));
        }
        let manifest = agent.manifest.clone().unwrap_or_default();
        let mut read = BTreeSet::new();
        for s in std::iter::once(own)
            .chain(manifest.reads.iter().map(String::as_str))
            .chain(manifest.writes.iter().map(String::as_str))
        {
            if !authorized.contains(s) {
                continue;
            }
            let Ok(sid) = ObjectId::parse(s) else {
                continue;
            };
            let decision = crate::policy::check(
                &self.store,
                sid,
                TransferOp::External,
                PROVIDER_DESTINATION,
                false,
            )
            .await
            .map_err(store_refusal)?;
            match decision {
                Ok(()) => {
                    read.insert(s.to_owned());
                }
                Err(e) if s == own => return Err(refusal("policy", e.to_string())),
                Err(_) => {}
            }
        }
        let write = std::iter::once(own)
            .chain(manifest.writes.iter().map(String::as_str))
            .filter(|s| read.contains(*s))
            .map(str::to_owned)
            .collect();
        Ok(Reach {
            read,
            write,
            extensions: manifest.extensions.iter().cloned().collect(),
            actor: format!("agent:{}", agent.id),
        })
    }

    async fn account_for(&self, chosen: &str) -> Result<String, Refusal> {
        if !chosen.is_empty() {
            return Ok(chosen.to_owned());
        }
        self.store
            .ai_accounts()
            .await
            .map_err(store_refusal)?
            .into_iter()
            .find(|a| !a.needs_sign_in)
            .map(|a| a.id)
            .ok_or_else(|| refusal("sign_in", "no AI account on this device; sign in first"))
    }

    /// The base prompt, the agent's place, its instructions and prompts,
    /// and the skills it can load.
    async fn instructions_for(
        &self,
        space: SpaceId,
        agent: &Agent,
        reach: &Reach,
        skills: &[Skill],
    ) -> String {
        use std::fmt::Write as _;
        let mut out = super::skills::BASE.to_owned();
        let join = |s: &BTreeSet<String>| s.iter().cloned().collect::<Vec<_>>().join(", ");
        let _ = write!(
            out,
            "\n\nYour Space is {space}. You may read spaces {} and write spaces {}.\n",
            join(&reach.read),
            join(&reach.write),
        );
        if !agent.instructions.trim().is_empty() {
            let _ = write!(
                out,
                "\n# Your instructions\n\n{}\n",
                agent.instructions.trim()
            );
        }
        if let Ok(store) = self.store.space(space).await {
            for p in &agent.prompt_ids {
                let Ok(pid) = ObjectId::parse(p) else {
                    continue;
                };
                let Ok(po) = store.get(pid).await else {
                    continue;
                };
                if po.kind() == ObjectKind::Prompt && po.deleted_at_ms.is_none() {
                    let pr = prompt_of(&po);
                    let _ = write!(out, "\n# {}\n\n{}\n", pr.name, pr.body.trim());
                }
            }
        }
        if !skills.is_empty() {
            out.push_str("\n# Skills you can load\n\n");
            for s in skills {
                let _ = writeln!(out, "- {}: {}", s.name, s.description);
            }
        }
        out
    }

    async fn prepare(&self, space_id: SpaceId, agent_id: &str) -> Result<Prepared, Refusal> {
        let store = self.store.space(space_id).await.map_err(store_refusal)?;
        let id = ObjectId::parse(agent_id).map_err(|_| refusal("invalid", "agent id"))?;
        let o = store.get(id).await.map_err(store_refusal)?;
        if o.kind() != ObjectKind::Agent || o.deleted_at_ms.is_some() {
            return Err(refusal("invalid", "the conversation's agent is gone"));
        }
        let agent = self
            .with_effective_version(space_id, agent_of(&o))
            .await
            .map_err(store_refusal)?;
        let ai = self
            .store
            .settings()
            .await
            .map_err(store_refusal)?
            .ai
            .unwrap_or_default();
        let authorized: BTreeSet<String> = ai.authorized_space_ids.iter().cloned().collect();
        let reach = self
            .reach_of(&space_id.to_string(), &agent, &authorized)
            .await?;
        let granted = crate::rules::grant_or_auto(&self.store, space_id, &as_script(&agent))
            .await
            .map_err(store_refusal)?;
        if granted.is_none() {
            return Err(refusal(
                "consent",
                format!(
                    "this version of agent {} is not granted on this device",
                    agent.name
                ),
            ));
        }
        let account = self.account_for(&ai.default_account_id).await?;
        let model = if agent.model.is_empty() {
            ai.default_model.clone()
        } else {
            agent.model.clone()
        };
        if model.is_empty() {
            return Err(refusal(
                "invalid",
                "no model chosen for the agent or the device",
            ));
        }
        let effort = if agent.effort.is_empty() {
            ai.default_effort.clone()
        } else {
            agent.effort.clone()
        };
        let skills = self
            .skills_for(space_id, &agent)
            .await
            .map_err(store_refusal)?;
        let instructions = self
            .instructions_for(space_id, &agent, &reach, &skills)
            .await;
        Ok(Prepared {
            account,
            model,
            effort,
            instructions,
            skills,
            reach,
        })
    }

    async fn save_items(
        &self,
        space: SpaceId,
        conversation: ObjectId,
        items: &[serde_json::Value],
        actor: &str,
    ) -> Result<(), AiError> {
        let values: Vec<elri_proto::elri::v1::object::Value> =
            items.iter().map(|i| string_value(i.to_string())).collect();
        self.store
            .space(space)
            .await?
            .update_as(
                conversation,
                move |doc| doc.set_field(field::ITEMS, &list_value(values)),
                actor,
            )
            .await?;
        Ok(())
    }

    async fn run_tool(
        &self,
        prep: &Prepared,
        item: &serde_json::Value,
        labels: &mut BTreeSet<String>,
    ) -> Result<String, String> {
        let args: serde_json::Value = item
            .get("arguments")
            .and_then(|a| a.as_str())
            .and_then(|a| serde_json::from_str(a).ok())
            .unwrap_or_default();
        let arg = |k: &str| {
            args.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned()
        };
        match tool_name(item).as_str() {
            "describe" => Ok(super::call::describe(&arg("target"))),
            "load_skill" => {
                let name = arg("name");
                prep.skills
                    .iter()
                    .find(|s| s.name == name || s.id == name)
                    .map(|s| s.body.clone())
                    .ok_or_else(|| format!("no skill {name} for this agent"))
            }
            "call" => {
                let request: serde_json::Value = serde_json::from_str(&arg("request"))
                    .map_err(|e| format!("request is not JSON: {e}"))?;
                let store = Arc::clone(&self.store);
                let copy_check = move |labels: &BTreeSet<String>,
                                       dest: &str|
                      -> futures_util::future::BoxFuture<
                    'static,
                    Result<(), String>,
                > {
                    let store = Arc::clone(&store);
                    let labels = labels.clone();
                    let dest = dest.to_owned();
                    Box::pin(async move {
                        crate::policy::check_labels(&store, &labels, TransferOp::Copy, &dest).await
                    })
                };
                self.caller
                    .call(&prep.reach, &arg("method"), &request, labels, &copy_check)
                    .await
            }
            other => Err(format!("no tool {other}")),
        }
    }

    /// One model request and the tool calls it asks for. Returns whether
    /// the agent wants to continue.
    async fn step(
        &self,
        prep: &Prepared,
        place: (SpaceId, ObjectId),
        items: &mut Vec<serde_json::Value>,
        labels: &mut BTreeSet<String>,
        tx: &mpsc::Sender<TurnEvent>,
    ) -> Result<bool, Refusal> {
        let mut body = serde_json::json!({
            "model": prep.model,
            "instructions": prep.instructions,
            "input": for_provider(items),
            "tools": tools(),
            "tool_choice": "auto",
            "parallel_tool_calls": false,
            "store": false,
            "stream": true,
            "include": ["reasoning.encrypted_content"],
        });
        if !prep.effort.is_empty() {
            body["reasoning"] = serde_json::json!({ "effort": prep.effort });
        }
        let deltas = tx.clone();
        let mut on_event = move |e: super::openai::StreamEvent| {
            let super::openai::StreamEvent::TextDelta(d) = e;
            let _ = deltas.try_send(TurnEvent {
                event: Some(turn_event::Event::TextDelta(d)),
            });
        };
        let done = self
            .respond(&prep.account, &body, &mut on_event)
            .await
            .map_err(|e| match e {
                AiError::Provider { code, message } => (code, message),
                AiError::SignIn(m) => refusal("sign_in", m),
                other => refusal("transport", other.to_string()),
            })?;
        let at = crate::model::now_ms();
        let mut calls = Vec::new();
        for mut item in done.items {
            if let Some(o) = item.as_object_mut() {
                o.insert("at_ms".into(), at.into());
                if o.get("type").and_then(|t| t.as_str()) == Some("message") {
                    o.insert("elri_model".into(), prep.model.clone().into());
                }
            }
            match item.get("type").and_then(|t| t.as_str()) {
                Some("message") => {
                    emit(
                        tx,
                        turn_event::Event::Message(Message {
                            role: MessageRole::Assistant.into(),
                            text: item_text(&item),
                            model: prep.model.clone(),
                            at_ms: at,
                            ..Message::default()
                        }),
                    )
                    .await;
                }
                Some("function_call") => calls.push(item.clone()),
                _ => {}
            }
            items.push(item);
        }
        self.save_items(place.0, place.1, items, &prep.reach.actor)
            .await
            .map_err(store_refusal)?;
        if calls.is_empty() {
            return Ok(false);
        }
        self.run_calls(prep, calls, items, labels, tx).await;
        self.save_items(place.0, place.1, items, &prep.reach.actor)
            .await
            .map_err(store_refusal)?;
        Ok(true)
    }

    /// Runs the tool calls of one response in order, reporting each and
    /// adding its output to the items.
    async fn run_calls(
        &self,
        prep: &Prepared,
        calls: Vec<serde_json::Value>,
        items: &mut Vec<serde_json::Value>,
        labels: &mut BTreeSet<String>,
        tx: &mpsc::Sender<TurnEvent>,
    ) {
        for call in calls {
            let tool = tool_name(&call);
            let arguments_json = call
                .get("arguments")
                .and_then(|a| a.as_str())
                .unwrap_or("")
                .to_owned();
            emit(
                tx,
                turn_event::Event::ToolCall(ToolCall {
                    tool: tool.clone(),
                    arguments_json,
                }),
            )
            .await;
            let (output, failed) = match self.run_tool(prep, &call, labels).await {
                Ok(o) => (o, false),
                Err(e) => (format!("refused: {e}"), true),
            };
            let output = clip(output);
            emit(
                tx,
                turn_event::Event::ToolResult(ToolResult {
                    tool,
                    result: output.clone(),
                    failed,
                }),
            )
            .await;
            items.push(serde_json::json!({
                "type": "function_call_output",
                "call_id": call.get("call_id").cloned().unwrap_or_default(),
                "output": output,
                "at_ms": crate::model::now_ms(),
                "elri_failed": failed,
            }));
        }
    }

    /// Runs one turn of a conversation: saves the user's message, then
    /// lets the agent answer, reporting each step on `tx`.
    pub async fn run_turn(
        self: &Arc<Self>,
        space_id: SpaceId,
        conversation: ObjectId,
        text: String,
        tx: mpsc::Sender<TurnEvent>,
    ) {
        if let Err((code, message)) = self.turn(space_id, conversation, text, &tx).await {
            emit(&tx, turn_event::Event::Error(TurnError { code, message })).await;
        }
    }

    async fn turn(
        &self,
        space_id: SpaceId,
        conversation: ObjectId,
        text: String,
        tx: &mpsc::Sender<TurnEvent>,
    ) -> Result<(), Refusal> {
        let conv = async { self.store.space(space_id).await?.get(conversation).await }
            .await
            .ok()
            .filter(|o| o.kind() == ObjectKind::Conversation)
            .ok_or_else(|| refusal("invalid", "no such conversation"))?;
        let agent_id = string_field(&conv, field::AGENT)
            .unwrap_or_default()
            .to_owned();
        let prep = self.prepare(space_id, &agent_id).await?;
        let mut items = items_of(&conv);
        let now = crate::model::now_ms();
        items.push(serde_json::json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}],
            "at_ms": now,
        }));
        self.save_items(space_id, conversation, &items, crate::store::ACTOR_USER)
            .await
            .map_err(store_refusal)?;
        emit(
            tx,
            turn_event::Event::Message(Message {
                role: MessageRole::User.into(),
                text,
                at_ms: now,
                ..Message::default()
            }),
        )
        .await;
        let mut labels: BTreeSet<String> = BTreeSet::from([space_id.to_string()]);
        for _ in 0..MAX_STEPS {
            if !self
                .step(&prep, (space_id, conversation), &mut items, &mut labels, tx)
                .await?
            {
                return Ok(());
            }
        }
        Err(refusal(
            "step_limit",
            format!("the agent made {MAX_STEPS} rounds of tool calls without finishing; ask it to continue"),
        ))
    }
}

async fn emit(tx: &mpsc::Sender<TurnEvent>, e: turn_event::Event) {
    let _ = tx.send(TurnEvent { event: Some(e) }).await;
}

fn clip(mut s: String) -> String {
    if s.len() > ANSWER_LIMIT {
        let mut cut = ANSWER_LIMIT;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
    s
}

/// A manifest with nothing beyond the agent's own Space.
pub fn own_space_only() -> Manifest {
    Manifest::default()
}
