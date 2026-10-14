//! `elri`: talks to the core running on this device through the endpoint file.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use elri_core::Endpoint;
use elri_proto::elri::v1::attachment::attachment_service_ws::AttachmentServiceClient;
use elri_proto::elri::v1::attachment::{
    upload_request, Attachment, DownloadRequest, ListAttachmentsRequest, PinRequest, UploadHeader,
    UploadRequest,
};
use elri_proto::elri::v1::backup::backup_service_ws::BackupServiceClient;
use elri_proto::elri::v1::backup::CreateBackupRequest;
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::{
    Change, DiscardChangeRequest, ListChangesRequest, RetryChangeRequest,
};
use elri_proto::elri::v1::core::core_service_ws::CoreServiceClient;
use elri_proto::elri::v1::core::GetInfoRequest;
use elri_proto::elri::v1::definition::definition_service_ws::DefinitionServiceClient;
use elri_proto::elri::v1::definition::{
    Constraint, CreateConstraintRequest, ListConstraintsRequest, ListViolationsRequest,
    UpdateConstraintRequest,
};
use elri_proto::elri::v1::events::event_service_ws::EventServiceClient;
use elri_proto::elri::v1::events::{event, WatchRequest};
use elri_proto::elri::v1::extension::extension_service_ws::ExtensionServiceClient;
use elri_proto::elri::v1::extension::{
    install_extension_request, CallExtensionRequest, Extension, ExtensionKind,
    GrantExtensionRequest, InstallExtensionRequest, InstallHeader, ListExtensionsRequest,
    RemoveExtensionRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, Object,
    ObjectKind, Participant, Placement, PurgeObjectRequest, UpdateObjectRequest, Value, ValueList,
    ValueMap,
};
use elri_proto::elri::v1::remote::remote_service_ws::RemoteServiceClient;
use elri_proto::elri::v1::remote::{
    AddMemberRequest, ConnectRequest, CreateRemoteSpaceRequest, DisconnectRequest,
    GetStatusRequest, JoinSpaceRequest, ListRemoteSpacesRequest,
};
use elri_proto::elri::v1::script::grant_service_ws::GrantServiceClient;
use elri_proto::elri::v1::script::rule_service_ws::RuleServiceClient;
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{
    CheckScriptRequest, CreateRuleRequest, CreateScriptRequest, GetPolicyRequest,
    GrantScriptRequest, ListGrantsRequest, ListRuleRunsRequest, ListRulesRequest,
    ListScriptsRequest, Manifest, RevokeGrantRequest, Rule, RunScriptRequest, Script,
    SetPolicyRequest, Trigger, UpdateRuleRequest,
};
use elri_proto::elri::v1::search::search_service_ws::SearchServiceClient;
use elri_proto::elri::v1::search::SearchRequest;
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::Role;
use elri_proto::elri::v1::space::{CreateSpaceRequest, ListSpacesRequest};
use elri_proto::elri::v1::transfer::transfer_service_ws::TransferServiceClient;
use elri_proto::elri::v1::transfer::{
    ExportObjectsRequest, ImportObjectsRequest, MoveObjectsRequest,
};
use elri_proto::elri::v1::ui::interface_service_ws::InterfaceServiceClient;
use elri_proto::elri::v1::ui::{
    CreateInterfaceRequest, EvaluateRequest, Interface, ListInterfacesRequest, Node,
    PublishInterfaceRequest, SelectVersionRequest, UpdateInterfaceRequest,
};
use serde_json::json;
use ws_proto::{Client, ClientConfig, Request};

#[derive(Parser, Debug)]
#[command(name = "elri", version, about)]
struct Args {
    /// Data directory of the running core (defaults to the platform data dir).
    #[arg(long, env = "ELRI_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Show what the core reports about itself.
    Info,
    /// Print core events as they happen, one JSON object per line.
    Watch {
        /// Only these Spaces; every Space when omitted.
        #[arg(long = "space")]
        spaces: Vec<String>,
    },
    /// Spaces on this device.
    Space {
        #[command(subcommand)]
        cmd: SpaceCmd,
    },
    /// Objects of a Space.
    Object {
        #[command(subcommand)]
        cmd: ObjectCmd,
    },
    /// Full-text search in a Space.
    Search {
        #[arg(long)]
        space: String,
        /// Words to find; `AND`, `OR`, quoted phrases, and `-word` work.
        query: String,
        /// Kind: entity, relation, layer, field-def, type-def, role-def.
        #[arg(long, value_parser = parse_kind)]
        kind: Option<ObjectKind>,
        #[arg(long, default_value_t = 20)]
        limit: i32,
    },
    /// Constraints: rules over field values that are reported or enforced.
    Constraint {
        #[command(subcommand)]
        cmd: ConstraintCmd,
    },
    /// Luau scripts kept in a Space, run on this device.
    Script {
        #[command(subcommand)]
        cmd: ScriptCmd,
    },
    /// Device grants: consent to run scripts.
    Grant {
        #[command(subcommand)]
        cmd: GrantCmd,
    },
    /// Rules: scripts that run when something happens.
    Rule {
        #[command(subcommand)]
        cmd: RuleCmd,
    },
    /// Extensions installed on this device: WASM modules and native libraries.
    Extension {
        #[command(subcommand)]
        cmd: ExtensionCmd,
    },
    /// Objects between Spaces: export, import, move.
    Transfer {
        #[command(subcommand)]
        cmd: TransferCmd,
    },
    /// Interfaces: trees of catalog nodes the renderer draws.
    Ui {
        #[command(subcommand)]
        cmd: UiCmd,
    },
    /// Attachments: files kept beside a Space.
    Attachment {
        #[command(subcommand)]
        cmd: AttachmentCmd,
    },
    /// Local backups.
    Backup {
        #[command(subcommand)]
        cmd: BackupCmd,
    },
    /// Changes recorded in a Space, newest first.
    Change {
        #[command(subcommand)]
        cmd: ChangeCmd,
    },
    /// The server this device synchronizes with.
    Remote {
        #[command(subcommand)]
        cmd: RemoteCmd,
    },
}

#[derive(Subcommand, Debug)]
enum RemoteCmd {
    /// Store the server URL and token after verifying them. The token is
    /// read from `ELRI_TOKEN` or, failing that, from standard input, so it
    /// never lands in shell history or the process list.
    Connect {
        /// WebSocket URL of the server.
        url: String,
    },
    /// Forget the server; replicated Spaces stay on the device.
    Disconnect,
    /// Connection and per-Space replication status.
    Status,
    /// Spaces visible on the server.
    Spaces,
    /// Create a server Space and join it.
    Create { name: String },
    /// Replicate a server Space to this device.
    Join { space: String },
    /// Owner/Admin: add a member by user name.
    AddMember {
        #[arg(long)]
        space: String,
        user: String,
        /// Role: admin, editor, or viewer.
        #[arg(long, default_value = "editor", value_parser = parse_role)]
        role: Role,
    },
}

#[derive(Subcommand, Debug)]
enum ScriptCmd {
    /// Create a script from a Luau source file.
    Create {
        #[arg(long)]
        space: String,
        name: String,
        /// Luau source file.
        file: PathBuf,
        /// Spaces read besides its own.
        #[arg(long = "read")]
        reads: Vec<String>,
        /// Spaces written besides its own.
        #[arg(long = "write")]
        writes: Vec<String>,
        /// Extensions the script calls.
        #[arg(long = "ext")]
        extensions: Vec<String>,
    },
    /// List scripts of a Space.
    List {
        #[arg(long)]
        space: String,
    },
    /// Compile a source file without running it.
    Check { file: PathBuf },
    /// Run a script now; ARGS is JSON for `elri.args`.
    Run {
        #[arg(long)]
        space: String,
        id: String,
        #[arg(default_value = "null")]
        args: String,
    },
}

#[derive(Subcommand, Debug)]
enum ConstraintCmd {
    /// Constraints of a Space.
    List {
        #[arg(long)]
        space: String,
        #[arg(long)]
        include_deleted: bool,
    },
    /// Add a unique constraint on a field; diagnostic unless --strict.
    Add {
        #[arg(long)]
        space: String,
        /// Field definition id.
        #[arg(long)]
        field: String,
        name: String,
        /// Refuse writes that break it instead of reporting them.
        #[arg(long)]
        strict: bool,
    },
    /// Rename a constraint or switch it between diagnostic and strict.
    Set {
        #[arg(long)]
        space: String,
        id: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        strict: Option<bool>,
    },
    /// Live Objects that break a constraint of the Space.
    Violations {
        #[arg(long)]
        space: String,
    },
}

#[derive(Subcommand, Debug)]
enum GrantCmd {
    /// Grants on this device.
    List {
        #[arg(long)]
        include_revoked: bool,
    },
    /// Consent to a script's current version.
    Add {
        #[arg(long)]
        space: String,
        script: String,
    },
    /// Revoke a grant by id.
    Revoke { id: String },
    /// Show or set whether benign scripts run without asking.
    Policy {
        #[arg(long)]
        auto_benign: Option<bool>,
    },
}

#[derive(Subcommand, Debug)]
enum RuleCmd {
    /// Create a rule that runs a script when an Object of the Space changes.
    Create {
        #[arg(long)]
        space: String,
        name: String,
        /// The script to run.
        script: String,
        /// Only these kinds: entity, relation, layer.
        #[arg(long = "kind", value_parser = parse_kind)]
        kinds: Vec<ObjectKind>,
        /// Only Objects of this type.
        #[arg(long = "type")]
        type_id: Option<String>,
        /// Also run every N seconds on this device.
        #[arg(long, default_value_t = 0)]
        every: i64,
        /// With --every: also fire on events (default: schedule only).
        #[arg(long)]
        on_events: bool,
    },
    /// Recent runs of a rule on this device.
    Runs {
        #[arg(long)]
        space: String,
        id: String,
    },
    /// List rules of a Space.
    List {
        #[arg(long)]
        space: String,
    },
    /// Enable or disable a rule.
    Enable {
        #[arg(long)]
        space: String,
        id: String,
        #[arg(long)]
        off: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ExtensionCmd {
    /// Install a module file as an extension.
    Install {
        name: String,
        /// `.wasm` module, or a native library with `--native`.
        file: PathBuf,
        /// Functions the module exports for callers.
        #[arg(long = "fn", required = true)]
        functions: Vec<String>,
        /// A native dynamic library (desktop only, at your own risk).
        #[arg(long)]
        native: bool,
        /// Spaces read besides the one of each call.
        #[arg(long = "read")]
        reads: Vec<String>,
        /// Spaces written besides the one of each call.
        #[arg(long = "write")]
        writes: Vec<String>,
    },
    /// List installed extensions.
    List,
    /// Remove an extension and its modules.
    Remove { name: String },
    /// Consent to the installed version.
    Grant {
        name: String,
        /// Required for a native extension.
        #[arg(long)]
        accept_native_risk: bool,
    },
    /// Call a function with JSON arguments in the context of a Space.
    Call {
        #[arg(long)]
        space: String,
        name: String,
        function: String,
        #[arg(default_value = "null")]
        args: String,
    },
}

#[derive(Subcommand, Debug)]
enum TransferCmd {
    /// Export Objects to a bundle file.
    Export {
        #[arg(long)]
        space: String,
        /// Where to write the bundle.
        #[arg(long)]
        out: PathBuf,
        /// Also what they refer to within the Space.
        #[arg(long)]
        deps: bool,
        ids: Vec<String>,
    },
    /// Import a bundle file as copies with fresh ids.
    Import {
        #[arg(long)]
        space: String,
        file: PathBuf,
        /// Keep the original ids (refused if any is taken).
        #[arg(long)]
        keep_ids: bool,
    },
    /// Move Objects between two local Spaces, keeping their ids.
    Move {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        ids: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
enum UiCmd {
    /// Create an interface draft.
    Create {
        #[arg(long)]
        space: String,
        name: String,
        /// Declared local state as a JSON object.
        #[arg(long, default_value = "{}")]
        state: String,
    },
    /// List drafts of a Space, or the versions of one draft.
    List {
        #[arg(long)]
        space: String,
        #[arg(long)]
        versions_of: Option<String>,
    },
    /// Add a node (or rewrite one with --id) in a draft.
    Node {
        #[arg(long)]
        space: String,
        /// The draft.
        id: String,
        /// Catalog type: column, row, text, button, input, checkbox, list, table, card.
        #[arg(long = "type")]
        node_type: String,
        /// Rewrite this node instead of creating one.
        #[arg(long)]
        node_id: Option<String>,
        #[arg(long, default_value = "")]
        parent: String,
        #[arg(long, default_value_t = i32::MAX)]
        index: i32,
        /// Literal properties, JSON object.
        #[arg(long, default_value = "{}")]
        props: String,
        /// Bindings: property → Luau expression, JSON object.
        #[arg(long, default_value = "{}")]
        bind: String,
        /// Handlers: event → Luau source, JSON object.
        #[arg(long, default_value = "{}")]
        on: String,
    },
    /// Remove nodes from a draft.
    Remove {
        #[arg(long)]
        space: String,
        id: String,
        nodes: Vec<String>,
    },
    /// Freeze the draft as the next version.
    Publish {
        #[arg(long)]
        space: String,
        id: String,
    },
    /// Choose which version this device shows (0 = the draft).
    Select {
        #[arg(long)]
        space: String,
        id: String,
        version: i32,
    },
    /// Evaluate every binding against the Space and STATE.
    Eval {
        #[arg(long)]
        space: String,
        id: String,
        #[arg(default_value = "{}")]
        state: String,
    },
}

#[derive(Subcommand, Debug)]
enum AttachmentCmd {
    /// Upload a file as an attachment.
    Upload {
        #[arg(long)]
        space: String,
        /// File to upload.
        file: PathBuf,
        /// Display name; the file name when omitted.
        #[arg(long)]
        name: Option<String>,
        /// Media type.
        #[arg(long = "type", default_value = "application/octet-stream")]
        media_type: String,
    },
    /// Download an attachment's bytes to a file.
    Download {
        #[arg(long)]
        space: String,
        id: String,
        /// Where to write the bytes.
        out: PathBuf,
    },
    /// List attachments of a Space.
    List {
        #[arg(long)]
        space: String,
        #[arg(long)]
        include_deleted: bool,
    },
    /// Keep the bytes on this device (fetching them if needed), or stop.
    Pin {
        #[arg(long)]
        space: String,
        id: String,
        #[arg(long)]
        off: bool,
    },
}

#[derive(Subcommand, Debug)]
enum BackupCmd {
    /// Write a verified backup of every database into a new directory
    /// under DEST. Secrets are not included.
    Create {
        /// Directory to create the backup in.
        dest: String,
    },
    /// Check a backup's manifest and checksums.
    Check {
        /// Backup directory.
        dir: String,
    },
    /// Restore a backup into the data directory. The core must be stopped.
    /// The restored device gets a fresh identity and no server token.
    Restore {
        /// Backup directory.
        dir: String,
        /// Move an existing data directory aside instead of refusing.
        #[arg(long)]
        replace: bool,
        /// Keep the device identity recorded in the backup.
        #[arg(long)]
        keep_identity: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ChangeCmd {
    /// List Changes.
    List {
        #[arg(long)]
        space: String,
        #[arg(long, default_value_t = 50)]
        limit: i32,
        /// Only Changes that touched this Object: its history.
        #[arg(long)]
        object: Option<String>,
    },
    /// Resubmit a rejected Change unchanged, with its paused dependents.
    Retry {
        #[arg(long)]
        space: String,
        id: String,
    },
    /// Drop a rejected or paused Change and its paused dependents for good.
    Discard {
        #[arg(long)]
        space: String,
        id: String,
    },
}

#[derive(Subcommand, Debug)]
enum SpaceCmd {
    /// List Spaces.
    List,
    /// Create a local Space.
    Create {
        /// Display name.
        name: String,
    },
}

/// Parses a protocol enum from its short lowercase name (`entity`,
/// `field-def`, `editor`, ...), given the enum's name prefix.
fn parse_enum<E: TryFrom<i32> + Copy>(
    prefix: &str,
    s: &str,
    from_str_name: fn(&str) -> Option<E>,
) -> Result<E, String> {
    let name = format!(
        "{prefix}_{}",
        s.trim().to_ascii_uppercase().replace('-', "_")
    );
    from_str_name(&name).ok_or_else(|| format!("unknown value `{s}`"))
}

fn parse_kind(s: &str) -> Result<ObjectKind, String> {
    let k = parse_enum("OBJECT_KIND", s, ObjectKind::from_str_name)?;
    if k == ObjectKind::Unspecified {
        return Err("kind is required".to_owned());
    }
    Ok(k)
}

fn parse_role(s: &str) -> Result<Role, String> {
    match parse_enum("ROLE", s, Role::from_str_name)? {
        Role::Unspecified | Role::Owner => Err("role must be admin, editor, or viewer".to_owned()),
        r => Ok(r),
    }
}

#[derive(Subcommand, Debug)]
enum ObjectCmd {
    /// Create an Object.
    Create {
        /// Space id.
        #[arg(long)]
        space: String,
        /// Kind: entity, relation, or layer.
        #[arg(long, default_value = "entity", value_parser = parse_kind)]
        kind: ObjectKind,
        /// Type-definition ids to apply.
        #[arg(long = "type")]
        types: Vec<String>,
        /// Fields as `id=value`; value is parsed as bool, int, float, `ref:<id>`,
        /// `time:<ms>`, `text:<collaborative text>`, `list:a,b,c`, `map:k=v,k2=v2`,
        /// or a string.
        #[arg(long = "field", value_parser = parse_field)]
        fields: Vec<(String, Value)>,
        /// Relation participants as `slot=object_id[:role_id]`.
        #[arg(long = "participant", value_parser = parse_participant)]
        participants: Vec<Participant>,
        /// Layers only: the Object this Layer enriches.
        #[arg(long)]
        target: Option<String>,
    },
    /// Show an Object.
    Get {
        #[arg(long)]
        space: String,
        id: String,
    },
    /// List Objects.
    List {
        #[arg(long)]
        space: String,
        /// Kind: entity, relation, layer, field-def, type-def, role-def.
        #[arg(long, value_parser = parse_kind)]
        kind: Option<ObjectKind>,
        #[arg(long = "type")]
        type_id: Option<String>,
        #[arg(long)]
        include_deleted: bool,
        /// Relations in which this Object participates.
        #[arg(long)]
        participant_of: Option<String>,
        /// Layers over this Object.
        #[arg(long)]
        layers_of: Option<String>,
        /// Entities that place this block.
        #[arg(long)]
        places: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: i32,
    },
    /// Delete an Object (recoverable until purge).
    Delete {
        #[arg(long)]
        space: String,
        id: String,
    },
    /// Erase a deleted Object for good; its id can never be reused.
    Purge {
        #[arg(long)]
        space: String,
        id: String,
    },
    /// Place a block Object inside a document Entity.
    Place {
        #[arg(long)]
        space: String,
        /// The document.
        id: String,
        /// The block to place.
        block: String,
        /// Parent placement id; top level when omitted.
        #[arg(long, default_value = "")]
        parent: String,
        /// Position among siblings; the end when omitted.
        #[arg(long, default_value_t = i32::MAX)]
        index: i32,
    },
    /// Move a placement within a document.
    Move {
        #[arg(long)]
        space: String,
        /// The document.
        id: String,
        /// The placement to move.
        placement: String,
        /// New parent placement id; top level when omitted.
        #[arg(long, default_value = "")]
        parent: String,
        /// New position among siblings; the end when omitted.
        #[arg(long, default_value_t = i32::MAX)]
        index: i32,
    },
    /// Remove a placement and its subtree; the blocks stay.
    Unplace {
        #[arg(long)]
        space: String,
        /// The document.
        id: String,
        /// The placement to remove.
        placement: String,
    },
}

fn parse_field(s: &str) -> Result<(String, Value), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| "expected id=value".to_owned())?;
    if k.is_empty() {
        return Err("field id must not be empty".to_owned());
    }
    let v = if let Some(id) = v.strip_prefix("ref:") {
        value::Value::Ref(id.to_owned())
    } else if let Some(t) = v.strip_prefix("text:") {
        value::Value::Text(t.to_owned())
    } else if let Some(items) = v.strip_prefix("list:") {
        value::Value::List(ValueList {
            items: items
                .split(',')
                .map(str::trim)
                .filter(|x| !x.is_empty())
                .map(|x| Value {
                    value: Some(value::Value::String(x.to_owned())),
                })
                .collect(),
        })
    } else if let Some(entries) = v.strip_prefix("map:") {
        value::Value::Map(ValueMap {
            entries: entries
                .split(',')
                .map(str::trim)
                .filter(|x| !x.is_empty())
                .map(|x| {
                    let (k, v) = x.split_once('=').unwrap_or((x, ""));
                    (
                        k.trim().to_owned(),
                        Value {
                            value: Some(value::Value::String(v.trim().to_owned())),
                        },
                    )
                })
                .collect(),
        })
    } else if let Some(ms) = v.strip_prefix("time:") {
        value::Value::TimeMs(ms.parse().map_err(|e| format!("time: {e}"))?)
    } else if v == "true" || v == "false" {
        value::Value::Bool(v == "true")
    } else if let Ok(i) = v.parse::<i64>() {
        value::Value::Int(i)
    } else if let Ok(d) = v.parse::<f64>() {
        value::Value::Double(d)
    } else {
        value::Value::String(v.to_owned())
    };
    Ok((k.to_owned(), Value { value: Some(v) }))
}

fn parse_participant(s: &str) -> Result<Participant, String> {
    let (slot, rest) = s
        .split_once('=')
        .ok_or_else(|| "expected slot=object_id[:role_id]".to_owned())?;
    let (object_id, role_id) = rest.split_once(':').unwrap_or((rest, ""));
    if slot.is_empty() || object_id.is_empty() {
        return Err("slot and object_id must not be empty".to_owned());
    }
    Ok(Participant {
        slot: slot.to_owned(),
        object_id: object_id.to_owned(),
        role_id: role_id.to_owned(),
    })
}

fn default_data_dir() -> Result<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .context("cannot determine the data directory")?;
    Ok(base.join("elri"))
}

/// The core checks the launch token on every call, so it rides as a
/// per-request header rather than on the WebSocket upgrade.
fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn value_json(v: &Value) -> serde_json::Value {
    match &v.value {
        Some(value::Value::Bool(b)) => json!(b),
        Some(value::Value::Int(i)) => json!(i),
        Some(value::Value::Double(d)) => json!(d),
        Some(value::Value::String(s)) => json!(s),
        Some(value::Value::TimeMs(t)) => json!({ "time_ms": t }),
        Some(value::Value::Ref(r)) => json!({ "ref": r }),
        Some(value::Value::Text(t)) => json!({ "text": t }),
        Some(value::Value::Rich(r)) => json!({
            "text": r.text,
            "marks": r.marks.iter().map(|m| json!({
                "start": m.start, "end": m.end, "key": m.key,
                "value": serde_json::from_str::<serde_json::Value>(&m.value_json).unwrap_or(json!(m.value_json)),
            })).collect::<Vec<_>>(),
        }),
        Some(value::Value::List(l)) => json!(l.items.iter().map(value_json).collect::<Vec<_>>()),
        Some(value::Value::Map(m)) => json!(m
            .entries
            .iter()
            .map(|(k, v)| (k.clone(), value_json(v)))
            .collect::<serde_json::Map<_, _>>()),
        None => serde_json::Value::Null,
    }
}

fn object_json(o: &Object) -> serde_json::Value {
    let fields: serde_json::Map<String, serde_json::Value> = o
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), value_json(v)))
        .collect();
    let participants: Vec<_> = o
        .participants
        .iter()
        .map(|p| json!({ "slot": p.slot, "object_id": p.object_id, "role_id": p.role_id }))
        .collect();
    let placements: Vec<_> = o
        .placements
        .iter()
        .map(|p| {
            json!({ "id": p.id, "parent_id": p.parent_id, "index": p.index, "block_id": p.block_id })
        })
        .collect();
    json!({
        "id": o.id,
        "space_id": o.space_id,
        "kind": o.kind().as_str_name(),
        "types": o.types,
        "fields": fields,
        "participants": participants,
        "placements": placements,
        "target_id": o.target_id,
        "created_at_ms": o.created_at_ms,
        "updated_at_ms": o.updated_at_ms,
        "deleted_at_ms": o.deleted_at_ms,
    })
}

fn script_json(s: &Script) -> serde_json::Value {
    json!({
        "id": s.id,
        "space_id": s.space_id,
        "name": s.name,
        "version": s.version,
        "benign": s.benign,
        "manifest": s.manifest.as_ref().map(|m| json!({
            "reads": m.reads, "writes": m.writes, "http": m.http, "ai": m.ai,
        })),
        "source_bytes": s.source.len(),
        "created_at_ms": s.created_at_ms,
        "updated_at_ms": s.updated_at_ms,
    })
}

fn rule_json(r: &Rule) -> serde_json::Value {
    json!({
        "id": r.id,
        "space_id": r.space_id,
        "name": r.name,
        "script_id": r.script_id,
        "trigger": r.trigger.as_ref().map(|t| json!({
            "kinds": t.kinds.iter().map(|k| ObjectKind::try_from(*k).map(|k| k.as_str_name().to_lowercase()).unwrap_or_default()).collect::<Vec<_>>(),
            "type_id": t.type_id,
        })),
        "executor_device_id": r.executor_device_id,
        "enabled": r.enabled,
    })
}

async fn run_script(
    endpoint: &Endpoint,
    conn: Client,
    cmd: ScriptCmd,
) -> Result<serde_json::Value> {
    let client = ScriptServiceClient::new(conn);
    match cmd {
        ScriptCmd::Create {
            space,
            name,
            file,
            reads,
            writes,
            extensions,
        } => {
            let source = std::fs::read_to_string(&file)?;
            let res = client
                .create_script(authed(
                    endpoint,
                    CreateScriptRequest {
                        space_id: space,
                        name,
                        source,
                        manifest: Some(Manifest {
                            reads,
                            writes,
                            http: false,
                            ai: false,
                            extensions,
                        }),
                    },
                ))
                .await?;
            let Some(s) = res.msg.script else {
                bail!("core returned no script");
            };
            Ok(script_json(&s))
        }
        ScriptCmd::List { space } => {
            let res = client
                .list_scripts(authed(
                    endpoint,
                    ListScriptsRequest {
                        space_id: space,
                        include_deleted: false,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .scripts
                .iter()
                .map(script_json)
                .collect::<Vec<_>>()))
        }
        ScriptCmd::Check { file } => {
            let source = std::fs::read_to_string(&file)?;
            let res = client
                .check_script(authed(endpoint, CheckScriptRequest { source }))
                .await?;
            Ok(json!({ "ok": res.msg.diagnostics.is_empty(), "diagnostics": res.msg.diagnostics }))
        }
        ScriptCmd::Run { space, id, args } => {
            let res = client
                .run_script(authed(
                    endpoint,
                    RunScriptRequest {
                        space_id: space,
                        id,
                        args_json: args,
                    },
                ))
                .await?;
            let Some(r) = res.msg.result else {
                bail!("core returned no result");
            };
            let output: serde_json::Value = if r.output_json.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(&r.output_json).unwrap_or(json!(r.output_json))
            };
            Ok(json!({
                "output": output,
                "logs": r.logs,
                "duration_ms": r.duration_ms,
                "error": r.error,
                "labels": r.labels,
                "grant_id": r.grant_id,
            }))
        }
    }
}

fn constraint_json(c: &Constraint) -> serde_json::Value {
    json!({
        "id": c.id, "space_id": c.space_id, "name": c.name, "field_id": c.field_id,
        "unique": c.unique, "strict": c.strict,
        "created_at_ms": c.created_at_ms, "updated_at_ms": c.updated_at_ms,
        "deleted_at_ms": c.deleted_at_ms,
    })
}

async fn run_constraint(
    endpoint: &Endpoint,
    conn: Client,
    cmd: ConstraintCmd,
) -> Result<serde_json::Value> {
    let client = DefinitionServiceClient::new(conn);
    match cmd {
        ConstraintCmd::List {
            space,
            include_deleted,
        } => {
            let res = client
                .list_constraints(authed(
                    endpoint,
                    ListConstraintsRequest {
                        space_id: space,
                        include_deleted,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .constraints
                .iter()
                .map(constraint_json)
                .collect::<Vec<_>>()))
        }
        ConstraintCmd::Add {
            space,
            field,
            name,
            strict,
        } => {
            let res = client
                .create_constraint(authed(
                    endpoint,
                    CreateConstraintRequest {
                        space_id: space,
                        name,
                        field_id: field,
                        unique: true,
                        strict,
                    },
                ))
                .await?;
            let Some(c) = res.msg.constraint else {
                bail!("core returned no constraint");
            };
            Ok(constraint_json(&c))
        }
        ConstraintCmd::Set {
            space,
            id,
            name,
            strict,
        } => {
            let res = client
                .update_constraint(authed(
                    endpoint,
                    UpdateConstraintRequest {
                        space_id: space,
                        id,
                        name,
                        strict,
                    },
                ))
                .await?;
            let Some(c) = res.msg.constraint else {
                bail!("core returned no constraint");
            };
            Ok(constraint_json(&c))
        }
        ConstraintCmd::Violations { space } => {
            let res = client
                .list_violations(authed(endpoint, ListViolationsRequest { space_id: space }))
                .await?;
            Ok(json!(res
                .msg
                .violations
                .iter()
                .map(|v| json!({
                    "constraint_id": v.constraint_id, "field_id": v.field_id,
                    "value": v.value, "object_ids": v.object_ids,
                }))
                .collect::<Vec<_>>()))
        }
    }
}

async fn run_grant(endpoint: &Endpoint, conn: Client, cmd: GrantCmd) -> Result<serde_json::Value> {
    let client = GrantServiceClient::new(conn);
    match cmd {
        GrantCmd::List { include_revoked } => {
            let res = client
                .list_grants(authed(
                    endpoint,
                    ListGrantsRequest {
                        space_id: String::new(),
                        include_revoked,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .grants
                .iter()
                .map(|g| json!({
                    "id": g.id, "space_id": g.space_id, "script_id": g.script_id,
                    "version": g.version, "benign": g.benign,
                    "granted_at_ms": g.granted_at_ms, "revoked_at_ms": g.revoked_at_ms,
                }))
                .collect::<Vec<_>>()))
        }
        GrantCmd::Add { space, script } => {
            let res = client
                .grant_script(authed(
                    endpoint,
                    GrantScriptRequest {
                        space_id: space,
                        script_id: script,
                    },
                ))
                .await?;
            let Some(g) = res.msg.grant else {
                bail!("core returned no grant");
            };
            Ok(json!({ "id": g.id, "version": g.version, "benign": g.benign }))
        }
        GrantCmd::Revoke { id } => {
            client
                .revoke_grant(authed(endpoint, RevokeGrantRequest { id: id.clone() }))
                .await?;
            Ok(json!({ "revoked": id }))
        }
        GrantCmd::Policy { auto_benign } => {
            let value = match auto_benign {
                Some(v) => {
                    client
                        .set_policy(authed(
                            endpoint,
                            SetPolicyRequest {
                                auto_grant_benign: v,
                            },
                        ))
                        .await?
                        .msg
                        .auto_grant_benign
                }
                None => {
                    client
                        .get_policy(authed(endpoint, GetPolicyRequest {}))
                        .await?
                        .msg
                        .auto_grant_benign
                }
            };
            Ok(json!({ "auto_grant_benign": value }))
        }
    }
}

async fn run_rule(endpoint: &Endpoint, conn: Client, cmd: RuleCmd) -> Result<serde_json::Value> {
    let client = RuleServiceClient::new(conn);
    match cmd {
        RuleCmd::Runs { space, id } => {
            let res = client
                .list_rule_runs(authed(
                    endpoint,
                    ListRuleRunsRequest {
                        space_id: space,
                        id,
                        limit: 0,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .runs
                .iter()
                .map(|r| json!({
                    "started_at_ms": r.started_at_ms, "duration_ms": r.duration_ms,
                    "cause": r.cause, "error": r.error,
                }))
                .collect::<Vec<_>>()))
        }
        RuleCmd::Create {
            space,
            name,
            script,
            kinds,
            type_id,
            every,
            on_events,
        } => {
            let res = client
                .create_rule(authed(
                    endpoint,
                    CreateRuleRequest {
                        space_id: space,
                        name,
                        script_id: script,
                        trigger: Some(Trigger {
                            kinds: kinds.into_iter().map(i32::from).collect(),
                            type_id: type_id.unwrap_or_default(),
                            every_seconds: every,
                            on_events,
                        }),
                        executor_device_id: String::new(),
                    },
                ))
                .await?;
            let Some(r) = res.msg.rule else {
                bail!("core returned no rule");
            };
            Ok(rule_json(&r))
        }
        RuleCmd::List { space } => {
            let res = client
                .list_rules(authed(
                    endpoint,
                    ListRulesRequest {
                        space_id: space,
                        include_deleted: false,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .rules
                .iter()
                .map(rule_json)
                .collect::<Vec<_>>()))
        }
        RuleCmd::Enable { space, id, off } => {
            let res = client
                .update_rule(authed(
                    endpoint,
                    UpdateRuleRequest {
                        space_id: space,
                        id,
                        enabled: Some(!off),
                        ..Default::default()
                    },
                ))
                .await?;
            let Some(r) = res.msg.rule else {
                bail!("core returned no rule");
            };
            Ok(rule_json(&r))
        }
    }
}

fn extension_json(e: &Extension) -> serde_json::Value {
    json!({
        "name": e.name,
        "kind": ExtensionKind::try_from(e.kind).map(|k| k.as_str_name().to_lowercase()).unwrap_or_default(),
        "version": e.version,
        "functions": e.functions,
        "benign": e.benign,
        "granted": e.granted,
        "manifest": e.manifest.as_ref().map(|m| json!({
            "reads": m.reads, "writes": m.writes, "http": m.http, "ai": m.ai, "extensions": m.extensions,
        })),
        "installed_at_ms": e.installed_at_ms,
    })
}

async fn install_extension(
    client: &ExtensionServiceClient,
    endpoint: &Endpoint,
    header: InstallHeader,
    file: &Path,
) -> Result<serde_json::Value> {
    let bytes = std::fs::read(file)?;
    let stream = client
        .install_extension(
            ws_proto::CallOptions::new()
                .with_header("authorization", format!("Bearer {}", endpoint.token)),
        )
        .await?;
    stream
        .send(&InstallExtensionRequest {
            request: Some(install_extension_request::Request::Header(header)),
        })
        .await?;
    for chunk in bytes.chunks(64 * 1024) {
        stream
            .send(&InstallExtensionRequest {
                request: Some(install_extension_request::Request::Chunk(
                    chunk.to_vec().into(),
                )),
            })
            .await?;
    }
    let res = stream.close_and_recv().await?;
    let Some(e) = res.msg.extension else {
        bail!("core returned no extension");
    };
    Ok(extension_json(&e))
}

async fn call_extension(
    client: &ExtensionServiceClient,
    endpoint: &Endpoint,
    space: String,
    name: String,
    function: String,
    args: String,
) -> Result<serde_json::Value> {
    let res = client
        .call_extension(authed(
            endpoint,
            CallExtensionRequest {
                name,
                function,
                space_id: space,
                args_json: args,
            },
        ))
        .await?;
    let Some(r) = res.msg.result else {
        bail!("core returned no result");
    };
    let output: serde_json::Value = if r.output_json.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&r.output_json).unwrap_or(json!(r.output_json))
    };
    Ok(json!({
        "output": output, "logs": r.logs, "duration_ms": r.duration_ms,
        "error": r.error, "labels": r.labels, "grant_id": r.grant_id,
    }))
}

async fn run_extension(
    endpoint: &Endpoint,
    conn: Client,
    cmd: ExtensionCmd,
) -> Result<serde_json::Value> {
    let client = ExtensionServiceClient::new(conn);
    match cmd {
        ExtensionCmd::Install {
            name,
            file,
            functions,
            native,
            reads,
            writes,
        } => {
            install_extension(
                &client,
                endpoint,
                InstallHeader {
                    name,
                    kind: if native {
                        ExtensionKind::Native
                    } else {
                        ExtensionKind::Wasm
                    }
                    .into(),
                    manifest: Some(Manifest {
                        reads,
                        writes,
                        ..Default::default()
                    }),
                    functions,
                },
                &file,
            )
            .await
        }
        ExtensionCmd::List => {
            let res = client
                .list_extensions(authed(endpoint, ListExtensionsRequest {}))
                .await?;
            Ok(json!(res
                .msg
                .extensions
                .iter()
                .map(extension_json)
                .collect::<Vec<_>>()))
        }
        ExtensionCmd::Remove { name } => {
            client
                .remove_extension(authed(
                    endpoint,
                    RemoveExtensionRequest { name: name.clone() },
                ))
                .await?;
            Ok(json!({ "removed": name }))
        }
        ExtensionCmd::Grant {
            name,
            accept_native_risk,
        } => {
            let res = client
                .grant_extension(authed(
                    endpoint,
                    GrantExtensionRequest {
                        name,
                        accept_native_risk,
                    },
                ))
                .await?;
            let Some(g) = res.msg.grant else {
                bail!("core returned no grant");
            };
            Ok(json!({ "id": g.id, "version": g.version, "benign": g.benign }))
        }
        ExtensionCmd::Call {
            space,
            name,
            function,
            args,
        } => call_extension(&client, endpoint, space, name, function, args).await,
    }
}

async fn run_transfer(
    endpoint: &Endpoint,
    conn: Client,
    cmd: TransferCmd,
) -> Result<serde_json::Value> {
    let client = TransferServiceClient::new(conn);
    match cmd {
        TransferCmd::Export {
            space,
            out,
            deps,
            ids,
        } => {
            let res = client
                .export_objects(authed(
                    endpoint,
                    ExportObjectsRequest {
                        space_id: space,
                        ids,
                        with_dependencies: deps,
                    },
                ))
                .await?
                .msg;
            std::fs::write(&out, &res.bundle)?;
            Ok(json!({ "written": out.display().to_string(), "objects": res.count }))
        }
        TransferCmd::Import {
            space,
            file,
            keep_ids,
        } => {
            let bundle = std::fs::read(&file)?;
            let res = client
                .import_objects(authed(
                    endpoint,
                    ImportObjectsRequest {
                        space_id: space,
                        bundle: bundle.into(),
                        keep_ids,
                    },
                ))
                .await?
                .msg;
            Ok(json!({
                "ids": res.ids,
                "objects": res.objects.iter().map(object_json).collect::<Vec<_>>(),
            }))
        }
        TransferCmd::Move { from, to, ids } => {
            let res = client
                .move_objects(authed(
                    endpoint,
                    MoveObjectsRequest {
                        source_space_id: from,
                        dest_space_id: to,
                        ids,
                    },
                ))
                .await?
                .msg;
            Ok(json!(res
                .objects
                .iter()
                .map(object_json)
                .collect::<Vec<_>>()))
        }
    }
}

fn interface_json(i: &Interface) -> serde_json::Value {
    json!({
        "id": i.id,
        "space_id": i.space_id,
        "name": i.name,
        "version": i.version,
        "draft_id": i.draft_id,
        "state": serde_json::from_str::<serde_json::Value>(&i.state_json).unwrap_or(json!({})),
        "nodes": i.nodes.iter().map(|n| json!({
            "id": n.id, "parent_id": n.parent_id, "index": n.index, "type": n.r#type,
            "props": serde_json::from_str::<serde_json::Value>(&n.props_json).unwrap_or(json!({})),
            "bindings": serde_json::from_str::<serde_json::Value>(&n.bindings_json).unwrap_or(json!({})),
            "handlers": serde_json::from_str::<serde_json::Value>(&n.handlers_json).unwrap_or(json!({})),
        })).collect::<Vec<_>>(),
    })
}

async fn run_ui(endpoint: &Endpoint, conn: Client, cmd: UiCmd) -> Result<serde_json::Value> {
    let client = InterfaceServiceClient::new(conn);
    let iface = |o: Option<Interface>| -> Result<serde_json::Value> {
        o.map(|i| interface_json(&i))
            .ok_or_else(|| anyhow::anyhow!("core returned no interface"))
    };
    match cmd {
        UiCmd::Create { space, name, state } => {
            let res = client
                .create_interface(authed(
                    endpoint,
                    CreateInterfaceRequest {
                        space_id: space,
                        name,
                        state_json: state,
                    },
                ))
                .await?;
            iface(res.msg.interface)
        }
        UiCmd::List { space, versions_of } => {
            let res = client
                .list_interfaces(authed(
                    endpoint,
                    ListInterfacesRequest {
                        space_id: space,
                        versions_of: versions_of.unwrap_or_default(),
                        include_deleted: false,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .interfaces
                .iter()
                .map(interface_json)
                .collect::<Vec<_>>()))
        }
        cmd @ (UiCmd::Node { .. } | UiCmd::Remove { .. }) => {
            run_ui_nodes(&client, endpoint, cmd).await
        }
        UiCmd::Publish { space, id } => {
            let res = client
                .publish_interface(authed(
                    endpoint,
                    PublishInterfaceRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            iface(res.msg.version)
        }
        UiCmd::Select { space, id, version } => {
            let res = client
                .select_version(authed(
                    endpoint,
                    SelectVersionRequest {
                        space_id: space,
                        draft_id: id,
                        version,
                    },
                ))
                .await?;
            Ok(json!({ "selected": res.msg.version }))
        }
        UiCmd::Eval { space, id, state } => {
            let res = client
                .evaluate(authed(
                    endpoint,
                    EvaluateRequest {
                        space_id: space,
                        id,
                        state_json: state,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .nodes
                .iter()
                .map(|n| json!({
                    "node_id": n.node_id,
                    "values": serde_json::from_str::<serde_json::Value>(&n.values_json).unwrap_or(json!({})),
                    "errors": n.errors,
                }))
                .collect::<Vec<_>>()))
        }
    }
}

async fn run_ui_nodes(
    client: &InterfaceServiceClient,
    endpoint: &Endpoint,
    cmd: UiCmd,
) -> Result<serde_json::Value> {
    let iface = |o: Option<Interface>| -> Result<serde_json::Value> {
        o.map(|i| interface_json(&i))
            .ok_or_else(|| anyhow::anyhow!("core returned no interface"))
    };
    match cmd {
        UiCmd::Node {
            space,
            id,
            node_type,
            node_id,
            parent,
            index,
            props,
            bind,
            on,
        } => {
            let res = client
                .update_interface(authed(
                    endpoint,
                    UpdateInterfaceRequest {
                        space_id: space,
                        id,
                        set_nodes: vec![Node {
                            id: node_id.unwrap_or_default(),
                            parent_id: parent,
                            index,
                            r#type: node_type,
                            props_json: props,
                            bindings_json: bind,
                            handlers_json: on,
                        }],
                        ..Default::default()
                    },
                ))
                .await?;
            iface(res.msg.interface)
        }
        UiCmd::Remove { space, id, nodes } => {
            let res = client
                .update_interface(authed(
                    endpoint,
                    UpdateInterfaceRequest {
                        space_id: space,
                        id,
                        remove_nodes: nodes,
                        ..Default::default()
                    },
                ))
                .await?;
            iface(res.msg.interface)
        }
        _ => bail!("not a node command"),
    }
}

fn attachment_json(a: &Attachment) -> serde_json::Value {
    json!({
        "id": a.id,
        "space_id": a.space_id,
        "name": a.name,
        "media_type": a.media_type,
        "size": a.size,
        "sha256": a.sha256,
        "available": a.available,
        "pinned": a.pinned,
        "created_at_ms": a.created_at_ms,
        "deleted_at_ms": a.deleted_at_ms,
    })
}

async fn upload_attachment(
    client: &AttachmentServiceClient,
    endpoint: &Endpoint,
    space: String,
    file: PathBuf,
    name: Option<String>,
    media_type: String,
) -> Result<serde_json::Value> {
    use std::io::Read as _;
    let name = name.unwrap_or_else(|| {
        file.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    let size = i64::try_from(std::fs::metadata(&file)?.len())?;
    let stream = client
        .upload(
            ws_proto::CallOptions::new()
                .with_header("authorization", format!("Bearer {}", endpoint.token)),
        )
        .await?;
    stream
        .send(&UploadRequest {
            request: Some(upload_request::Request::Header(UploadHeader {
                space_id: space,
                name,
                media_type,
                size,
            })),
        })
        .await?;
    let mut f = std::fs::File::open(&file)?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        stream
            .send(&UploadRequest {
                request: Some(upload_request::Request::Chunk(buf[..n].to_vec().into())),
            })
            .await?;
    }
    let res = stream.close_and_recv().await?;
    let Some(a) = res.msg.attachment else {
        bail!("core returned no attachment");
    };
    Ok(attachment_json(&a))
}

async fn run_attachment(
    endpoint: &Endpoint,
    conn: Client,
    cmd: AttachmentCmd,
) -> Result<serde_json::Value> {
    let client = AttachmentServiceClient::new(conn);
    match cmd {
        AttachmentCmd::Upload {
            space,
            file,
            name,
            media_type,
        } => upload_attachment(&client, endpoint, space, file, name, media_type).await,
        AttachmentCmd::Download { space, id, out } => {
            use std::io::Write as _;
            let stream = client
                .download(authed(
                    endpoint,
                    DownloadRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            let mut f = std::fs::File::create(&out)?;
            let mut total = 0u64;
            while let Some(chunk) = stream.recv().await? {
                f.write_all(&chunk.data)?;
                total += chunk.data.len() as u64;
            }
            Ok(json!({ "written": out.display().to_string(), "bytes": total }))
        }
        AttachmentCmd::List {
            space,
            include_deleted,
        } => {
            let res = client
                .list_attachments(authed(
                    endpoint,
                    ListAttachmentsRequest {
                        space_id: space,
                        include_deleted,
                    },
                ))
                .await?;
            Ok(json!(res
                .msg
                .attachments
                .iter()
                .map(attachment_json)
                .collect::<Vec<_>>()))
        }
        AttachmentCmd::Pin { space, id, off } => {
            let res = client
                .pin(authed(
                    endpoint,
                    PinRequest {
                        space_id: space,
                        id,
                        pinned: !off,
                    },
                ))
                .await?;
            let Some(a) = res.msg.attachment else {
                bail!("core returned no attachment");
            };
            Ok(attachment_json(&a))
        }
    }
}

async fn run_backup_offline(data_dir: &Path, cmd: &BackupCmd) -> Result<()> {
    match cmd {
        BackupCmd::Check { dir } => {
            let manifest = elri_core::backup::check(Path::new(dir))?;
            print(&json!({
                "verified": manifest.verified,
                "created_at_ms": manifest.created_at_ms,
                "device_id": manifest.device_id,
                "spaces": manifest.spaces.iter().map(|s| json!({
                    "id": s.id, "name": s.name, "cursor": s.cursor,
                    "pending": s.pending, "rejected": s.rejected,
                })).collect::<Vec<_>>(),
                "files": manifest.files.len(),
                "omissions": manifest.omissions,
            }))
        }
        BackupCmd::Restore {
            dir,
            replace,
            keep_identity,
        } => {
            let occupied = if *replace {
                elri_core::backup::Occupied::MoveAside
            } else {
                elri_core::backup::Occupied::Refuse
            };
            let manifest =
                elri_core::backup::restore(Path::new(dir), data_dir, occupied, *keep_identity)
                    .await?;
            print(&json!({
                "restored_into": data_dir.display().to_string(),
                "spaces": manifest.spaces.len(),
                "omissions": manifest.omissions,
            }))
        }
        BackupCmd::Create { .. } => bail!("backup create needs a running core"),
    }
}

fn print(v: &serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let data_dir = match args.data_dir {
        Some(d) => d,
        None => default_data_dir()?,
    };
    // Restore and check work on files, with no core involved.
    if let Cmd::Backup { cmd } = &args.cmd {
        if !matches!(cmd, BackupCmd::Create { .. }) {
            return run_backup_offline(&data_dir, cmd).await;
        }
    }
    let endpoint = Endpoint::read(&data_dir)
        .with_context(|| format!("no running core found in {}", data_dir.display()))?;
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .with_context(|| format!("dial {}", endpoint.url))?;

    let out = match args.cmd {
        Cmd::Info => run_info(&endpoint, conn).await?,
        Cmd::Watch { spaces } => return run_watch(&endpoint, conn, spaces).await,
        Cmd::Space { cmd } => run_space(&endpoint, conn, cmd).await?,
        Cmd::Object { cmd } => run_object(&endpoint, conn, cmd).await?,
        Cmd::Search {
            space,
            query,
            kind,
            limit,
        } => {
            let res = SearchServiceClient::new(conn)
                .search(authed(
                    &endpoint,
                    SearchRequest {
                        space_id: space,
                        query,
                        kind: kind.unwrap_or(ObjectKind::Unspecified).into(),
                        limit,
                    },
                ))
                .await?;
            let hits: Vec<_> = res
                .msg
                .hits
                .iter()
                .map(|h| {
                    json!({
                        "score": h.score,
                        "fields": h.fields,
                        "object": h.object.as_ref().map(object_json),
                    })
                })
                .collect();
            json!(hits)
        }
        Cmd::Change { cmd } => run_change(&endpoint, conn, cmd).await?,
        Cmd::Attachment { cmd } => run_attachment(&endpoint, conn, cmd).await?,
        Cmd::Script { cmd } => run_script(&endpoint, conn, cmd).await?,
        Cmd::Grant { cmd } => run_grant(&endpoint, conn, cmd).await?,
        Cmd::Constraint { cmd } => run_constraint(&endpoint, conn, cmd).await?,
        Cmd::Rule { cmd } => run_rule(&endpoint, conn, cmd).await?,
        Cmd::Ui { cmd } => run_ui(&endpoint, conn, cmd).await?,
        Cmd::Transfer { cmd } => run_transfer(&endpoint, conn, cmd).await?,
        Cmd::Extension { cmd } => run_extension(&endpoint, conn, cmd).await?,
        Cmd::Backup {
            cmd: BackupCmd::Create { dest },
        } => {
            let res = BackupServiceClient::new(conn)
                .create_backup(authed(&endpoint, CreateBackupRequest { dest_dir: dest }))
                .await?
                .msg;
            json!({
                "dir": res.dir,
                "created_at_ms": res.created_at_ms,
                "verified": res.verified,
                "bytes": res.bytes,
                "spaces": res.spaces.iter().map(|s| json!({
                    "id": s.id, "name": s.name, "cursor": s.cursor,
                    "pending": s.pending, "rejected": s.rejected,
                })).collect::<Vec<_>>(),
                "omissions": res.omissions,
            })
        }
        Cmd::Backup { .. } => unreachable!("handled before dialing"),
        Cmd::Remote { cmd } => run_remote(&endpoint, conn, cmd).await?,
    };
    print(&out)
}

async fn run_watch(endpoint: &Endpoint, conn: Client, spaces: Vec<String>) -> Result<()> {
    let stream = EventServiceClient::new(conn)
        .watch(authed(endpoint, WatchRequest { space_ids: spaces }))
        .await?;
    while let Some(ev) = stream.recv().await? {
        let body = match ev.event {
            Some(event::Event::Object(o)) => json!({
                "object": { "space_id": o.space_id, "object_id": o.object_id,
                            "kind": o.kind().as_str_name(), "removed": o.removed }
            }),
            Some(event::Event::Change(c)) => json!({
                "change": { "space_id": c.space_id, "change_id": c.change_id,
                            "status": c.status().as_str_name() }
            }),
            Some(event::Event::Space(s)) => json!({ "space": { "space_id": s.space_id } }),
            Some(event::Event::Remote(_)) => json!({ "remote": {} }),
            Some(event::Event::Notice(n)) => json!({
                "notice": { "space_id": n.space_id, "source": n.source, "text": n.text }
            }),
            Some(event::Event::Lagged(_)) => json!({ "lagged": {} }),
            None => continue,
        };
        let mut line = body;
        line["at_ms"] = json!(ev.at_ms);
        println!("{line}");
    }
    Ok(())
}

async fn run_info(endpoint: &Endpoint, conn: Client) -> Result<serde_json::Value> {
    let res = CoreServiceClient::new(conn)
        .get_info(authed(endpoint, GetInfoRequest {}))
        .await?;
    Ok(json!({
        "version": res.msg.version,
        "commit": res.msg.commit,
        "kind": res.msg.kind().as_str_name(),
        "device_id": res.msg.device_id,
    }))
}

async fn run_space(endpoint: &Endpoint, conn: Client, cmd: SpaceCmd) -> Result<serde_json::Value> {
    let client = SpaceServiceClient::new(conn);
    match cmd {
        SpaceCmd::List => {
            let res = client
                .list_spaces(authed(endpoint, ListSpacesRequest {}))
                .await?;
            let spaces: Vec<_> = res.msg.spaces.iter().map(space_json).collect();
            Ok(json!(spaces))
        }
        SpaceCmd::Create { name } => {
            let res = client
                .create_space(authed(endpoint, CreateSpaceRequest { name }))
                .await?;
            let Some(s) = res.msg.space else {
                bail!("core returned no space");
            };
            Ok(json!({ "id": s.id, "name": s.name }))
        }
    }
}

async fn run_object(
    endpoint: &Endpoint,
    conn: Client,
    cmd: ObjectCmd,
) -> Result<serde_json::Value> {
    let client = ObjectServiceClient::new(conn);
    match cmd {
        ObjectCmd::Create {
            space,
            kind,
            types,
            fields,
            participants,
            target,
        } => {
            let fields: HashMap<String, Value> = fields.into_iter().collect();
            let res = client
                .create_object(authed(
                    endpoint,
                    CreateObjectRequest {
                        space_id: space,
                        kind: kind.into(),
                        types,
                        fields,
                        participants,
                        target_id: target.unwrap_or_default(),
                    },
                ))
                .await?;
            let Some(o) = res.msg.object else {
                bail!("core returned no object");
            };
            Ok(object_json(&o))
        }
        ObjectCmd::Get { space, id } => {
            let res = client
                .get_object(authed(
                    endpoint,
                    GetObjectRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            let Some(o) = res.msg.object else {
                bail!("core returned no object");
            };
            Ok(object_json(&o))
        }
        ObjectCmd::List {
            space,
            kind,
            type_id,
            include_deleted,
            participant_of,
            layers_of,
            places,
            limit,
        } => {
            let res = client
                .list_objects(authed(
                    endpoint,
                    ListObjectsRequest {
                        space_id: space,
                        kind: kind.unwrap_or(ObjectKind::Unspecified).into(),
                        type_id: type_id.unwrap_or_default(),
                        include_deleted,
                        limit,
                        participant_object_id: participant_of.unwrap_or_default(),
                        target_id: layers_of.unwrap_or_default(),
                        placed_block_id: places.unwrap_or_default(),
                    },
                ))
                .await?;
            let objects: Vec<_> = res.msg.objects.iter().map(object_json).collect();
            Ok(json!(objects))
        }
        cmd @ (ObjectCmd::Delete { .. } | ObjectCmd::Purge { .. }) => {
            run_object_lifecycle(&client, endpoint, cmd).await
        }
        cmd @ (ObjectCmd::Place { .. } | ObjectCmd::Move { .. } | ObjectCmd::Unplace { .. }) => {
            run_placement(&client, endpoint, cmd).await
        }
    }
}

async fn run_object_lifecycle(
    client: &ObjectServiceClient,
    endpoint: &Endpoint,
    cmd: ObjectCmd,
) -> Result<serde_json::Value> {
    match cmd {
        ObjectCmd::Delete { space, id } => {
            client
                .delete_object(authed(
                    endpoint,
                    DeleteObjectRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            Ok(json!({ "deleted": true }))
        }
        ObjectCmd::Purge { space, id } => {
            client
                .purge_object(authed(
                    endpoint,
                    PurgeObjectRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            Ok(json!({ "purged": true }))
        }
        _ => bail!("not a lifecycle command"),
    }
}

async fn run_placement(
    client: &ObjectServiceClient,
    endpoint: &Endpoint,
    cmd: ObjectCmd,
) -> Result<serde_json::Value> {
    match cmd {
        ObjectCmd::Place {
            space,
            id,
            block,
            parent,
            index,
        } => {
            let placement = Placement {
                id: String::new(),
                parent_id: parent,
                index,
                block_id: block,
            };
            place(client, endpoint, space, id, vec![placement], vec![]).await
        }
        ObjectCmd::Move {
            space,
            id,
            placement,
            parent,
            index,
        } => {
            let placement = Placement {
                id: placement,
                parent_id: parent,
                index,
                block_id: String::new(),
            };
            place(client, endpoint, space, id, vec![placement], vec![]).await
        }
        ObjectCmd::Unplace {
            space,
            id,
            placement,
        } => place(client, endpoint, space, id, vec![], vec![placement]).await,
        _ => bail!("not a placement command"),
    }
}

async fn place(
    client: &ObjectServiceClient,
    endpoint: &Endpoint,
    space: String,
    id: String,
    set: Vec<Placement>,
    remove: Vec<String>,
) -> Result<serde_json::Value> {
    let res = client
        .update_object(authed(
            endpoint,
            UpdateObjectRequest {
                space_id: space,
                id,
                set_placements: set,
                remove_placements: remove,
                ..Default::default()
            },
        ))
        .await?;
    let Some(o) = res.msg.object else {
        bail!("core returned no object");
    };
    Ok(object_json(&o))
}

fn change_json(c: &Change) -> serde_json::Value {
    json!({
        "id": c.id,
        "device_id": c.device_id,
        "created_at_ms": c.created_at_ms,
        "status": c.status().as_str_name(),
        "accepted_seq": c.accepted_seq,
        "rejection": c.rejection,
        "depends_on": c.depends_on,
        "parts": c.parts.iter().map(|p| json!({
            "object_id": p.object_id,
            "bytes_len": p.bytes_len,
        })).collect::<Vec<_>>(),
    })
}

async fn run_change(
    endpoint: &Endpoint,
    conn: Client,
    cmd: ChangeCmd,
) -> Result<serde_json::Value> {
    let client = ChangeServiceClient::new(conn);
    match cmd {
        ChangeCmd::List {
            space,
            limit,
            object,
        } => {
            let res = client
                .list_changes(authed(
                    endpoint,
                    ListChangesRequest {
                        space_id: space,
                        limit,
                        object_id: object.unwrap_or_default(),
                    },
                ))
                .await?;
            let changes: Vec<_> = res.msg.changes.iter().map(change_json).collect();
            Ok(json!(changes))
        }
        ChangeCmd::Retry { space, id } => {
            let res = client
                .retry_change(authed(
                    endpoint,
                    RetryChangeRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            let Some(c) = res.msg.change else {
                bail!("core returned no change");
            };
            Ok(change_json(&c))
        }
        ChangeCmd::Discard { space, id } => {
            let res = client
                .discard_change(authed(
                    endpoint,
                    DiscardChangeRequest {
                        space_id: space,
                        id,
                    },
                ))
                .await?;
            Ok(json!({ "discarded": res.msg.discarded }))
        }
    }
}

fn read_token() -> Result<String> {
    use std::io::{BufRead as _, IsTerminal as _};
    if let Ok(t) = std::env::var("ELRI_TOKEN") {
        if !t.trim().is_empty() {
            return Ok(t.trim().to_owned());
        }
    }
    if std::io::stdin().is_terminal() {
        eprintln!("paste the server token, then press Enter:");
    }
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    let token = line.trim().to_owned();
    if token.is_empty() {
        bail!("no token given: set ELRI_TOKEN or pass it on standard input");
    }
    Ok(token)
}

fn space_json(s: &elri_proto::elri::v1::space::Space) -> serde_json::Value {
    json!({
        "id": s.id,
        "name": s.name,
        "role": s.role().as_str_name(),
        "authority": s.authority().as_str_name(),
    })
}

async fn run_remote(
    endpoint: &Endpoint,
    conn: Client,
    cmd: RemoteCmd,
) -> Result<serde_json::Value> {
    let client = RemoteServiceClient::new(conn);
    match cmd {
        RemoteCmd::Connect { url } => {
            let token = read_token()?;
            let res = client
                .connect(authed(endpoint, ConnectRequest { url, token }))
                .await?;
            Ok(json!({ "user_id": res.msg.user_id, "user_name": res.msg.user_name }))
        }
        RemoteCmd::Disconnect => {
            client
                .disconnect(authed(endpoint, DisconnectRequest {}))
                .await?;
            Ok(json!({ "disconnected": true }))
        }
        RemoteCmd::Status => {
            let st = client
                .get_status(authed(endpoint, GetStatusRequest {}))
                .await?
                .msg;
            Ok(json!({
                "configured": st.configured,
                "url": st.url,
                "user_name": st.user_name,
                "connected": st.connected,
                "spaces": st.spaces.iter().map(|s| json!({
                    "space_id": s.space_id,
                    "cursor": s.cursor,
                    "pending": s.pending,
                    "rejected": s.rejected,
                })).collect::<Vec<_>>(),
            }))
        }
        RemoteCmd::Spaces => {
            let res = client
                .list_remote_spaces(authed(endpoint, ListRemoteSpacesRequest {}))
                .await?;
            let spaces: Vec<_> = res
                .msg
                .spaces
                .iter()
                .map(|r| {
                    let mut v = r.space.as_ref().map(space_json).unwrap_or_default();
                    v["joined"] = json!(r.joined);
                    v
                })
                .collect();
            Ok(json!(spaces))
        }
        RemoteCmd::Create { name } => {
            let res = client
                .create_remote_space(authed(endpoint, CreateRemoteSpaceRequest { name }))
                .await?;
            let Some(s) = res.msg.space else {
                bail!("core returned no space");
            };
            Ok(space_json(&s))
        }
        RemoteCmd::Join { space } => {
            let res = client
                .join_space(authed(endpoint, JoinSpaceRequest { space_id: space }))
                .await?;
            let Some(s) = res.msg.space else {
                bail!("core returned no space");
            };
            Ok(space_json(&s))
        }
        RemoteCmd::AddMember { space, user, role } => {
            client
                .add_member(authed(
                    endpoint,
                    AddMemberRequest {
                        space_id: space,
                        user_name: user,
                        role: role.into(),
                    },
                ))
                .await?;
            Ok(json!({ "added": true }))
        }
    }
}
