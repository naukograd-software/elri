//! `elri`: talks to the core running on this device through the endpoint file.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use elri_core::Endpoint;
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::{
    Change, DiscardChangeRequest, ListChangesRequest, RetryChangeRequest,
};
use elri_proto::elri::v1::core::core_service_ws::CoreServiceClient;
use elri_proto::elri::v1::core::GetInfoRequest;
use elri_proto::elri::v1::events::event_service_ws::EventServiceClient;
use elri_proto::elri::v1::events::{event, WatchRequest};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, Object,
    ObjectKind, Participant, Placement, PurgeObjectRequest, UpdateObjectRequest, Value, ValueList,
};
use elri_proto::elri::v1::remote::remote_service_ws::RemoteServiceClient;
use elri_proto::elri::v1::remote::{
    AddMemberRequest, ConnectRequest, CreateRemoteSpaceRequest, DisconnectRequest,
    GetStatusRequest, JoinSpaceRequest, ListRemoteSpacesRequest,
};
use elri_proto::elri::v1::search::search_service_ws::SearchServiceClient;
use elri_proto::elri::v1::search::SearchRequest;
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::Role;
use elri_proto::elri::v1::space::{CreateSpaceRequest, ListSpacesRequest};
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
enum ChangeCmd {
    /// List Changes.
    List {
        #[arg(long)]
        space: String,
        #[arg(long, default_value_t = 50)]
        limit: i32,
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
        /// `time:<ms>`, `text:<collaborative text>`, `list:a,b,c`, or a string.
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
        Some(value::Value::List(l)) => json!(l.items.iter().map(value_json).collect::<Vec<_>>()),
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
        ChangeCmd::List { space, limit } => {
            let res = client
                .list_changes(authed(
                    endpoint,
                    ListChangesRequest {
                        space_id: space,
                        limit,
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
