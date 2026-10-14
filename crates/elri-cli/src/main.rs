//! `elri`: talks to the core running on this device through the endpoint file.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use elri_core::Endpoint;
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::ListChangesRequest;
use elri_proto::elri::v1::core::core_service_ws::CoreServiceClient;
use elri_proto::elri::v1::core::GetInfoRequest;
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, Object,
    ObjectKind, Value,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
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
    /// Changes recorded in a Space, newest first.
    Change {
        #[command(subcommand)]
        cmd: ChangeCmd,
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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Kind {
    Entity,
    Relation,
    Layer,
}

impl From<Kind> for ObjectKind {
    fn from(k: Kind) -> Self {
        match k {
            Kind::Entity => ObjectKind::Entity,
            Kind::Relation => ObjectKind::Relation,
            Kind::Layer => ObjectKind::Layer,
        }
    }
}

#[derive(Subcommand, Debug)]
enum ObjectCmd {
    /// Create an Object.
    Create {
        /// Space id.
        #[arg(long)]
        space: String,
        /// Kind.
        #[arg(long, value_enum, default_value_t = Kind::Entity)]
        kind: Kind,
        /// Type-definition ids to apply.
        #[arg(long = "type")]
        types: Vec<String>,
        /// Fields as `id=value`; value is parsed as bool, int, float, `ref:<id>`,
        /// `time:<ms>`, or text.
        #[arg(long = "field", value_parser = parse_field)]
        fields: Vec<(String, Value)>,
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
        #[arg(long, value_enum)]
        kind: Option<Kind>,
        #[arg(long = "type")]
        type_id: Option<String>,
        #[arg(long)]
        include_deleted: bool,
        #[arg(long, default_value_t = 50)]
        limit: i32,
    },
    /// Delete an Object (recoverable until purge).
    Delete {
        #[arg(long)]
        space: String,
        id: String,
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
        None => serde_json::Value::Null,
    }
}

fn object_json(o: &Object) -> serde_json::Value {
    let fields: serde_json::Map<String, serde_json::Value> = o
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), value_json(v)))
        .collect();
    json!({
        "id": o.id,
        "space_id": o.space_id,
        "kind": o.kind().as_str_name(),
        "types": o.types,
        "fields": fields,
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
        Cmd::Space { cmd } => run_space(&endpoint, conn, cmd).await?,
        Cmd::Object { cmd } => run_object(&endpoint, conn, cmd).await?,
        Cmd::Change { cmd } => run_change(&endpoint, conn, cmd).await?,
    };
    print(&out)
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
            let spaces: Vec<_> = res
                .msg
                .spaces
                .iter()
                .map(|s| {
                    json!({
                        "id": s.id,
                        "name": s.name,
                        "role": s.role().as_str_name(),
                        "authority": s.authority().as_str_name(),
                    })
                })
                .collect();
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
        } => {
            let fields: HashMap<String, Value> = fields.into_iter().collect();
            let res = client
                .create_object(authed(
                    endpoint,
                    CreateObjectRequest {
                        space_id: space,
                        kind: ObjectKind::from(kind).into(),
                        types,
                        fields,
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
            limit,
        } => {
            let res = client
                .list_objects(authed(
                    endpoint,
                    ListObjectsRequest {
                        space_id: space,
                        kind: kind
                            .map_or(ObjectKind::Unspecified, ObjectKind::from)
                            .into(),
                        type_id: type_id.unwrap_or_default(),
                        include_deleted,
                        limit,
                    },
                ))
                .await?;
            let objects: Vec<_> = res.msg.objects.iter().map(object_json).collect();
            Ok(json!(objects))
        }
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
    }
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
            let changes: Vec<_> = res
                .msg
                .changes
                .iter()
                .map(|c| {
                    json!({
                        "id": c.id,
                        "device_id": c.device_id,
                        "created_at_ms": c.created_at_ms,
                        "status": c.status().as_str_name(),
                        "parts": c.parts.iter().map(|p| json!({
                            "object_id": p.object_id,
                            "bytes_len": p.bytes_len,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            Ok(json!(changes))
        }
    }
}
