//! `elri`: talks to the core running on this device through the endpoint file.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use elri_core::Endpoint;
use elri_proto::elri::v1::core::core_service_ws::CoreServiceClient;
use elri_proto::elri::v1::core::GetInfoRequest;
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::ListSpacesRequest;
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
    /// List Spaces known to the core.
    Spaces,
}

fn default_data_dir() -> Result<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .context("cannot determine the data directory")?;
    Ok(base.join("elri"))
}

async fn connect(endpoint: &Endpoint) -> Result<Client> {
    Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .with_context(|| format!("dial {}", endpoint.url))
}

/// The core checks the launch token on every call, so it rides as a
/// per-request header rather than on the WebSocket upgrade.
fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
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
    let conn = connect(&endpoint).await?;

    match args.cmd {
        Cmd::Info => {
            let res = CoreServiceClient::new(conn)
                .get_info(authed(&endpoint, GetInfoRequest {}))
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "version": res.msg.version,
                    "commit": res.msg.commit,
                    "kind": res.msg.kind().as_str_name(),
                    "device_id": res.msg.device_id,
                }))?
            );
        }
        Cmd::Spaces => {
            let res = SpaceServiceClient::new(conn)
                .list_spaces(authed(&endpoint, ListSpacesRequest {}))
                .await?;
            for s in res.msg.spaces {
                println!("{}\t{}\t{}", s.id, s.name, s.role().as_str_name());
            }
        }
    }
    Ok(())
}
