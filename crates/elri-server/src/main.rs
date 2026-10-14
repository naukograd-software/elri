//! Authoritative server. Serves the same protobuf contract as the local core;
//! for now only `CoreService.GetInfo` answers.

use std::net::SocketAddr;

use clap::Parser;
use elri_proto::elri::v1::core::core_service_ws::{register_core_service, CoreServiceHandler};
use elri_proto::elri::v1::core::{EndpointKind, GetInfoRequest, GetInfoResponse};
use ws_proto::server::OriginPolicy;
use ws_proto::{Request, Response, Server, ServerConfig, Status};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8710")]
    bind: SocketAddr,
}

struct ServerServices;

impl CoreServiceHandler for ServerServices {
    async fn get_info(
        &self,
        _req: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            commit: option_env!("ELRI_COMMIT").unwrap_or("").to_owned(),
            kind: EndpointKind::Server.into(),
            device_id: String::new(),
        }))
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    let server = Server::new(ServerConfig {
        origin: OriginPolicy::same_origin_only(),
        ..ServerConfig::default()
    });
    register_core_service(&server, ServerServices);

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    tracing::info!(addr = %args.bind, "elri-server listening");
    tokio::select! {
        r = server.serve_listener(listener) => r,
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            Ok(())
        }
    }
}
