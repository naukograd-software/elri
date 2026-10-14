//! ws-proto surface of the core. Every client goes through the same server:
//! a token middleware first, then the typed service handlers.

use std::sync::Arc;

use elri_proto::elri::v1::core::core_service_ws::{register_core_service_arc, CoreServiceHandler};
use elri_proto::elri::v1::core::{EndpointKind, GetInfoRequest, GetInfoResponse};
use elri_proto::elri::v1::space::space_service_ws::{
    register_space_service_arc, SpaceServiceHandler,
};
use elri_proto::elri::v1::space::{ListSpacesRequest, ListSpacesResponse};
use ws_proto::server::{Handler, OriginPolicy};
use ws_proto::{Code, Request, Response, Server, ServerConfig, Status};

use crate::device::{DeviceId, LaunchToken};
use crate::BuildInfo;

/// Everything the handlers need.
#[derive(Clone)]
pub struct Services {
    /// Version of the embedding binary.
    pub build: BuildInfo,
    /// This device.
    pub device_id: DeviceId,
    /// Token every request must carry.
    pub token: LaunchToken,
}

/// Builds the loopback server: same-origin policy, bearer-token middleware,
/// all services registered.
pub fn build_server(services: Services) -> Server {
    let server = Server::new(ServerConfig {
        origin: OriginPolicy::same_origin_only(),
        ..ServerConfig::default()
    });
    let token = services.token.clone();
    server.add_middleware(move |next: Handler| require_token(token.clone(), next));

    let services = Arc::new(services);
    register_core_service_arc(&server, Arc::clone(&services));
    register_space_service_arc(&server, services);
    server
}

fn require_token(token: LaunchToken, next: Handler) -> Handler {
    Arc::new(move |stream| {
        let next = Arc::clone(&next);
        let presented = stream.header().get("authorization").cloned();
        let ok = presented
            .as_deref()
            .is_some_and(|h| token.matches_header(h));
        Box::pin(async move {
            if ok {
                next(stream).await
            } else {
                Err(Status::new(
                    Code::Unauthenticated,
                    "missing or invalid token",
                ))
            }
        })
    })
}

impl CoreServiceHandler for Services {
    async fn get_info(
        &self,
        _req: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            version: self.build.version.to_owned(),
            commit: self.build.commit.to_owned(),
            kind: EndpointKind::LocalCore.into(),
            device_id: self.device_id.as_string(),
        }))
    }
}

impl SpaceServiceHandler for Services {
    async fn list_spaces(
        &self,
        _req: Request<ListSpacesRequest>,
    ) -> Result<Response<ListSpacesResponse>, Status> {
        Err(Status::new(
            Code::Unimplemented,
            "spaces are not implemented yet",
        ))
    }
}
