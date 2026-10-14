//! Starts a core on a temporary data directory and talks to it the way the
//! CLI does: endpoint file, loopback WebSocket, bearer token per call.

#![allow(clippy::expect_used)]

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::core::core_service_ws::CoreServiceClient;
use elri_proto::elri::v1::core::{EndpointKind, GetInfoRequest};
use ws_proto::{Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "deadbeef",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn dial(endpoint: &Endpoint) -> Client {
    Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial loopback")
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

#[tokio::test]
async fn get_info_with_token() {
    let dir = tmp_dir("info");
    let core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint file");
    assert_eq!(endpoint.url, core.endpoint().url);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode =
            |p: &std::path::Path| std::fs::metadata(p).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700, "data dir is owner-only");
        assert_eq!(
            mode(&dir.join("endpoint.json")),
            0o600,
            "endpoint file is owner-only"
        );
    }

    let client = CoreServiceClient::new(dial(&endpoint).await);
    let res = client
        .get_info(authed(&endpoint, GetInfoRequest {}))
        .await
        .expect("get_info");
    assert_eq!(res.msg.version, "test");
    assert_eq!(res.msg.kind(), EndpointKind::LocalCore);
    assert_eq!(res.msg.device_id, core.device_id().as_string());

    core.shutdown();
    assert!(
        Endpoint::read(&dir).is_err(),
        "endpoint file removed on shutdown"
    );
}

#[tokio::test]
async fn rejects_without_token() {
    let dir = tmp_dir("notoken");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint file");

    let client = CoreServiceClient::new(dial(&endpoint).await);
    let err = client
        .get_info(GetInfoRequest {})
        .await
        .expect_err("no token must fail");
    assert_eq!(err.code, Code::Unauthenticated);

    let wrong = Request::new(GetInfoRequest {}).with_header("authorization", "Bearer nope");
    let err = client
        .get_info(wrong)
        .await
        .expect_err("wrong token must fail");
    assert_eq!(err.code, Code::Unauthenticated);
}

#[tokio::test]
async fn device_id_is_stable_across_restarts() {
    let dir = tmp_dir("device");
    let first = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let id = first.device_id();
    first.shutdown();
    let second = Core::start(Config::new(&dir, build()))
        .await
        .expect("restart");
    assert_eq!(second.device_id(), id);
}

#[tokio::test]
async fn second_core_on_the_same_data_dir_is_refused() {
    let dir = tmp_dir("single");
    let first = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let err = Core::start(Config::new(&dir, build()))
        .await
        .err()
        .expect("second start must fail");
    assert!(err.to_string().contains("already running"), "{err}");
    first.shutdown();
    let _again = Core::start(Config::new(&dir, build()))
        .await
        .expect("starts again after shutdown");
}
