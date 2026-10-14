//! A device set to a manual proxy reaches its server through it: the link
//! is a CONNECT tunnel, the settings say where requests go, and a bad
//! proxy URL is refused without losing the working one.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use elri_core::{BuildInfo, Config as CoreConfig, Core, Endpoint};
use elri_proto::elri::v1::remote::remote_service_ws::RemoteServiceClient;
use elri_proto::elri::v1::remote::{ConnectRequest, GetStatusRequest};
use elri_proto::elri::v1::settings::settings_service_ws::SettingsServiceClient;
use elri_proto::elri::v1::settings::{
    GetSettingsRequest, ProxyMode, ProxySettings, ResolveProxyRequest, UpdateSettingsRequest,
};
use elri_server::{Config as ServerConfig, Server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ws_proto::{Client, ClientConfig, Code, Request};

struct Authed(String);

impl Authed {
    fn req<T>(&self, msg: T) -> Request<T> {
        Request::new(msg).with_header("authorization", self.0.clone())
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-proxy-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A CONNECT proxy that counts the tunnels it opens.
async fn toy_proxy() -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let opened = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&opened);
    tokio::spawn(async move {
        loop {
            let Ok((mut client, _)) = listener.accept().await else {
                return;
            };
            let count = Arc::clone(&count);
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if client.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    head.push(byte[0]);
                }
                let text = String::from_utf8_lossy(&head).into_owned();
                let Some(target) = text
                    .strip_prefix("CONNECT ")
                    .and_then(|r| r.split_whitespace().next())
                    .map(str::to_owned)
                else {
                    return;
                };
                let Ok(mut upstream) = tokio::net::TcpStream::connect(target).await else {
                    let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                let _ = client
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await;
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    (format!("http://127.0.0.1:{port}"), opened)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_device_reaches_its_server_through_a_manual_proxy() {
    // The server listens on another loopback address: 127.0.0.1 itself is
    // never sent through a proxy.
    let server = Server::start(ServerConfig {
        data_dir: tmp_dir("server"),
        bind: "127.0.0.2:0".parse().expect("addr"),
        tls: None,
    })
    .await
    .expect("server");
    let (_, _, token) = server
        .state()
        .registry
        .create_user("alice", false, "test")
        .await
        .expect("user");
    let (proxy, opened) = toy_proxy().await;

    let dir = tmp_dir("device");
    let _core = Core::start(CoreConfig::new(
        &dir,
        BuildInfo {
            version: "test",
            commit: "",
        },
    ))
    .await
    .expect("core");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial core");
    let authed = Authed(format!("Bearer {}", endpoint.token));
    let settings = SettingsServiceClient::new(conn.clone());
    let remote = RemoteServiceClient::new(conn);

    // Defaults: the system's proxy, semantic search on.
    let current = settings
        .get_settings(authed.req(GetSettingsRequest {}))
        .await
        .expect("get")
        .msg
        .settings
        .expect("settings");
    assert_eq!(
        current.proxy.as_ref().expect("proxy").mode(),
        ProxyMode::System
    );
    assert!(current.semantic.as_ref().expect("semantic").enabled);

    // A manual proxy without a URL, or with garbage, is refused.
    let err = settings
        .update_settings(authed.req(UpdateSettingsRequest {
            proxy: Some(ProxySettings {
                mode: ProxyMode::Manual.into(),
                url: String::new(),
                no_proxy: String::new(),
            }),
            semantic: None,
            ai: None,
        }))
        .await
        .expect_err("no url");
    assert_eq!(err.code, Code::InvalidArgument);
    let err = settings
        .update_settings(authed.req(UpdateSettingsRequest {
            proxy: Some(ProxySettings {
                mode: ProxyMode::Manual.into(),
                url: "::nope::".into(),
                no_proxy: String::new(),
            }),
            semantic: None,
            ai: None,
        }))
        .await
        .expect_err("garbage");
    assert_eq!(err.code, Code::InvalidArgument);

    // A password goes to the credential store and is shown back masked;
    // sending the mask keeps it.
    let with_pw = proxy.replacen("http://", "http://alice:s3cret@", 1);
    let shown = settings
        .update_settings(authed.req(UpdateSettingsRequest {
            proxy: Some(ProxySettings {
                mode: ProxyMode::Manual.into(),
                url: with_pw,
                no_proxy: "intranet.example".into(),
            }),
            semantic: None,
            ai: None,
        }))
        .await
        .expect("manual proxy with password")
        .msg
        .settings
        .expect("settings");
    let shown_url = shown.proxy.expect("proxy").url;
    assert!(shown_url.contains("alice:***@"), "{shown_url}");
    let db = std::fs::read(dir.join("device.db")).expect("device.db");
    assert!(
        !db.windows(6).any(|w| w == b"s3cret"),
        "the proxy password must not be in device.db"
    );
    let secrets = std::fs::read_to_string(dir.join("secrets.json")).expect("secrets");
    assert!(secrets.contains("s3cret"));
    settings
        .update_settings(authed.req(UpdateSettingsRequest {
            proxy: Some(ProxySettings {
                mode: ProxyMode::Manual.into(),
                url: proxy.clone(),
                no_proxy: "intranet.example".into(),
            }),
            semantic: None,
            ai: None,
        }))
        .await
        .expect("manual proxy");
    let secrets = std::fs::read_to_string(dir.join("secrets.json")).expect("secrets");
    assert!(
        !secrets.contains("s3cret"),
        "a URL without a password drops it"
    );
    let resolved = settings
        .resolve_proxy(authed.req(ResolveProxyRequest { url: server.url() }))
        .await
        .expect("resolve")
        .msg;
    assert_eq!(resolved.source, "manual");
    assert_eq!(resolved.proxy.trim_end_matches('/'), proxy);
    let bypass = settings
        .resolve_proxy(authed.req(ResolveProxyRequest {
            url: "https://intranet.example/".into(),
        }))
        .await
        .expect("resolve")
        .msg;
    assert_eq!(bypass.source, "bypass");

    // Connecting goes through the tunnel and replication comes up.
    remote
        .connect(authed.req(ConnectRequest {
            url: server.url(),
            token,
        }))
        .await
        .expect("connect through the proxy");
    assert!(opened.load(Ordering::SeqCst) >= 1, "no tunnel was opened");
    let mut connected = false;
    for _ in 0..50 {
        let st = remote
            .get_status(authed.req(GetStatusRequest {}))
            .await
            .expect("status")
            .msg;
        if st.connected {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(connected, "the link through the proxy is up");
    server.shutdown();
}
