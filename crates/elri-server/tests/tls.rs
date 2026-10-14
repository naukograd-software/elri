//! With a certificate and key the server speaks wss; a client that does
//! not trust the certificate is turned away at the handshake, not at the
//! socket.

#![allow(clippy::expect_used)]

use elri_server::{Config, Server, Tls};
use ws_proto::{Client, ClientConfig};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-tls-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_serves_wss_with_a_certificate() {
    let dir = tmp_dir("srv");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("cert");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");

    let server = Server::start(Config {
        data_dir: dir.join("data"),
        bind: "127.0.0.1:0".parse().expect("addr"),
        tls: Some(Tls {
            cert: cert_path,
            key: key_path,
        }),
    })
    .await
    .expect("server");
    assert!(server.url().starts_with("wss://"));

    // The socket answers, but a plain ws:// client gets no WebSocket, and a
    // wss:// client that does not trust the self-signed certificate is
    // refused at the TLS handshake.
    let plain = Client::dial(
        &server.url().replacen("wss", "ws", 1),
        ClientConfig::default(),
    )
    .await;
    assert!(plain.is_err());
    let untrusted = Client::dial(&server.url(), ClientConfig::default()).await;
    let err = untrusted
        .expect_err("untrusted certificate refused")
        .to_string();
    assert!(
        err.to_lowercase().contains("tls") || err.to_lowercase().contains("certificate"),
        "{err}"
    );

    // Missing files are reported, not ignored.
    let bad = Server::start(Config {
        data_dir: dir.join("data2"),
        bind: "127.0.0.1:0".parse().expect("addr"),
        tls: Some(Tls {
            cert: dir.join("missing.pem"),
            key: dir.join("missing.pem"),
        }),
    })
    .await;
    assert!(matches!(bad, Err(elri_server::ServerError::Tls(_))));
    server.shutdown();
}
