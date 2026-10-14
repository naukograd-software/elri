//! Attachments on one device: bytes in, bytes out, listing, pinning, and
//! purge releasing the bytes.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    clippy::cast_possible_wrap
)]

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::attachment::attachment_service_ws::AttachmentServiceClient;
use elri_proto::elri::v1::attachment::{
    upload_request, DownloadRequest, ListAttachmentsRequest, PinRequest, UploadHeader,
    UploadRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    CreateObjectRequest, DeleteObjectRequest, ObjectKind, PurgeObjectRequest,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{CallOptions, Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-att-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn opts(endpoint: &Endpoint) -> CallOptions {
    CallOptions::new().with_header("authorization", format!("Bearer {}", endpoint.token))
}

async fn upload(
    client: &AttachmentServiceClient,
    endpoint: &Endpoint,
    space_id: &str,
    name: &str,
    bytes: &[u8],
    announced: i64,
) -> Result<elri_proto::elri::v1::attachment::Attachment, ws_proto::Status> {
    let stream = client.upload(opts(endpoint)).await?;
    stream
        .send(&UploadRequest {
            request: Some(upload_request::Request::Header(UploadHeader {
                space_id: space_id.to_owned(),
                name: name.to_owned(),
                media_type: "text/plain".to_owned(),
                size: announced,
            })),
        })
        .await?;
    for chunk in bytes.chunks(5) {
        stream
            .send(&UploadRequest {
                request: Some(upload_request::Request::Chunk(chunk.to_vec().into())),
            })
            .await?;
    }
    let res = stream.close_and_recv().await?;
    Ok(res.msg.attachment.expect("attachment"))
}

#[tokio::test]
async fn attachments_round_trip_and_purge_releases_bytes() {
    let dir = tmp_dir("local");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let attachments = AttachmentServiceClient::new(conn);
    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space");

    let bytes = b"hello, attachments! these are some bytes.";
    let a = upload(
        &attachments,
        &endpoint,
        &space.id,
        "hello.txt",
        bytes,
        bytes.len() as i64,
    )
    .await
    .expect("upload");
    assert_eq!(a.name, "hello.txt");
    assert_eq!(a.media_type, "text/plain");
    assert_eq!(a.size, bytes.len() as i64);
    assert_eq!(a.sha256, sha256_hex(bytes));
    assert!(a.available);
    assert!(!a.pinned);
    let blob_path = dir.join("blobs").join(&space.id).join(&a.sha256);
    assert!(blob_path.is_file());

    // A wrong announced size is refused and leaves nothing behind.
    let bad = upload(&attachments, &endpoint, &space.id, "bad.txt", bytes, 3)
        .await
        .expect_err("size mismatch");
    assert_eq!(bad.code, Code::InvalidArgument);
    let parts: Vec<_> = std::fs::read_dir(dir.join("blobs").join(&space.id))
        .expect("dir")
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(".part-"))
        .collect();
    assert!(parts.is_empty());

    // Download gives the bytes back.
    let stream = attachments
        .download(authed(
            &endpoint,
            DownloadRequest {
                space_id: space.id.clone(),
                id: a.id.clone(),
            },
        ))
        .await
        .expect("download");
    let mut got = Vec::new();
    while let Some(c) = stream.recv().await.expect("chunk") {
        got.extend_from_slice(&c.data);
    }
    assert_eq!(got, bytes);

    // Listed; pinned; attachments are not created through ObjectService.
    let listed = attachments
        .list_attachments(authed(
            &endpoint,
            ListAttachmentsRequest {
                space_id: space.id.clone(),
                include_deleted: false,
            },
        ))
        .await
        .expect("list")
        .msg
        .attachments;
    assert_eq!(listed.len(), 1);
    let pinned = attachments
        .pin(authed(
            &endpoint,
            PinRequest {
                space_id: space.id.clone(),
                id: a.id.clone(),
                pinned: true,
            },
        ))
        .await
        .expect("pin")
        .msg
        .attachment
        .expect("attachment");
    assert!(pinned.pinned);
    let refused = objects
        .create_object(authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.id.clone(),
                kind: ObjectKind::Attachment.into(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("not through ObjectService");
    assert_eq!(refused.code, Code::InvalidArgument);

    // The same bytes uploaded twice share one file; purging one
    // attachment keeps the bytes, purging the other releases them.
    let b = upload(
        &attachments,
        &endpoint,
        &space.id,
        "again.txt",
        bytes,
        bytes.len() as i64,
    )
    .await
    .expect("upload again");
    assert_eq!(b.sha256, a.sha256);
    for id in [&a.id, &b.id] {
        objects
            .delete_object(authed(
                &endpoint,
                DeleteObjectRequest {
                    space_id: space.id.clone(),
                    id: id.clone(),
                },
            ))
            .await
            .expect("delete");
    }
    objects
        .purge_object(authed(
            &endpoint,
            PurgeObjectRequest {
                space_id: space.id.clone(),
                id: a.id.clone(),
            },
        ))
        .await
        .expect("purge a");
    assert!(blob_path.is_file(), "b still names the bytes");
    objects
        .purge_object(authed(
            &endpoint,
            PurgeObjectRequest {
                space_id: space.id.clone(),
                id: b.id.clone(),
            },
        ))
        .await
        .expect("purge b");
    assert!(!blob_path.exists(), "nothing names the bytes any more");
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    use std::fmt::Write as _;
    sha2::Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}
