//! A backup is consistent, verified, and restorable into a fresh data
//! directory with a new identity.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use elri_core::backup::{self, Occupied};
use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::backup::backup_service_ws::BackupServiceClient;
use elri_proto::elri::v1::backup::CreateBackupRequest;
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{CreateObjectRequest, ListObjectsRequest, ObjectKind};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::{CreateSpaceRequest, ListSpacesRequest};
use ws_proto::{Client, ClientConfig, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-backup-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

#[tokio::test]
async fn backup_is_verified_and_restores_with_a_fresh_identity() {
    let dir = tmp_dir("src");
    let core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let backups = BackupServiceClient::new(conn);

    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space");
    let created = objects
        .create_object(authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.id.clone(),
                kind: ObjectKind::Entity.into(),
                ..Default::default()
            },
        ))
        .await
        .expect("object")
        .msg
        .object
        .expect("object");

    let dest = tmp_dir("dest");
    let res = backups
        .create_backup(authed(
            &endpoint,
            CreateBackupRequest {
                dest_dir: dest.to_string_lossy().into_owned(),
            },
        ))
        .await
        .expect("backup")
        .msg;
    assert!(res.verified);
    assert!(res.bytes > 0);
    assert_eq!(res.spaces.len(), 1);
    assert_eq!(res.spaces[0].id, space.id);
    assert!(res.omissions.iter().any(|o| o.starts_with("secrets")));
    let backup_dir = std::path::PathBuf::from(&res.dir);
    let manifest = backup::check(&backup_dir).expect("check");
    assert_eq!(manifest.files.len(), 2);
    assert_eq!(manifest.device_id, core.device_id().as_string());
    assert!(!backup_dir.join("secrets.json").exists());

    // A tampered file fails the check.
    let tampered = tmp_dir("tampered");
    copy_dir(&backup_dir, &tampered);
    std::fs::write(tampered.join("device.db"), b"garbage").expect("tamper");
    assert!(matches!(
        backup::check(&tampered),
        Err(backup::BackupError::Checksum { .. })
    ));

    // Restoring onto a running core is refused; onto a fresh directory it
    // brings the data back under a new identity.
    assert!(matches!(
        backup::restore(&backup_dir, &dir, Occupied::MoveAside, false).await,
        Err(backup::BackupError::InUse(_))
    ));
    let target = tmp_dir("restored");
    backup::restore(&backup_dir, &target, Occupied::Refuse, false)
        .await
        .expect("restore");
    assert!(matches!(
        backup::restore(&backup_dir, &target, Occupied::Refuse, false).await,
        Err(backup::BackupError::Occupied(_))
    ));
    core.shutdown();

    let restored = Core::start(Config::new(&target, build()))
        .await
        .expect("start restored");
    assert_ne!(restored.device_id().as_string(), manifest.device_id);
    let endpoint = Endpoint::read(&target).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let listed = SpaceServiceClient::new(conn.clone())
        .list_spaces(authed(&endpoint, ListSpacesRequest {}))
        .await
        .expect("spaces")
        .msg
        .spaces;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, space.id);
    let objs = ObjectServiceClient::new(conn)
        .list_objects(authed(
            &endpoint,
            ListObjectsRequest {
                space_id: space.id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("objects")
        .msg
        .objects;
    assert_eq!(objs.len(), 1);
    assert_eq!(objs[0].id, created.id);
    restored.shutdown();

    // Keeping the identity is an explicit choice.
    let kept = tmp_dir("kept");
    backup::restore(&backup_dir, &kept, Occupied::Refuse, true)
        .await
        .expect("restore");
    let kept_core = Core::start(Config::new(&kept, build()))
        .await
        .expect("start kept");
    assert_eq!(kept_core.device_id().as_string(), manifest.device_id);
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("entry");
        let dest = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).expect("copy");
        }
    }
}
