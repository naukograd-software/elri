//! The event stream: a watcher sees Spaces appear, Objects change, and
//! Changes move, without polling.

#![allow(clippy::expect_used, clippy::panic)]

use std::time::Duration;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::change::ChangeStatus;
use elri_proto::elri::v1::events::event_service_ws::EventServiceClient;
use elri_proto::elri::v1::events::{event, Event, WatchRequest};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{CreateObjectRequest, DeleteObjectRequest, ObjectKind};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-events-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

async fn next(stream: &elri_proto::elri::v1::events::event_service_ws::WatchClientStream) -> Event {
    tokio::time::timeout(Duration::from_secs(5), stream.recv())
        .await
        .expect("event in time")
        .expect("stream ok")
        .expect("stream open")
}

#[tokio::test]
async fn watcher_sees_spaces_objects_and_changes() {
    let dir = tmp_dir("watch");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let events = EventServiceClient::new(conn.clone());
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn);

    let all = events
        .watch(authed(&endpoint, WatchRequest::default()))
        .await
        .expect("watch");

    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space");
    let ev = next(&all).await;
    assert!(ev.at_ms > 0);
    match ev.event {
        Some(event::Event::Space(s)) => assert_eq!(s.space_id, space.id),
        other => panic!("expected a space event, got {other:?}"),
    }

    // A watcher of another Space sees nothing of this one but still gets
    // Space and remote events.
    let other = events
        .watch(authed(
            &endpoint,
            WatchRequest {
                space_ids: vec![uuid::Uuid::now_v7().to_string()],
            },
        ))
        .await
        .expect("watch other");

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
        .expect("create")
        .msg
        .object
        .expect("object");
    let ev = next(&all).await;
    match ev.event {
        Some(event::Event::Change(c)) => {
            assert_eq!(c.space_id, space.id);
            assert_eq!(c.status(), ChangeStatus::Local);
        }
        other => panic!("expected a change event, got {other:?}"),
    }
    let ev = next(&all).await;
    match ev.event {
        Some(event::Event::Object(o)) => {
            assert_eq!(o.object_id, created.id);
            assert_eq!(o.kind(), ObjectKind::Entity);
            assert!(!o.removed);
        }
        other => panic!("expected an object event, got {other:?}"),
    }

    objects
        .delete_object(authed(
            &endpoint,
            DeleteObjectRequest {
                space_id: space.id.clone(),
                id: created.id.clone(),
            },
        ))
        .await
        .expect("delete");
    let _change = next(&all).await;
    let ev = next(&all).await;
    assert!(matches!(ev.event, Some(event::Event::Object(o)) if o.object_id == created.id));

    let quiet = tokio::time::timeout(Duration::from_millis(300), other.recv()).await;
    assert!(quiet.is_err(), "the other watcher saw {quiet:?}");
}
