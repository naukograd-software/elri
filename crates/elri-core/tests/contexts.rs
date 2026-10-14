//! A Context selects across Spaces without owning anything.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::context::context_service_ws::ContextServiceClient;
use elri_proto::elri::v1::context::{
    CreateContextRequest, HideInContextRequest, ListContextsRequest, ResolveContextRequest,
    Selection, UpdateContextRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{value, CreateObjectRequest, ObjectKind, Value};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-ctx-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

#[tokio::test]
async fn contexts_select_across_spaces_and_hide_without_deleting() {
    let dir = tmp_dir("ctx");
    let _core = Core::start(Config::new(
        &dir,
        BuildInfo {
            version: "test",
            commit: "",
        },
    ))
    .await
    .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let contexts = ContextServiceClient::new(conn);
    let mut ids = Vec::new();
    for name in ["work", "personal"] {
        ids.push(
            spaces
                .create_space(authed(&endpoint, CreateSpaceRequest { name: name.into() }))
                .await
                .expect("space")
                .msg
                .space
                .expect("space")
                .id,
        );
    }
    let (work, personal) = (ids[0].clone(), ids[1].clone());
    let mut made = Vec::new();
    for (space, ty) in [(&work, "task"), (&work, "note"), (&personal, "task")] {
        made.push(
            objects
                .create_object(authed(
                    &endpoint,
                    CreateObjectRequest {
                        space_id: space.clone(),
                        kind: ObjectKind::Entity.into(),
                        types: vec![ty.into()],
                        fields: HashMap::from([(
                            "title".to_owned(),
                            Value {
                                value: Some(value::Value::String(ty.into())),
                            },
                        )]),
                        ..Default::default()
                    },
                ))
                .await
                .expect("object")
                .msg
                .object
                .expect("object"),
        );
    }

    // Tasks across both Spaces, plus a Space this device lacks.
    let ghost = uuid::Uuid::now_v7().to_string();
    let ctx = contexts
        .create_context(authed(
            &endpoint,
            CreateContextRequest {
                space_id: personal.clone(),
                name: "my week".into(),
                selection: Some(Selection {
                    space_ids: vec![work.clone(), personal.clone(), ghost.clone()],
                    type_ids: vec!["task".into()],
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect("context")
        .msg
        .context
        .expect("context");
    let resolved = contexts
        .resolve_context(authed(
            &endpoint,
            ResolveContextRequest {
                space_id: personal.clone(),
                id: ctx.id.clone(),
                limit: 0,
            },
        ))
        .await
        .expect("resolve")
        .msg;
    assert_eq!(resolved.objects.len(), 2);
    assert!(resolved.objects.iter().all(|o| o.types == vec!["task"]));
    assert_eq!(resolved.missing_space_ids, vec![ghost]);

    // Hiding takes it out of the Context only.
    contexts
        .hide_in_context(authed(
            &endpoint,
            HideInContextRequest {
                space_id: personal.clone(),
                id: ctx.id.clone(),
                object_id: made[0].id.clone(),
                hidden: true,
            },
        ))
        .await
        .expect("hide");
    let resolved = contexts
        .resolve_context(authed(
            &endpoint,
            ResolveContextRequest {
                space_id: personal.clone(),
                id: ctx.id.clone(),
                limit: 0,
            },
        ))
        .await
        .expect("resolve")
        .msg;
    assert_eq!(resolved.objects.len(), 1);
    assert_eq!(resolved.objects[0].id, made[2].id);
    let still = objects
        .get_object(authed(
            &endpoint,
            elri_proto::elri::v1::object::GetObjectRequest {
                space_id: work.clone(),
                id: made[0].id.clone(),
            },
        ))
        .await
        .expect("get")
        .msg
        .object
        .expect("object");
    assert!(still.deleted_at_ms.is_none());

    // Widening the selection to every kind in one Space.
    let updated = contexts
        .update_context(authed(
            &endpoint,
            UpdateContextRequest {
                space_id: personal.clone(),
                id: ctx.id.clone(),
                name: None,
                selection: Some(Selection {
                    space_ids: vec![work.clone()],
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect("update")
        .msg
        .context
        .expect("context");
    assert!(updated.selection.expect("selection").hidden_ids.is_empty());
    let resolved = contexts
        .resolve_context(authed(
            &endpoint,
            ResolveContextRequest {
                space_id: personal.clone(),
                id: ctx.id.clone(),
                limit: 0,
            },
        ))
        .await
        .expect("resolve")
        .msg;
    assert_eq!(resolved.objects.len(), 2, "task and note of work");
    let listed = contexts
        .list_contexts(authed(
            &endpoint,
            ListContextsRequest {
                space_id: personal.clone(),
                include_deleted: false,
            },
        ))
        .await
        .expect("list")
        .msg
        .contexts;
    assert_eq!(listed.len(), 1);
    let bad = contexts
        .create_context(authed(
            &endpoint,
            CreateContextRequest {
                space_id: personal.clone(),
                name: "bad".into(),
                selection: Some(Selection {
                    space_ids: vec!["nope".into()],
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect_err("bad id");
    assert_eq!(bad.code, Code::InvalidArgument);
}
