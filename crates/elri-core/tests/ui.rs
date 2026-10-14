//! An interface is data: nodes in a tree, bindings the core evaluates,
//! handlers the core runs, versions a device picks.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, ListObjectsRequest, ObjectKind, Value,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use elri_proto::elri::v1::ui::interface_service_ws::InterfaceServiceClient;
use elri_proto::elri::v1::ui::{
    CreateInterfaceRequest, DispatchRequest, EvaluateRequest, GetInterfaceRequest,
    ListInterfacesRequest, Node, PublishInterfaceRequest, SelectVersionRequest,
    UpdateInterfaceRequest,
};
use ws_proto::{Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-ui-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

fn node(parent: &str, index: i32, ty: &str, props: &str, bindings: &str, handlers: &str) -> Node {
    Node {
        id: String::new(),
        parent_id: parent.into(),
        index,
        r#type: ty.into(),
        props_json: props.into(),
        bindings_json: bindings.into(),
        handlers_json: handlers.into(),
    }
}

#[tokio::test]
async fn interfaces_are_evaluated_and_driven_by_the_core() {
    let dir = tmp_dir("ui");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let ui = InterfaceServiceClient::new(conn);
    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;

    // A task list: a heading bound to the count, a list bound to the
    // tasks, an input bound to local state, a button whose handler creates
    // a task from that state.
    let draft = ui
        .create_interface(authed(
            &endpoint,
            CreateInterfaceRequest {
                space_id: space.clone(),
                name: "tasks".into(),
                state_json: r#"{"draft": ""}"#.into(),
            },
        ))
        .await
        .expect("create")
        .msg
        .interface
        .expect("interface");
    assert_eq!(draft.version, 0);
    let root = ui
        .update_interface(authed(
            &endpoint,
            UpdateInterfaceRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                set_nodes: vec![node("", 0, "column", "{}", "{}", "{}")],
                ..Default::default()
            },
        ))
        .await
        .expect("root")
        .msg
        .interface
        .expect("interface")
        .nodes[0]
        .id
        .clone();
    let iface = ui
        .update_interface(authed(
            &endpoint,
            UpdateInterfaceRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                set_nodes: vec![
                    node(
                        &root,
                        0,
                        "text",
                        r#"{"heading": true}"#,
                        r#"{"text": "\"Tasks: \" .. #elri.list({ type = \"task\" })"}"#,
                        "{}",
                    ),
                    node(
                        &root,
                        1,
                        "list",
                        r#"{"label": "title"}"#,
                        r#"{"items": "elri.list({ type = \"task\" })"}"#,
                        "{}",
                    ),
                    node(&root, 2, "input", r#"{"state": "draft"}"#, "{}", "{}"),
                    node(
                        &root,
                        3,
                        "button",
                        r#"{"label": "Add", "primary": true}"#,
                        r#"{"disabled": "state.draft == \"\""}"#,
                        r#"{"click": "elri.create({ types = {\"task\"}, fields = { title = state.draft } }); state.draft = \"\""}"#,
                    ),
                ],
                ..Default::default()
            },
        ))
        .await
        .expect("nodes")
        .msg
        .interface
        .expect("interface");
    assert_eq!(iface.nodes.len(), 5);
    assert_eq!(iface.nodes[1].parent_id, root);
    let button = iface.nodes[4].id.clone();

    // A node outside the catalog, or with a broken binding, is refused.
    let bad = ui
        .update_interface(authed(
            &endpoint,
            UpdateInterfaceRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                set_nodes: vec![node(&root, 9, "iframe", "{}", "{}", "{}")],
                ..Default::default()
            },
        ))
        .await
        .expect_err("not in catalog");
    assert_eq!(bad.code, Code::InvalidArgument);
    let bad = ui
        .update_interface(authed(
            &endpoint,
            UpdateInterfaceRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                set_nodes: vec![node(&root, 9, "text", "{}", r#"{"text": "1 +"}"#, "{}")],
                ..Default::default()
            },
        ))
        .await
        .expect_err("broken binding");
    assert_eq!(bad.code, Code::InvalidArgument);

    // Evaluate: bindings merged over literal props, state respected.
    let eval = |state: &str| {
        let req = authed(
            &endpoint,
            EvaluateRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                state_json: state.to_owned(),
            },
        );
        let ui = ui.clone();
        async move { ui.evaluate(req).await.expect("evaluate").msg.nodes }
    };
    let values = |nodes: &[elri_proto::elri::v1::ui::NodeValues], id: &str| -> serde_json::Value {
        serde_json::from_str(
            &nodes
                .iter()
                .find(|n| n.node_id == id)
                .expect("node")
                .values_json,
        )
        .expect("json")
    };
    let nodes = eval(r#"{"draft": ""}"#).await;
    assert_eq!(values(&nodes, &iface.nodes[1].id)["text"], "Tasks: 0");
    assert_eq!(values(&nodes, &iface.nodes[1].id)["heading"], true);
    assert_eq!(values(&nodes, &button)["disabled"], true);
    assert_eq!(values(&nodes, &button)["label"], "Add");
    let nodes = eval(r#"{"draft": "milk"}"#).await;
    assert_eq!(values(&nodes, &button)["disabled"], false);

    // Dispatch the click: the handler creates the task and clears the
    // state; the next evaluation sees the task.
    let res = ui
        .dispatch(authed(
            &endpoint,
            DispatchRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                node_id: button.clone(),
                event: "click".into(),
                state_json: r#"{"draft": "milk"}"#.into(),
                payload_json: String::new(),
            },
        ))
        .await
        .expect("dispatch")
        .msg;
    assert_eq!(res.error, "", "{:?}", res.logs);
    let after: serde_json::Value = serde_json::from_str(&res.state_json).expect("state");
    assert_eq!(after["draft"], "");
    let tasks = objects
        .list_objects(authed(
            &endpoint,
            ListObjectsRequest {
                space_id: space.clone(),
                type_id: "task".into(),
                ..Default::default()
            },
        ))
        .await
        .expect("list")
        .msg
        .objects;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].fields["title"], s("milk"));
    let nodes = eval(r#"{"draft": ""}"#).await;
    assert_eq!(values(&nodes, &iface.nodes[1].id)["text"], "Tasks: 1");
    let items = values(&nodes, &iface.nodes[2].id);
    assert_eq!(items["items"].as_array().expect("items").len(), 1);

    // Publish freezes a copy; the device picks which to show; the draft
    // keeps changing underneath.
    let v1 = ui
        .publish_interface(authed(
            &endpoint,
            PublishInterfaceRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
            },
        ))
        .await
        .expect("publish")
        .msg
        .version
        .expect("version");
    assert_eq!(v1.version, 1);
    assert_eq!(v1.draft_id, draft.id);
    assert_eq!(v1.nodes.len(), 5);
    let frozen = ui
        .update_interface(authed(
            &endpoint,
            UpdateInterfaceRequest {
                space_id: space.clone(),
                id: v1.id.clone(),
                name: Some("renamed".into()),
                ..Default::default()
            },
        ))
        .await
        .expect_err("fixed");
    assert_eq!(frozen.code, Code::FailedPrecondition);
    ui.update_interface(authed(
        &endpoint,
        UpdateInterfaceRequest {
            space_id: space.clone(),
            id: draft.id.clone(),
            name: Some("tasks v2".into()),
            ..Default::default()
        },
    ))
    .await
    .expect("edit draft");
    ui.select_version(authed(
        &endpoint,
        SelectVersionRequest {
            space_id: space.clone(),
            draft_id: draft.id.clone(),
            version: 1,
        },
    ))
    .await
    .expect("select");
    let shown = ui
        .get_interface(authed(
            &endpoint,
            GetInterfaceRequest {
                space_id: space.clone(),
                id: draft.id.clone(),
                selected: true,
            },
        ))
        .await
        .expect("get")
        .msg;
    assert_eq!(shown.selected_version, 1);
    assert_eq!(shown.interface.expect("interface").name, "tasks");
    let listed = ui
        .list_interfaces(authed(
            &endpoint,
            ListInterfacesRequest {
                space_id: space.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("list")
        .msg
        .interfaces;
    assert_eq!(listed.len(), 1, "versions are not drafts");
    let versions = ui
        .list_interfaces(authed(
            &endpoint,
            ListInterfacesRequest {
                space_id: space.clone(),
                versions_of: draft.id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("versions")
        .msg
        .interfaces;
    assert_eq!(versions.len(), 1);

    // The interface's own Space is the only one its handlers reach: the
    // interface object is not something ObjectService creates.
    let refused = objects
        .create_object(authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.clone(),
                kind: ObjectKind::Interface.into(),
                fields: HashMap::new(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("managed kind");
    assert_eq!(refused.code, Code::InvalidArgument);
}
