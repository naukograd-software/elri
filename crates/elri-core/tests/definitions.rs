//! Definitions through the API, and the field-kind rule they impose on
//! object writes.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::definition::definition_service_ws::DefinitionServiceClient;
use elri_proto::elri::v1::definition::{
    CreateFieldDefinitionRequest, CreateTypeDefinitionRequest, FieldKind,
    ListFieldDefinitionsRequest, ListTypeDefinitionsRequest, TypeField,
    UpdateFieldDefinitionRequest, UpdateTypeDefinitionRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, ListObjectsRequest, ObjectKind, Value,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-defs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

fn t(ms: i64) -> Value {
    Value {
        value: Some(value::Value::TimeMs(ms)),
    }
}

struct Session {
    endpoint: Endpoint,
    spaces: SpaceServiceClient,
    objects: ObjectServiceClient,
    defs: DefinitionServiceClient,
}

impl Session {
    fn authed<T>(&self, msg: T) -> Request<T> {
        Request::new(msg).with_header("authorization", format!("Bearer {}", self.endpoint.token))
    }
}

#[tokio::test]
async fn definitions_fix_field_kinds() {
    let dir = tmp_dir("kinds");
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
    let sess = Session {
        endpoint,
        spaces: SpaceServiceClient::new(conn.clone()),
        objects: ObjectServiceClient::new(conn.clone()),
        defs: DefinitionServiceClient::new(conn),
    };
    let space = sess
        .spaces
        .create_space(sess.authed(CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space");

    // A time field and a type that requires it.
    let due = sess
        .defs
        .create_field_definition(sess.authed(CreateFieldDefinitionRequest {
            space_id: space.id.clone(),
            name: "Due".into(),
            description: "when it is due".into(),
            kind: FieldKind::Time.into(),
        }))
        .await
        .expect("field def")
        .msg
        .definition
        .expect("definition");
    assert_eq!(due.kind(), FieldKind::Time);
    assert_eq!(due.name, "Due");
    let task = sess
        .defs
        .create_type_definition(sess.authed(CreateTypeDefinitionRequest {
            space_id: space.id.clone(),
            name: "Task".into(),
            description: String::new(),
            fields: vec![TypeField {
                field_id: due.id.clone(),
                required: true,
            }],
        }))
        .await
        .expect("type def")
        .msg
        .definition
        .expect("definition");
    assert_eq!(task.fields.len(), 1);
    assert!(task.fields[0].required);

    // The wrong kind for a defined field is refused; the right one and an
    // undefined field id both pass.
    let mut wrong = HashMap::new();
    wrong.insert(due.id.clone(), s("tomorrow"));
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            fields: wrong,
            ..Default::default()
        }))
        .await
        .expect_err("string into a time field");
    assert_eq!(err.code, Code::InvalidArgument);
    let mut right = HashMap::new();
    right.insert(due.id.clone(), t(1_000));
    right.insert("free".to_owned(), s("anything"));
    let obj = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            types: vec![task.id.clone()],
            fields: right,
            ..Default::default()
        }))
        .await
        .expect("valid object")
        .msg
        .object
        .expect("object");
    assert_eq!(obj.fields[&due.id], t(1_000));

    // Definitions are not created through ObjectService, but they are
    // Objects: they list by kind and delete like any other.
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::FieldDef.into(),
            ..Default::default()
        }))
        .await
        .expect_err("definition via ObjectService");
    assert_eq!(err.code, Code::InvalidArgument);
    let field_defs = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::FieldDef.into(),
            ..Default::default()
        }))
        .await
        .expect("list")
        .msg
        .objects;
    assert_eq!(field_defs.len(), 1);
    assert_eq!(field_defs[0].id, due.id);

    // Updates: rename, retype the type, kind stays.
    let due = sess
        .defs
        .update_field_definition(sess.authed(UpdateFieldDefinitionRequest {
            space_id: space.id.clone(),
            id: due.id.clone(),
            name: Some("Due date".into()),
            description: Some(String::new()),
        }))
        .await
        .expect("rename")
        .msg
        .definition
        .expect("definition");
    assert_eq!(due.name, "Due date");
    assert_eq!(due.description, "");
    assert_eq!(due.kind(), FieldKind::Time);
    let task = sess
        .defs
        .update_type_definition(sess.authed(UpdateTypeDefinitionRequest {
            space_id: space.id.clone(),
            id: task.id.clone(),
            name: None,
            description: None,
            set_fields: vec![TypeField {
                field_id: "free".into(),
                required: false,
            }],
            remove_fields: vec![due.id.clone()],
        }))
        .await
        .expect("retype")
        .msg
        .definition
        .expect("definition");
    assert_eq!(task.fields.len(), 1);
    assert_eq!(task.fields[0].field_id, "free");
    let types = sess
        .defs
        .list_type_definitions(sess.authed(ListTypeDefinitionsRequest {
            space_id: space.id.clone(),
            include_deleted: false,
        }))
        .await
        .expect("list types")
        .msg
        .definitions;
    assert_eq!(types.len(), 1);

    // Deleting the field definition lifts its rule.
    sess.objects
        .delete_object(sess.authed(DeleteObjectRequest {
            space_id: space.id.clone(),
            id: due.id.clone(),
        }))
        .await
        .expect("delete def");
    let visible = sess
        .defs
        .list_field_definitions(sess.authed(ListFieldDefinitionsRequest {
            space_id: space.id.clone(),
            include_deleted: false,
        }))
        .await
        .expect("list")
        .msg
        .definitions;
    assert!(visible.is_empty());
    let mut now_free = HashMap::new();
    now_free.insert(due.id.clone(), s("tomorrow"));
    sess.objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            fields: now_free,
            ..Default::default()
        }))
        .await
        .expect("no rule after deletion");
}
