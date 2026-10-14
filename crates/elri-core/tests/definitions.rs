//! Definitions through the API, and the field-kind rule they impose on
//! object writes.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::definition::definition_service_ws::DefinitionServiceClient;
use elri_proto::elri::v1::definition::{
    CreateConstraintRequest, CreateFieldDefinitionRequest, CreateTypeDefinitionRequest, FieldKind,
    GetSchemaRequest, ListFieldDefinitionsRequest, ListPublicationsRequest,
    ListTypeDefinitionsRequest, ListViolationsRequest, PinDefinitionRequest,
    PublishDefinitionRequest, TypeField, UpdateConstraintRequest, UpdateFieldDefinitionRequest,
    UpdateTypeDefinitionRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, ListObjectsRequest, ObjectKind,
    UpdateObjectRequest, Value,
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

#[tokio::test]
async fn publications_are_immutable_and_pins_move_the_epoch() {
    let dir = tmp_dir("pins");
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
    let due = sess
        .defs
        .create_field_definition(sess.authed(CreateFieldDefinitionRequest {
            space_id: space.id.clone(),
            name: "Due".into(),
            description: String::new(),
            kind: FieldKind::Time.into(),
        }))
        .await
        .expect("field")
        .msg
        .definition
        .expect("definition");
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
        .expect("type")
        .msg
        .definition
        .expect("definition");

    // Publishing the type publishes its unpublished field first; the
    // dependency is exact and the digest covers both.
    let p1 = sess
        .defs
        .publish_definition(sess.authed(PublishDefinitionRequest {
            space_id: space.id.clone(),
            id: task.id.clone(),
        }))
        .await
        .expect("publish")
        .msg
        .publication
        .expect("publication");
    assert_eq!(p1.version, 1);
    assert_eq!(p1.definition_id, task.id);
    assert_eq!(p1.dependencies.get(&due.id), Some(&1));
    assert_eq!(p1.digest.len(), 64);
    let snapshot: serde_json::Value = serde_json::from_str(&p1.snapshot_json).expect("json");
    assert_eq!(snapshot["name"], "Task");
    assert_eq!(snapshot["fields"][0]["field_id"], due.id);
    assert_eq!(snapshot["fields"][0]["version"], 1);
    let all = sess
        .defs
        .list_publications(sess.authed(ListPublicationsRequest {
            space_id: space.id.clone(),
            definition_id: String::new(),
        }))
        .await
        .expect("list")
        .msg
        .publications;
    assert_eq!(all.len(), 2, "type and field");

    // Renaming the live definition does not touch the publication; a second
    // publication is version 2 with the new name.
    sess.defs
        .update_type_definition(sess.authed(UpdateTypeDefinitionRequest {
            space_id: space.id.clone(),
            id: task.id.clone(),
            name: Some("Chore".into()),
            ..Default::default()
        }))
        .await
        .expect("rename");
    let p2 = sess
        .defs
        .publish_definition(sess.authed(PublishDefinitionRequest {
            space_id: space.id.clone(),
            id: task.id.clone(),
        }))
        .await
        .expect("publish")
        .msg
        .publication
        .expect("publication");
    assert_eq!(p2.version, 2);
    assert_ne!(p2.digest, p1.digest);
    let still = sess
        .defs
        .list_publications(sess.authed(ListPublicationsRequest {
            space_id: space.id.clone(),
            definition_id: task.id.clone(),
        }))
        .await
        .expect("list")
        .msg
        .publications;
    assert_eq!(still[1].snapshot_json, p1.snapshot_json);

    // Pins move the epoch; a stale expected epoch is refused.
    let schema = sess
        .defs
        .get_schema(sess.authed(GetSchemaRequest {
            space_id: space.id.clone(),
        }))
        .await
        .expect("schema")
        .msg
        .schema
        .expect("schema");
    assert_eq!(schema.epoch, 0);
    assert!(schema.pins.is_empty());
    let pin = sess
        .defs
        .pin_definition(sess.authed(PinDefinitionRequest {
            space_id: space.id.clone(),
            publication_id: p1.id.clone(),
            expected_epoch: 0,
        }))
        .await
        .expect("pin")
        .msg
        .pin
        .expect("pin");
    assert_eq!(pin.epoch, 1);
    assert_eq!(pin.version, 1);
    let stale = sess
        .defs
        .pin_definition(sess.authed(PinDefinitionRequest {
            space_id: space.id.clone(),
            publication_id: p2.id.clone(),
            expected_epoch: 0,
        }))
        .await
        .expect_err("epoch moved");
    assert_eq!(stale.code, Code::FailedPrecondition);
    let not_a_publication = sess
        .defs
        .pin_definition(sess.authed(PinDefinitionRequest {
            space_id: space.id.clone(),
            publication_id: task.id.clone(),
            expected_epoch: 1,
        }))
        .await
        .expect_err("not a publication");
    assert_eq!(not_a_publication.code, Code::InvalidArgument);
    sess.defs
        .pin_definition(sess.authed(PinDefinitionRequest {
            space_id: space.id.clone(),
            publication_id: p2.id.clone(),
            expected_epoch: 1,
        }))
        .await
        .expect("pin v2");
    let schema = sess
        .defs
        .get_schema(sess.authed(GetSchemaRequest {
            space_id: space.id.clone(),
        }))
        .await
        .expect("schema")
        .msg
        .schema
        .expect("schema");
    assert_eq!(schema.epoch, 2);
    assert_eq!(schema.pins.len(), 1);
    assert_eq!(schema.pins[0].version, 2);

    // A pinned field keeps enforcing its published kind.
    let mut fields = HashMap::new();
    fields.insert(due.id.clone(), s("not a time"));
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            fields,
            ..Default::default()
        }))
        .await
        .expect_err("kind enforced");
    assert_eq!(err.code, Code::InvalidArgument);
    let _ = t(0);
}

#[tokio::test]
async fn constraints_report_by_default_and_refuse_when_strict() {
    let dir = tmp_dir("constraints");
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
    let email = sess
        .defs
        .create_field_definition(sess.authed(CreateFieldDefinitionRequest {
            space_id: space.id.clone(),
            name: "Email".into(),
            description: String::new(),
            kind: FieldKind::String.into(),
        }))
        .await
        .expect("field")
        .msg
        .definition
        .expect("definition");
    let create = |value: &str| {
        let mut fields = HashMap::new();
        fields.insert(email.id.clone(), s(value));
        let req = sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            fields,
            ..Default::default()
        });
        let objects = sess.objects.clone();
        async move { objects.create_object(req).await }
    };
    // Two objects already share a value when the constraint arrives.
    let first = create("a@x")
        .await
        .expect("first")
        .msg
        .object
        .expect("object");
    let second = create(" a@x ")
        .await
        .expect("second")
        .msg
        .object
        .expect("object");

    // Diagnostic: both are listed as a violation; nothing is refused.
    let c = sess
        .defs
        .create_constraint(sess.authed(CreateConstraintRequest {
            space_id: space.id.clone(),
            name: "unique email".into(),
            field_id: email.id.clone(),
            unique: true,
            strict: false,
        }))
        .await
        .expect("constraint")
        .msg
        .constraint
        .expect("constraint");
    assert!(!c.strict);
    let v = sess
        .defs
        .list_violations(sess.authed(ListViolationsRequest {
            space_id: space.id.clone(),
        }))
        .await
        .expect("violations")
        .msg
        .violations;
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].value, "text:a@x");
    let mut ids = v[0].object_ids.clone();
    ids.sort();
    let mut expected = vec![first.id.clone(), second.id.clone()];
    expected.sort();
    assert_eq!(ids, expected);
    create("a@x").await.expect("diagnostic does not refuse");

    // Strict: a new claim is refused; a different value passes; the same
    // object keeping its value passes; a deleted holder frees the value.
    sess.defs
        .update_constraint(sess.authed(UpdateConstraintRequest {
            space_id: space.id.clone(),
            id: c.id.clone(),
            name: None,
            strict: Some(true),
        }))
        .await
        .expect("strict");
    let refused = create("a@x").await.expect_err("strict refuses");
    assert_eq!(refused.code, Code::FailedPrecondition);
    assert!(
        refused.message.contains("constraint"),
        "{}",
        refused.message
    );
    let third = create("b@x")
        .await
        .expect("other value")
        .msg
        .object
        .expect("object");
    let mut same = HashMap::new();
    same.insert(email.id.clone(), s("b@x"));
    sess.objects
        .update_object(sess.authed(UpdateObjectRequest {
            space_id: space.id.clone(),
            id: third.id.clone(),
            set_fields: same.clone(),
            ..Default::default()
        }))
        .await
        .expect("same object keeps its value");
    let taken = sess
        .objects
        .update_object(sess.authed(UpdateObjectRequest {
            space_id: space.id.clone(),
            id: first.id.clone(),
            set_fields: same,
            ..Default::default()
        }))
        .await
        .expect_err("value taken by third");
    assert_eq!(taken.code, Code::FailedPrecondition);
    sess.objects
        .delete_object(sess.authed(DeleteObjectRequest {
            space_id: space.id.clone(),
            id: third.id.clone(),
        }))
        .await
        .expect("delete");
    create("b@x").await.expect("freed by the tombstone");
}
