//! Spaces and Objects through the API: create, read, update, list, delete,
//! and survival across a core restart.

#![allow(clippy::expect_used, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::{ChangeStatus, ListChangesRequest};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, GetObjectRequest, ListObjectsRequest,
    ObjectKind, Participant, UpdateObjectRequest, Value,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::{Authority, CreateSpaceRequest, ListSpacesRequest};
use ws_proto::{Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-objects-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

struct Session {
    endpoint: Endpoint,
    spaces: SpaceServiceClient,
    objects: ObjectServiceClient,
    changes: ChangeServiceClient,
}

impl Session {
    async fn open(dir: &std::path::Path) -> Self {
        let endpoint = Endpoint::read(dir).expect("endpoint file");
        let conn = Client::dial(&endpoint.url, ClientConfig::default())
            .await
            .expect("dial");
        Self {
            endpoint,
            spaces: SpaceServiceClient::new(conn.clone()),
            objects: ObjectServiceClient::new(conn.clone()),
            changes: ChangeServiceClient::new(conn),
        }
    }

    fn authed<T>(&self, msg: T) -> Request<T> {
        Request::new(msg).with_header("authorization", format!("Bearer {}", self.endpoint.token))
    }
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

fn i(v: i64) -> Value {
    Value {
        value: Some(value::Value::Int(v)),
    }
}

fn b(v: bool) -> Value {
    Value {
        value: Some(value::Value::Bool(v)),
    }
}

#[tokio::test]
async fn spaces_and_objects_end_to_end() {
    let dir = tmp_dir("e2e");
    let core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let sess = Session::open(&dir).await;

    // Fresh device: no Spaces.
    let list = sess
        .spaces
        .list_spaces(sess.authed(ListSpacesRequest {}))
        .await
        .expect("list");
    assert!(list.msg.spaces.is_empty());

    // Create a local Space.
    let space = sess
        .spaces
        .create_space(sess.authed(CreateSpaceRequest {
            name: "Personal".into(),
        }))
        .await
        .expect("create space")
        .msg
        .space
        .expect("space");
    assert_eq!(space.name, "Personal");
    assert_eq!(space.authority(), Authority::Local);
    let space_id = space.id.clone();

    // Create an Object with types and fields.
    let mut fields = HashMap::new();
    fields.insert("title".to_owned(), s("Learn Loro"));
    fields.insert("priority".to_owned(), i(2));
    fields.insert("done".to_owned(), b(false));
    let created = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space_id.clone(),
            kind: ObjectKind::Entity.into(),
            types: vec!["task".into()],
            fields,
            ..Default::default()
        }))
        .await
        .expect("create object")
        .msg
        .object
        .expect("object");
    assert_eq!(created.kind(), ObjectKind::Entity);
    assert_eq!(created.types, vec!["task".to_owned()]);
    assert_eq!(created.fields["title"], s("Learn Loro"));
    assert!(created.deleted_at_ms.is_none());

    // Read it back.
    let got = sess
        .objects
        .get_object(sess.authed(GetObjectRequest {
            space_id: space_id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("get")
        .msg
        .object
        .expect("object");
    assert_eq!(got, created);

    // Update: set, remove, retype.
    let mut set = HashMap::new();
    set.insert("done".to_owned(), b(true));
    let updated = sess
        .objects
        .update_object(sess.authed(UpdateObjectRequest {
            space_id: space_id.clone(),
            id: created.id.clone(),
            set_fields: set,
            remove_fields: vec!["priority".into()],
            add_types: vec!["habit".into()],
            remove_types: vec!["task".into()],
            ..Default::default()
        }))
        .await
        .expect("update")
        .msg
        .object
        .expect("object");
    assert_eq!(updated.fields["done"], b(true));
    assert!(!updated.fields.contains_key("priority"));
    assert_eq!(updated.types, vec!["habit".to_owned()]);
    assert!(updated.updated_at_ms >= created.updated_at_ms);

    // A second Object of another kind, then list with filters.
    sess.objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space_id.clone(),
            kind: ObjectKind::Relation.into(),
            ..Default::default()
        }))
        .await
        .expect("create relation");
    let all = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list all");
    assert_eq!(all.msg.objects.len(), 2);
    let habits = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space_id.clone(),
            type_id: "habit".into(),
            ..Default::default()
        }))
        .await
        .expect("list by type");
    assert_eq!(habits.msg.objects.len(), 1);
    assert_eq!(habits.msg.objects[0].id, created.id);
    let relations = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space_id.clone(),
            kind: ObjectKind::Relation.into(),
            ..Default::default()
        }))
        .await
        .expect("list by kind");
    assert_eq!(relations.msg.objects.len(), 1);

    // Delete hides by default, shows with include_deleted.
    sess.objects
        .delete_object(sess.authed(DeleteObjectRequest {
            space_id: space_id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("delete");
    let visible = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list after delete");
    assert_eq!(visible.msg.objects.len(), 1);
    let with_deleted = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space_id.clone(),
            include_deleted: true,
            ..Default::default()
        }))
        .await
        .expect("list incl deleted");
    assert_eq!(with_deleted.msg.objects.len(), 2);
    let deleted = sess
        .objects
        .get_object(sess.authed(GetObjectRequest {
            space_id: space_id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("get deleted")
        .msg
        .object
        .expect("object");
    assert!(deleted.deleted_at_ms.is_some());

    // Restart the core: everything is still there.
    core.shutdown();
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("restart");
    let sess = Session::open(&dir).await;
    let spaces = sess
        .spaces
        .list_spaces(sess.authed(ListSpacesRequest {}))
        .await
        .expect("list after restart");
    assert_eq!(spaces.msg.spaces.len(), 1);
    assert_eq!(spaces.msg.spaces[0].id, space_id);
    let again = sess
        .objects
        .get_object(sess.authed(GetObjectRequest {
            space_id: space_id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("get after restart")
        .msg
        .object
        .expect("object");
    assert_eq!(again, deleted);
}

#[tokio::test]
async fn invalid_input_is_rejected() {
    let dir = tmp_dir("invalid");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let sess = Session::open(&dir).await;

    let err = sess
        .spaces
        .create_space(sess.authed(CreateSpaceRequest { name: "  ".into() }))
        .await
        .expect_err("blank name");
    assert_eq!(err.code, Code::InvalidArgument);

    let err = sess
        .objects
        .get_object(sess.authed(GetObjectRequest {
            space_id: "nope".into(),
            id: "nope".into(),
        }))
        .await
        .expect_err("bad ids");
    assert_eq!(err.code, Code::InvalidArgument);

    let unknown_space = uuid::Uuid::now_v7().to_string();
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: unknown_space.clone(),
            kind: ObjectKind::Entity.into(),
            ..Default::default()
        }))
        .await
        .expect_err("unknown space");
    assert_eq!(err.code, Code::NotFound);

    let space = sess
        .spaces
        .create_space(sess.authed(CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("create")
        .msg
        .space
        .expect("space");
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Unspecified.into(),
            ..Default::default()
        }))
        .await
        .expect_err("kind required");
    assert_eq!(err.code, Code::InvalidArgument);
    let err = sess
        .objects
        .get_object(sess.authed(GetObjectRequest {
            space_id: space.id,
            id: uuid::Uuid::now_v7().to_string(),
        }))
        .await
        .expect_err("unknown object");
    assert_eq!(err.code, Code::NotFound);
}

#[tokio::test]
async fn every_write_is_a_change_and_snapshots_compact() {
    let dir = tmp_dir("changes");
    let core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let sess = Session::open(&dir).await;
    let space = sess
        .spaces
        .create_space(sess.authed(CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("create")
        .msg
        .space
        .expect("space");

    let created = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            ..Default::default()
        }))
        .await
        .expect("create object")
        .msg
        .object
        .expect("object");

    // Well past the compaction threshold.
    let writes: i64 = 70;
    for n in 0..writes {
        let mut set = HashMap::new();
        set.insert("counter".to_owned(), i(n));
        sess.objects
            .update_object(sess.authed(UpdateObjectRequest {
                space_id: space.id.clone(),
                id: created.id.clone(),
                set_fields: set,
                ..Default::default()
            }))
            .await
            .expect("update");
    }

    let changes = sess
        .changes
        .list_changes(sess.authed(ListChangesRequest {
            space_id: space.id.clone(),
            limit: 1000,
        }))
        .await
        .expect("list changes")
        .msg
        .changes;
    assert_eq!(
        i64::try_from(changes.len()).expect("len"),
        writes + 1,
        "one Change per write, plus the create"
    );
    assert!(changes.iter().all(|c| c.status() == ChangeStatus::Local));
    assert!(changes
        .iter()
        .all(|c| c.parts.len() == 1 && c.parts[0].object_id == created.id));
    assert!(changes
        .iter()
        .all(|c| c.device_id == core.device_id().as_string()));

    // State is intact after compaction and after a restart.
    core.shutdown();
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("restart");
    let sess = Session::open(&dir).await;
    let got = sess
        .objects
        .get_object(sess.authed(GetObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("get")
        .msg
        .object
        .expect("object");
    assert_eq!(got.fields["counter"], i(writes - 1));
}

#[tokio::test]
async fn relations_and_layers() {
    let dir = tmp_dir("relations");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let sess = Session::open(&dir).await;
    let space = sess
        .spaces
        .create_space(sess.authed(CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("create")
        .msg
        .space
        .expect("space");
    let entity = |sess: &Session| {
        let space_id = space.id.clone();
        let req = sess.authed(CreateObjectRequest {
            space_id,
            kind: ObjectKind::Entity.into(),
            ..Default::default()
        });
        let objects = sess.objects.clone();
        async move {
            objects
                .create_object(req)
                .await
                .expect("entity")
                .msg
                .object
                .expect("object")
        }
    };
    let a = entity(&sess).await;
    let b = entity(&sess).await;

    // Relation with two participants.
    let rel = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Relation.into(),
            participants: vec![
                Participant {
                    slot: "from".into(),
                    object_id: a.id.clone(),
                    role_id: "parent".into(),
                },
                Participant {
                    slot: "to".into(),
                    object_id: b.id.clone(),
                    role_id: String::new(),
                },
            ],
            ..Default::default()
        }))
        .await
        .expect("relation")
        .msg
        .object
        .expect("object");
    assert_eq!(rel.participants.len(), 2);
    assert_eq!(rel.participants[0].slot, "from");
    assert_eq!(rel.participants[0].role_id, "parent");

    // Found through either participant; not through an unrelated id.
    for who in [&a.id, &b.id] {
        let found = sess
            .objects
            .list_objects(sess.authed(ListObjectsRequest {
                space_id: space.id.clone(),
                participant_object_id: who.clone(),
                ..Default::default()
            }))
            .await
            .expect("list by participant")
            .msg
            .objects;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, rel.id);
    }
    let none = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space.id.clone(),
            participant_object_id: uuid::Uuid::now_v7().to_string(),
            ..Default::default()
        }))
        .await
        .expect("list none")
        .msg
        .objects;
    assert!(none.is_empty());

    // Remove a participant, replace another.
    let rel = sess
        .objects
        .update_object(sess.authed(UpdateObjectRequest {
            space_id: space.id.clone(),
            id: rel.id.clone(),
            set_participants: vec![Participant {
                slot: "from".into(),
                object_id: b.id.clone(),
                role_id: "child".into(),
            }],
            remove_participants: vec!["to".into()],
            ..Default::default()
        }))
        .await
        .expect("update relation")
        .msg
        .object
        .expect("object");
    assert_eq!(rel.participants.len(), 1);
    assert_eq!(rel.participants[0].object_id, b.id);
    assert_eq!(rel.participants[0].role_id, "child");
    let a_rels = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space.id.clone(),
            participant_object_id: a.id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list a")
        .msg
        .objects;
    assert!(a_rels.is_empty(), "a no longer participates");

    // Participants on an entity are rejected.
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            participants: vec![Participant {
                slot: "x".into(),
                object_id: a.id.clone(),
                role_id: String::new(),
            }],
            ..Default::default()
        }))
        .await
        .expect_err("entity with participants");
    assert_eq!(err.code, Code::InvalidArgument);

    // Layers need a target; they are found through it.
    let err = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Layer.into(),
            ..Default::default()
        }))
        .await
        .expect_err("layer without target");
    assert_eq!(err.code, Code::InvalidArgument);
    let mut fields = HashMap::new();
    fields.insert("note".to_owned(), s("mine"));
    let layer = sess
        .objects
        .create_object(sess.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Layer.into(),
            target_id: a.id.clone(),
            fields,
            ..Default::default()
        }))
        .await
        .expect("layer")
        .msg
        .object
        .expect("object");
    assert_eq!(layer.target_id.as_deref(), Some(a.id.as_str()));
    let layers = sess
        .objects
        .list_objects(sess.authed(ListObjectsRequest {
            space_id: space.id.clone(),
            target_id: a.id.clone(),
            ..Default::default()
        }))
        .await
        .expect("layers of a")
        .msg
        .objects;
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].id, layer.id);
    assert_eq!(layers[0].fields["note"], s("mine"));
}
