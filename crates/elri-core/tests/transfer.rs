//! Objects travel between Spaces as explicit copies with rewritten
//! references, or move between local Spaces keeping their ids.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::definition::definition_service_ws::DefinitionServiceClient;
use elri_proto::elri::v1::definition::{CreateFieldDefinitionRequest, FieldKind};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, GetObjectRequest, ListObjectsRequest, ObjectKind, Participant,
    Placement, UpdateObjectRequest, Value,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use elri_proto::elri::v1::transfer::transfer_service_ws::TransferServiceClient;
use elri_proto::elri::v1::transfer::{
    ExportObjectsRequest, ImportObjectsRequest, MoveObjectsRequest,
};
use ws_proto::{Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-transfer-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn r(id: &str) -> Value {
    Value {
        value: Some(value::Value::Ref(id.to_owned())),
    }
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

#[tokio::test]
async fn export_import_rewrites_references_and_move_keeps_ids() {
    let dir = tmp_dir("xfer");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let defs = DefinitionServiceClient::new(conn.clone());
    let transfer = TransferServiceClient::new(conn);
    let mk_space = |name: &str| {
        let req = authed(&endpoint, CreateSpaceRequest { name: name.into() });
        let spaces = spaces.clone();
        async move {
            spaces
                .create_space(req)
                .await
                .expect("space")
                .msg
                .space
                .expect("space")
                .id
        }
    };
    let src = mk_space("src").await;
    let dst = mk_space("dst").await;
    let create = |space: &str, req: CreateObjectRequest| {
        let req = authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.to_owned(),
                ..req
            },
        );
        let objects = objects.clone();
        async move {
            objects
                .create_object(req)
                .await
                .expect("create")
                .msg
                .object
                .expect("object")
        }
    };

    // A definition, a block, a document placing it, a relation between
    // the document and the block that refers to the definition's field,
    // and a layer over the document.
    let due = defs
        .create_field_definition(authed(
            &endpoint,
            CreateFieldDefinitionRequest {
                space_id: src.clone(),
                name: "Due".into(),
                description: String::new(),
                kind: FieldKind::String.into(),
            },
        ))
        .await
        .expect("def")
        .msg
        .definition
        .expect("definition");
    let block = create(
        &src,
        CreateObjectRequest {
            kind: ObjectKind::Entity.into(),
            fields: HashMap::from([("title".to_owned(), s("block"))]),
            ..Default::default()
        },
    )
    .await;
    let doc = create(
        &src,
        CreateObjectRequest {
            kind: ObjectKind::Entity.into(),
            types: vec!["doc".into()],
            fields: HashMap::from([
                ("title".to_owned(), s("document")),
                (due.id.clone(), s("friday")),
                ("other".to_owned(), r(&uuid::Uuid::now_v7().to_string())),
            ]),
            ..Default::default()
        },
    )
    .await;
    objects
        .update_object(authed(
            &endpoint,
            UpdateObjectRequest {
                space_id: src.clone(),
                id: doc.id.clone(),
                set_placements: vec![Placement {
                    id: String::new(),
                    parent_id: String::new(),
                    index: 0,
                    block_id: block.id.clone(),
                }],
                ..Default::default()
            },
        ))
        .await
        .expect("place");
    let rel = create(
        &src,
        CreateObjectRequest {
            kind: ObjectKind::Relation.into(),
            participants: vec![
                Participant {
                    slot: "from".into(),
                    object_id: doc.id.clone(),
                    role_id: String::new(),
                },
                Participant {
                    slot: "to".into(),
                    object_id: block.id.clone(),
                    role_id: String::new(),
                },
            ],
            ..Default::default()
        },
    )
    .await;
    let layer = create(
        &src,
        CreateObjectRequest {
            kind: ObjectKind::Layer.into(),
            target_id: doc.id.clone(),
            fields: HashMap::from([("note".to_owned(), s("mine"))]),
            ..Default::default()
        },
    )
    .await;

    // Exporting the layer with dependencies pulls in the document, the
    // block, and the field definition; the relation is not referenced.
    let exported = transfer
        .export_objects(authed(
            &endpoint,
            ExportObjectsRequest {
                space_id: src.clone(),
                ids: vec![layer.id.clone(), rel.id.clone()],
                with_dependencies: true,
            },
        ))
        .await
        .expect("export")
        .msg;
    assert_eq!(exported.count, 5);

    let imported = transfer
        .import_objects(authed(
            &endpoint,
            ImportObjectsRequest {
                space_id: dst.clone(),
                bundle: exported.bundle.clone(),
                keep_ids: false,
            },
        ))
        .await
        .expect("import")
        .msg;
    assert_eq!(imported.objects.len(), 5);
    let new = |old: &str| imported.ids.get(old).expect("mapped").clone();
    assert_ne!(new(&doc.id), doc.id);
    let get = |space: &str, id: &str| {
        let req = authed(
            &endpoint,
            GetObjectRequest {
                space_id: space.to_owned(),
                id: id.to_owned(),
            },
        );
        let objects = objects.clone();
        async move {
            objects
                .get_object(req)
                .await
                .expect("get")
                .msg
                .object
                .expect("object")
        }
    };
    let doc2 = get(&dst, &new(&doc.id)).await;
    assert_eq!(doc2.fields["title"], s("document"));
    assert_eq!(
        doc2.fields[&new(&due.id)],
        s("friday"),
        "field id rewritten"
    );
    assert_eq!(
        doc2.fields["other"], doc.fields["other"],
        "external ref kept"
    );
    assert_eq!(doc2.placements.len(), 1);
    assert_eq!(doc2.placements[0].block_id, new(&block.id));
    assert!(doc2.fields.contains_key("$origin"));
    let rel2 = get(&dst, &new(&rel.id)).await;
    assert_eq!(rel2.participants[0].object_id, new(&doc.id));
    assert_eq!(rel2.participants[1].object_id, new(&block.id));
    let layer2 = get(&dst, &new(&layer.id)).await;
    assert_eq!(layer2.target_id.as_deref(), Some(new(&doc.id).as_str()));
    let due2 = get(&dst, &new(&due.id)).await;
    assert_eq!(due2.kind(), ObjectKind::FieldDef);

    // Importing with kept ids into the destination is refused now that
    // they are... not there: the copies have new ids, so it works, and a
    // second time it is refused.
    let kept = transfer
        .import_objects(authed(
            &endpoint,
            ImportObjectsRequest {
                space_id: dst.clone(),
                bundle: exported.bundle.clone(),
                keep_ids: true,
            },
        ))
        .await
        .expect("keep ids")
        .msg;
    assert_eq!(kept.ids[&doc.id], doc.id);
    let again = transfer
        .import_objects(authed(
            &endpoint,
            ImportObjectsRequest {
                space_id: dst.clone(),
                bundle: exported.bundle.clone(),
                keep_ids: true,
            },
        ))
        .await
        .expect_err("ids taken");
    assert_eq!(again.code, Code::AlreadyExists);

    // Moving keeps the id and leaves a tombstone naming the destination.
    let third = mk_space("third").await;
    let moved = transfer
        .move_objects(authed(
            &endpoint,
            MoveObjectsRequest {
                source_space_id: src.clone(),
                dest_space_id: third.clone(),
                ids: vec![block.id.clone()],
            },
        ))
        .await
        .expect("move")
        .msg
        .objects;
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].id, block.id);
    assert_eq!(moved[0].space_id, third);
    assert!(!moved[0].fields.contains_key("$origin"));
    let gone = get(&src, &block.id).await;
    assert!(gone.deleted_at_ms.is_some());
    assert_eq!(gone.fields["$moved_to"], s(&third));
    let listed = objects
        .list_objects(authed(
            &endpoint,
            ListObjectsRequest {
                space_id: third.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("list")
        .msg
        .objects;
    assert_eq!(listed.len(), 1);
}
