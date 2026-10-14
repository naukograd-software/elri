//! Parts of documents: a fragment keeps pointing at its text through
//! edits and says so when it cannot; blocks split and merge as one
//! Change with a redirect for the absorbed one.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::document::document_service_ws::DocumentServiceClient;
use elri_proto::elri::v1::document::{
    CaptureFragmentRequest, Fragment, FragmentState, MergeBlocksRequest, ResolveFragmentRequest,
    SplitBlockRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, GetObjectRequest, ObjectKind, Placement, UpdateObjectRequest, Value,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-documents-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn t(v: &str) -> Value {
    Value {
        value: Some(value::Value::Text(v.to_owned())),
    }
}

#[tokio::test]
async fn fragments_follow_edits_and_blocks_split_and_merge() {
    let dir = tmp_dir("parts");
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
    let documents = DocumentServiceClient::new(conn);
    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    let create = |fields: HashMap<String, Value>| {
        let req = authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.clone(),
                kind: ObjectKind::Entity.into(),
                fields,
                ..Default::default()
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
    let set_text = |id: &str, text: &str| {
        let mut fields = HashMap::new();
        fields.insert("body".to_owned(), t(text));
        let req = authed(
            &endpoint,
            UpdateObjectRequest {
                space_id: space.clone(),
                id: id.to_owned(),
                set_fields: fields,
                ..Default::default()
            },
        );
        let objects = objects.clone();
        async move { objects.update_object(req).await.expect("update") }
    };
    let get = |id: &str| {
        let req = authed(
            &endpoint,
            GetObjectRequest {
                space_id: space.clone(),
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
    let text_of = |o: &elri_proto::elri::v1::object::Object| -> String {
        match o.fields.get("body").and_then(|v| v.value.clone()) {
            Some(value::Value::Text(s)) => s,
            other => panic!("not text: {other:?}"),
        }
    };
    let doc = create(HashMap::new()).await;
    let mut fields = HashMap::new();
    fields.insert(
        "body".to_owned(),
        t("Ideas about graphs when planning studies."),
    );
    let block = create(fields).await;
    let placed = objects
        .update_object(authed(
            &endpoint,
            UpdateObjectRequest {
                space_id: space.clone(),
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
        .expect("place")
        .msg
        .object
        .expect("object");
    let placement = placed.placements[0].id.clone();

    // Capture "graphs"; an out-of-range capture is refused.
    let capture = |start: i32, end: i32| {
        let req = authed(
            &endpoint,
            CaptureFragmentRequest {
                space_id: space.clone(),
                object_id: block.id.clone(),
                field_id: "body".into(),
                start,
                end,
            },
        );
        let documents = documents.clone();
        async move { documents.capture_fragment(req).await }
    };
    assert_eq!(
        capture(10, 500).await.expect_err("range").code,
        Code::InvalidArgument
    );
    let fragment = capture(12, 18)
        .await
        .expect("capture")
        .msg
        .fragment
        .expect("fragment");
    assert_eq!(fragment.quote, "graphs");
    assert_eq!(fragment.before, "Ideas about ");
    assert_eq!(fragment.after, " when planning studi");
    let resolve = |f: &Fragment| {
        let req = authed(
            &endpoint,
            ResolveFragmentRequest {
                space_id: space.clone(),
                fragment: Some(f.clone()),
            },
        );
        let documents = documents.clone();
        async move { documents.resolve_fragment(req).await.expect("resolve").msg }
    };
    let r = resolve(&fragment).await;
    assert_eq!(r.state(), FragmentState::Ok);
    let range = r.range.expect("range");
    assert_eq!((range.start, range.end), (12, 18));

    // Text inserted before it: the cursors follow, the fragment is still
    // whole.
    set_text(&block.id, "Some ideas about graphs when planning studies.").await;
    let r = resolve(&fragment).await;
    assert_eq!(r.state(), FragmentState::Ok, "{}", r.reason);
    assert_eq!(r.range.expect("range").start, 17);

    // The quoted word replaced: the position survives, the meaning is gone.
    set_text(&block.id, "Some ideas about trees when planning studies.").await;
    let r = resolve(&fragment).await;
    assert_eq!(r.state(), FragmentState::Moved, "{}", r.reason);
    // The cursors still point somewhere, but not at the quote.
    assert_ne!(r.range.expect("range").text, "graphs");

    // The word appears elsewhere: ambiguous, with candidates to confirm.
    set_text(
        &block.id,
        "Some ideas about trees; graphs come later, graphs again.",
    )
    .await;
    let r = resolve(&fragment).await;
    assert_eq!(r.state(), FragmentState::Ambiguous, "{}", r.reason);
    assert_eq!(r.candidates.len(), 2);
    assert_eq!(r.candidates[0].text, "graphs");

    // Split at the semicolon: the head keeps its identity, the tail is a
    // new block placed right after, remembering its origin.
    set_text(&block.id, "Head text. Tail text.").await;
    let split = documents
        .split_block(authed(
            &endpoint,
            SplitBlockRequest {
                space_id: space.clone(),
                document_id: doc.id.clone(),
                placement_id: placement.clone(),
                field_id: "body".into(),
                at: 11,
            },
        ))
        .await
        .expect("split")
        .msg;
    let head = split.head.expect("head");
    let tail = split.tail.expect("tail");
    assert_eq!(head.id, block.id);
    assert_eq!(text_of(&head), "Head text. ");
    assert_eq!(text_of(&tail), "Tail text.");
    assert_eq!(
        tail.fields["$split_from"],
        Value {
            value: Some(value::Value::String(block.id.clone()))
        }
    );
    let tail_placement = split.placement.expect("placement");
    assert_eq!(tail_placement.index, 1);
    assert_eq!(tail_placement.block_id, tail.id);
    let doc_now = get(&doc.id).await;
    assert_eq!(doc_now.placements.len(), 2);
    assert_eq!(doc_now.placements[1].block_id, tail.id);

    // A fragment captured in the tail; then merge the tail back: the text
    // is whole again, the tail is deleted with a redirect, and the
    // fragment resolves through it to candidates in the merged block.
    let tail_fragment = documents
        .capture_fragment(authed(
            &endpoint,
            CaptureFragmentRequest {
                space_id: space.clone(),
                object_id: tail.id.clone(),
                field_id: "body".into(),
                start: 0,
                end: 4,
            },
        ))
        .await
        .expect("capture")
        .msg
        .fragment
        .expect("fragment");
    assert_eq!(tail_fragment.quote, "Tail");
    let merged = documents
        .merge_blocks(authed(
            &endpoint,
            MergeBlocksRequest {
                space_id: space.clone(),
                document_id: doc.id.clone(),
                placement_id: placement.clone(),
                field_id: "body".into(),
            },
        ))
        .await
        .expect("merge")
        .msg;
    assert_eq!(merged.absorbed_id, tail.id);
    assert!(merged.absorbed_deleted);
    assert_eq!(
        text_of(&merged.merged.expect("merged")),
        "Head text. Tail text."
    );
    assert_eq!(get(&doc.id).await.placements.len(), 1);
    let gone = get(&tail.id).await;
    assert!(gone.deleted_at_ms.is_some());
    assert_eq!(
        gone.fields["$merged_into"],
        Value {
            value: Some(value::Value::String(block.id.clone()))
        }
    );
    let r = resolve(&tail_fragment).await;
    assert_eq!(r.redirected_to, block.id);
    assert_eq!(r.state(), FragmentState::Ambiguous, "{}", r.reason);
    assert_eq!(r.candidates.len(), 1);
    assert_eq!((r.candidates[0].start, r.candidates[0].end), (11, 15));

    // Nothing left to merge with.
    let err = documents
        .merge_blocks(authed(
            &endpoint,
            MergeBlocksRequest {
                space_id: space.clone(),
                document_id: doc.id.clone(),
                placement_id: placement,
                field_id: "body".into(),
            },
        ))
        .await
        .expect_err("no sibling");
    assert_eq!(err.code, Code::FailedPrecondition);
}
