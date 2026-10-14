//! Mappings: fields of several Spaces that mean the same; code proposes,
//! a person approves; search and context resolution use approved ones.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::context::context_service_ws::ContextServiceClient;
use elri_proto::elri::v1::context::{
    ApproveMappingRequest, CreateContextRequest, ListMappingsRequest, MappingMember,
    ProposeMappingRequest, ResolveContextRequest, Selection,
};
use elri_proto::elri::v1::definition::definition_service_ws::DefinitionServiceClient;
use elri_proto::elri::v1::definition::{CreateFieldDefinitionRequest, FieldKind};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{value, CreateObjectRequest, ObjectKind, Value};
use elri_proto::elri::v1::script::grant_service_ws::GrantServiceClient;
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{
    CreateScriptRequest, GrantScriptRequest, Manifest, RunScriptRequest,
};
use elri_proto::elri::v1::search::search_service_ws::SearchServiceClient;
use elri_proto::elri::v1::search::SearchRequest;
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-mappings-{name}-{}", std::process::id()));
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

#[tokio::test]
async fn mappings_are_proposed_approved_and_used() {
    let dir = tmp_dir("use");
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
    let defs = DefinitionServiceClient::new(conn.clone());
    let contexts = ContextServiceClient::new(conn.clone());
    let search = SearchServiceClient::new(conn.clone());
    let scripts = ScriptServiceClient::new(conn.clone());
    let grants = GrantServiceClient::new(conn);
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
    let work = mk_space("work").await;
    let home = mk_space("home").await;
    let field = |space: &str, name: &str| {
        let req = authed(
            &endpoint,
            CreateFieldDefinitionRequest {
                space_id: space.to_owned(),
                name: name.to_owned(),
                description: String::new(),
                kind: FieldKind::String.into(),
            },
        );
        let defs = defs.clone();
        async move {
            defs.create_field_definition(req)
                .await
                .expect("field")
                .msg
                .definition
                .expect("definition")
                .id
        }
    };
    let work_prio = field(&work, "Priority").await;
    let work_note = field(&work, "Note").await;
    let home_urgency = field(&home, "Urgency").await;
    let create = |space: &str, fields: Vec<(&str, &str)>| {
        let req = authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.to_owned(),
                kind: ObjectKind::Entity.into(),
                fields: fields
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), s(v)))
                    .collect::<HashMap<_, _>>(),
                ..Default::default()
            },
        );
        let objects = objects.clone();
        async move {
            objects
                .create_object(req)
                .await
                .expect("object")
                .msg
                .object
                .expect("object")
        }
    };
    let urgent = create(&work, vec![(&work_prio, "urgent"), (&work_note, "later")]).await;
    let noted = create(&work, vec![(&work_prio, "low"), (&work_note, "urgent fix")]).await;

    // A person's proposal is approved at once; a bad member is refused.
    let err = contexts
        .propose_mapping(authed(
            &endpoint,
            ProposeMappingRequest {
                space_id: work.clone(),
                name: "priority".into(),
                members: vec![MappingMember {
                    space_id: work.clone(),
                    field_id: "nope".into(),
                }],
            },
        ))
        .await
        .expect_err("bad member");
    assert_eq!(err.code, Code::InvalidArgument);
    let mapping = contexts
        .propose_mapping(authed(
            &endpoint,
            ProposeMappingRequest {
                space_id: work.clone(),
                name: "priority".into(),
                members: vec![
                    MappingMember {
                        space_id: work.clone(),
                        field_id: work_prio.clone(),
                    },
                    MappingMember {
                        space_id: home.clone(),
                        field_id: home_urgency.clone(),
                    },
                ],
            },
        ))
        .await
        .expect("propose")
        .msg
        .mapping
        .expect("mapping");
    assert!(mapping.approved);
    assert_eq!(mapping.proposed_by, "user");
    assert_eq!(mapping.members.len(), 2);

    // Search through the mapping sees only the mapped field: the note
    // that says "urgent" in Note is not a hit.
    let hits = search
        .search(authed(
            &endpoint,
            SearchRequest {
                space_id: work.clone(),
                query: "urgent".into(),
                kind: 0,
                limit: 10,
                mapping_id: mapping.id.clone(),
            },
        ))
        .await
        .expect("search")
        .msg
        .hits;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].object.as_ref().expect("object").id, urgent.id);
    assert_eq!(hits[0].fields, vec![work_prio.clone()]);
    let plain = search
        .search(authed(
            &endpoint,
            SearchRequest {
                space_id: work.clone(),
                query: "urgent".into(),
                kind: 0,
                limit: 10,
                mapping_id: String::new(),
            },
        ))
        .await
        .expect("search")
        .msg
        .hits;
    assert_eq!(plain.len(), 2);
    let _ = noted;

    // A script's proposal waits for a person; until approved it is not
    // used and not returned with a resolved Context.
    let script = scripts
        .create_script(authed(
            &endpoint,
            CreateScriptRequest {
                space_id: work.clone(),
                name: "suggest".into(),
                source: format!(
                    r#"return elri.propose_mapping({{ name = "note", members = {{ {{ space = "{work}", field = "{work_note}" }} }} }})"#
                ),
                manifest: Some(Manifest::default()),
            },
        ))
        .await
        .expect("script")
        .msg
        .script
        .expect("script");
    grants
        .grant_script(authed(
            &endpoint,
            GrantScriptRequest {
                expected_version: String::new(),
                space_id: work.clone(),
                script_id: script.id.clone(),
            },
        ))
        .await
        .expect("grant");
    let res = scripts
        .run_script(authed(
            &endpoint,
            RunScriptRequest {
                space_id: work.clone(),
                id: script.id.clone(),
                args_json: String::new(),
            },
        ))
        .await
        .expect("run")
        .msg
        .result
        .expect("result");
    assert_eq!(res.error, "", "{:?}", res.logs);
    let out: serde_json::Value = serde_json::from_str(&res.output_json).expect("out");
    assert_eq!(out["approved"], false);
    let proposed_id = out["id"].as_str().expect("id").to_owned();
    let all = contexts
        .list_mappings(authed(
            &endpoint,
            ListMappingsRequest {
                space_id: work.clone(),
                approved_only: false,
                include_deleted: false,
            },
        ))
        .await
        .expect("list")
        .msg
        .mappings;
    assert_eq!(all.len(), 2);
    let pending = all.iter().find(|m| m.id == proposed_id).expect("proposed");
    assert!(!pending.approved);
    assert_eq!(pending.proposed_by, format!("script:{}", script.id));
    let hits = search
        .search(authed(
            &endpoint,
            SearchRequest {
                space_id: work.clone(),
                query: "urgent".into(),
                kind: 0,
                limit: 10,
                mapping_id: proposed_id.clone(),
            },
        ))
        .await
        .expect("search")
        .msg
        .hits;
    assert!(hits.is_empty(), "an unapproved mapping names no fields");
    let ctx = contexts
        .create_context(authed(
            &endpoint,
            CreateContextRequest {
                space_id: work.clone(),
                name: "all".into(),
                selection: Some(Selection::default()),
            },
        ))
        .await
        .expect("context")
        .msg
        .context
        .expect("context");
    let resolve = || {
        let req = authed(
            &endpoint,
            ResolveContextRequest {
                space_id: work.clone(),
                id: ctx.id.clone(),
                limit: 0,
            },
        );
        let contexts = contexts.clone();
        async move { contexts.resolve_context(req).await.expect("resolve").msg }
    };
    assert_eq!(resolve().await.mappings.len(), 1);
    contexts
        .approve_mapping(authed(
            &endpoint,
            ApproveMappingRequest {
                space_id: work.clone(),
                id: proposed_id.clone(),
                approved: true,
            },
        ))
        .await
        .expect("approve");
    let resolved = resolve().await;
    assert_eq!(resolved.mappings.len(), 2);
    assert!(resolved.mappings.iter().all(|m| m.approved));
    let approved = resolved
        .mappings
        .iter()
        .find(|m| m.id == proposed_id)
        .expect("approved");
    assert!(approved.approved_at_ms > 0);
    assert_eq!(approved.proposed_by, format!("script:{}", script.id));
}
