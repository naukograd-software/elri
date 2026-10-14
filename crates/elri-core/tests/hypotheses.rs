//! Hypotheses: an agent proposes a Layer with its provenance; only that
//! agent revises it; a person reviews it without displacing the author.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, GetObjectRequest, HypothesisStatus, ListHypothesesRequest,
    ObjectKind, ReviewHypothesisRequest, UpdateObjectRequest, Value,
};
use elri_proto::elri::v1::script::grant_service_ws::GrantServiceClient;
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{
    CreateScriptRequest, GrantScriptRequest, Manifest, RunScriptRequest,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-hypotheses-{name}-{}", std::process::id()));
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
async fn agents_propose_and_people_review() {
    let dir = tmp_dir("review");
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
    let scripts = ScriptServiceClient::new(conn.clone());
    let grants = GrantServiceClient::new(conn);
    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    let mut fields = HashMap::new();
    fields.insert("title".to_owned(), s("graphs and planning"));
    let note = objects
        .create_object(authed(
            &endpoint,
            CreateObjectRequest {
                space_id: space.clone(),
                kind: ObjectKind::Entity.into(),
                fields,
                ..Default::default()
            },
        ))
        .await
        .expect("note")
        .msg
        .object
        .expect("object");

    // A person may not write reserved fields through the ordinary API.
    let mut forged = HashMap::new();
    forged.insert("$agent".to_owned(), s("script:fake"));
    let err = objects
        .update_object(authed(
            &endpoint,
            UpdateObjectRequest {
                space_id: space.clone(),
                id: note.id.clone(),
                set_fields: forged,
                ..Default::default()
            },
        ))
        .await
        .expect_err("reserved");
    assert_eq!(err.code, Code::InvalidArgument);

    // An agent proposes a hypothesis over the note, naming its model,
    // sources, and reason; it may revise its own proposal.
    let make_script = |name: &str, source: &str| {
        let req = authed(
            &endpoint,
            CreateScriptRequest {
                space_id: space.clone(),
                name: name.to_owned(),
                source: source.to_owned(),
                manifest: Some(Manifest::default()),
            },
        );
        let scripts = scripts.clone();
        let grants = grants.clone();
        let endpoint = endpoint.clone();
        let space = space.clone();
        async move {
            let script = scripts
                .create_script(req)
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
                        space_id: space,
                        script_id: script.id.clone(),
                    },
                ))
                .await
                .expect("grant");
            script
        }
    };
    let run = |id: &str, args: &str| {
        let req = authed(
            &endpoint,
            RunScriptRequest {
                space_id: space.clone(),
                id: id.to_owned(),
                args_json: args.to_owned(),
            },
        );
        let scripts = scripts.clone();
        async move {
            scripts
                .run_script(req)
                .await
                .expect("run")
                .msg
                .result
                .expect("result")
        }
    };
    let proposer = make_script(
        "proposer",
        r#"
            local h = elri.create({
                kind = "layer", target = elri.args.target,
                fields = { topic = "graph theory" },
                hypothesis = { model = "local-mini-1", sources = { elri.args.target }, basis = "the title mentions graphs" },
            })
            elri.update(h.id, { fields = { topic = "graph theory, revised" } })
            return h.id
        "#,
    )
    .await;
    let res = run(&proposer.id, &format!(r#"{{"target":"{}"}}"#, note.id)).await;
    assert_eq!(res.error, "", "{:?}", res.logs);
    let layer_id: String = serde_json::from_str(&res.output_json).expect("id");

    let list = |status: Option<HypothesisStatus>| {
        let req = authed(
            &endpoint,
            ListHypothesesRequest {
                space_id: space.clone(),
                target_id: note.id.clone(),
                status: status.map_or(0, i32::from),
                limit: 0,
            },
        );
        let objects = objects.clone();
        async move {
            objects
                .list_hypotheses(req)
                .await
                .expect("list")
                .msg
                .hypotheses
        }
    };
    let open = list(Some(HypothesisStatus::Proposed)).await;
    assert_eq!(open.len(), 1);
    let h = open[0].hypothesis.clone().expect("hypothesis");
    assert_eq!(h.agent, format!("script:{}", proposer.id));
    assert_eq!(h.model, "local-mini-1");
    assert_eq!(h.sources, vec![note.id.clone()]);
    assert_eq!(h.basis, "the title mentions graphs");
    assert_eq!(h.status(), HypothesisStatus::Proposed);
    assert_eq!(h.reviewed_by, "");
    let layer = open[0].layer.clone().expect("layer");
    assert_eq!(layer.id, layer_id);
    assert_eq!(layer.fields["topic"], s("graph theory, revised"));
    assert_eq!(layer.target_id.as_deref(), Some(note.id.as_str()));

    // Another agent may not revise or delete it; a person may not forge
    // its fields.
    let meddler = make_script(
        "meddler",
        r#"
            local ok, err = pcall(function()
                elri.update(elri.args.layer, { fields = { topic = "mine now" } })
            end)
            local ok2, err2 = pcall(function() elri.delete(elri.args.layer) end)
            return { updated = ok, deleted = ok2, err = tostring(err) }
        "#,
    )
    .await;
    let res = run(&meddler.id, &format!(r#"{{"layer":"{layer_id}"}}"#)).await;
    assert_eq!(res.error, "", "{:?}", res.logs);
    let out: serde_json::Value = serde_json::from_str(&res.output_json).expect("out");
    assert_eq!(out["updated"], false);
    assert_eq!(out["deleted"], false);
    assert!(
        out["err"].as_str().expect("err").contains("belongs to"),
        "{out}"
    );

    // A person confirms: the status and reviewer are recorded, the author
    // stays the agent, the Layer's own fields are untouched.
    let reviewed = objects
        .review_hypothesis(authed(
            &endpoint,
            ReviewHypothesisRequest {
                space_id: space.clone(),
                id: layer_id.clone(),
                status: HypothesisStatus::Confirmed.into(),
                note: "checked the source".into(),
            },
        ))
        .await
        .expect("review")
        .msg
        .hypothesis
        .expect("hypothesis");
    let h = reviewed.hypothesis.expect("hypothesis");
    assert_eq!(h.status(), HypothesisStatus::Confirmed);
    assert_eq!(h.reviewed_by, "user");
    assert_eq!(h.note, "checked the source");
    assert!(h.reviewed_at_ms > 0);
    assert_eq!(h.agent, format!("script:{}", proposer.id));
    assert!(list(Some(HypothesisStatus::Proposed)).await.is_empty());
    assert_eq!(list(Some(HypothesisStatus::Confirmed)).await.len(), 1);

    // Reviewing an ordinary Object is refused; a rejected hypothesis is a
    // deleted Layer, listed only when asked for rejected ones.
    let err = objects
        .review_hypothesis(authed(
            &endpoint,
            ReviewHypothesisRequest {
                space_id: space.clone(),
                id: note.id.clone(),
                status: HypothesisStatus::Rejected.into(),
                note: String::new(),
            },
        ))
        .await
        .expect_err("not a hypothesis");
    assert_eq!(err.code, Code::NotFound);
    let res = run(&proposer.id, &format!(r#"{{"target":"{}"}}"#, note.id)).await;
    assert_eq!(res.error, "", "{:?}", res.logs);
    let second: String = serde_json::from_str(&res.output_json).expect("id");
    objects
        .review_hypothesis(authed(
            &endpoint,
            ReviewHypothesisRequest {
                space_id: space.clone(),
                id: second.clone(),
                status: HypothesisStatus::Rejected.into(),
                note: "no".into(),
            },
        ))
        .await
        .expect("reject");
    assert_eq!(list(None).await.len(), 1, "the rejected one is deleted");
    let rejected = list(Some(HypothesisStatus::Rejected)).await;
    assert_eq!(rejected.len(), 1);
    assert!(rejected[0]
        .layer
        .as_ref()
        .expect("layer")
        .deleted_at_ms
        .is_some());
    let gone = objects
        .get_object(authed(
            &endpoint,
            GetObjectRequest {
                space_id: space.clone(),
                id: second,
            },
        ))
        .await
        .expect("get")
        .msg
        .object
        .expect("object");
    assert!(gone.deleted_at_ms.is_some());
}
