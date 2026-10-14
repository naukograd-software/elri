//! Semantic search through the API: the index fills in the background,
//! the watermark says how complete it is, and hybrid search fuses words
//! and meaning. The hashing embedder stands in for the model here.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use elri_core::semantic::Backend;
use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, ObjectKind, Value,
};
use elri_proto::elri::v1::search::search_service_ws::SearchServiceClient;
use elri_proto::elri::v1::search::{
    GetSemanticStatusRequest, SearchMode, SearchRequest, SemanticState,
};
use elri_proto::elri::v1::settings::settings_service_ws::SettingsServiceClient;
use elri_proto::elri::v1::settings::{SemanticSettings, UpdateSettingsRequest};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

struct Authed(String);

impl Authed {
    fn req<T>(&self, msg: T) -> Request<T> {
        Request::new(msg).with_header("authorization", self.0.clone())
    }
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn meaning_search_fills_in_and_reports_its_completeness() {
    let dir = std::env::temp_dir().join(format!("elri-semantic-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut config = Config::new(
        &dir,
        BuildInfo {
            version: "test",
            commit: "",
        },
    );
    config.semantic = Backend::Hashing;
    let _core = Core::start(config).await.expect("core");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let a = Authed(format!("Bearer {}", endpoint.token));
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let search = SearchServiceClient::new(conn.clone());
    let settings = SettingsServiceClient::new(conn);
    let space = spaces
        .create_space(a.req(CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    let create = |title: &str| {
        let mut fields = HashMap::new();
        fields.insert("title".to_owned(), s(title));
        let req = a.req(CreateObjectRequest {
            space_id: space.clone(),
            kind: ObjectKind::Entity.into(),
            fields,
            ..Default::default()
        });
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
    let graphs = create("planning my studies with graph theory").await;
    let soup = create("recipe for beetroot soup").await;
    let gym = create("deadlift and squat workout").await;

    // The index fills in by itself; the status and the watermark show it.
    let mut ready = false;
    for _ in 0..100 {
        let st = search
            .get_semantic_status(a.req(GetSemanticStatusRequest {}))
            .await
            .expect("status")
            .msg
            .status
            .expect("status");
        if st.state() == SemanticState::Ready && st.pending == 0 {
            assert_eq!(st.model, "hashing-v1");
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "the semantic index never caught up");

    let query = |q: &str, mode: SearchMode| {
        let req = a.req(SearchRequest {
            space_id: space.clone(),
            query: q.to_owned(),
            kind: 0,
            limit: 10,
            mapping_id: String::new(),
            mode: mode.into(),
        });
        let search = search.clone();
        async move { search.search(req).await }
    };
    // Different words, shared word forms: meaning finds the study note;
    // words alone find nothing.
    let res = query("graphs for study planning", SearchMode::Semantic)
        .await
        .expect("semantic")
        .msg;
    let first = res.hits[0].object.as_ref().expect("object");
    assert_eq!(first.id, graphs.id);
    assert_eq!(res.hits[0].reasons, vec!["meaning".to_owned()]);
    assert!(res.hits[0].similarity > 0.0);
    let wm = res.watermark.expect("watermark");
    assert_eq!(wm.semantic_pending, 0);
    assert_eq!(wm.semantic_state(), SemanticState::Ready);

    // Hybrid: an exact word plus meaning; the object found both ways
    // carries both reasons and ranks first.
    let res = query("workout planning", SearchMode::Hybrid)
        .await
        .expect("hybrid")
        .msg;
    let ids: Vec<&str> = res
        .hits
        .iter()
        .filter_map(|h| h.object.as_ref().map(|o| o.id.as_str()))
        .collect();
    assert!(ids.contains(&gym.id.as_str()));
    let gym_hit = res
        .hits
        .iter()
        .find(|h| h.object.as_ref().is_some_and(|o| o.id == gym.id))
        .expect("gym");
    assert!(gym_hit.reasons.contains(&"words".to_owned()));
    assert!(gym_hit.reasons.contains(&"meaning".to_owned()));
    assert_eq!(res.hits[0].object.as_ref().expect("o").id, gym.id);

    // A deleted object leaves meaning search.
    objects
        .delete_object(a.req(DeleteObjectRequest {
            space_id: space.clone(),
            id: soup.id.clone(),
        }))
        .await
        .expect("delete");
    let res = query("beetroot soup recipe", SearchMode::Semantic)
        .await
        .expect("semantic")
        .msg;
    assert!(res
        .hits
        .iter()
        .all(|h| h.object.as_ref().is_some_and(|o| o.id != soup.id)));

    // Switched off: meaning search refuses, words still work.
    settings
        .update_settings(a.req(UpdateSettingsRequest {
            proxy: None,
            semantic: Some(SemanticSettings { enabled: false }),
            ai: None,
        }))
        .await
        .expect("off");
    let err = query("graphs", SearchMode::Semantic)
        .await
        .expect_err("off");
    assert_eq!(err.code, Code::FailedPrecondition);
    query("graph", SearchMode::Lexical)
        .await
        .expect("lexical still works");
}
