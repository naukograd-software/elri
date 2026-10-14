//! Scripts run in a sandbox with a manifest; grants gate what runs; rules
//! fire on changes and never on their own writes.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::ListChangesRequest;
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, ListObjectsRequest, ObjectKind, Value,
};
use elri_proto::elri::v1::script::grant_service_ws::GrantServiceClient;
use elri_proto::elri::v1::script::rule_service_ws::RuleServiceClient;
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{
    CreateRuleRequest, CreateScriptRequest, GrantScriptRequest, ListGrantsRequest,
    ListRuleRunsRequest, Manifest, RevokeGrantRequest, RunRuleRequest, RunScriptRequest,
    SetPolicyRequest, Trigger,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-scripts-{name}-{}", std::process::id()));
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

struct Sess {
    endpoint: Endpoint,
    spaces: SpaceServiceClient,
    objects: ObjectServiceClient,
    scripts: ScriptServiceClient,
    grants: GrantServiceClient,
    rules: RuleServiceClient,
    changes: ChangeServiceClient,
}

async fn open(dir: &std::path::Path) -> Sess {
    let endpoint = Endpoint::read(dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    Sess {
        endpoint,
        spaces: SpaceServiceClient::new(conn.clone()),
        objects: ObjectServiceClient::new(conn.clone()),
        scripts: ScriptServiceClient::new(conn.clone()),
        grants: GrantServiceClient::new(conn.clone()),
        rules: RuleServiceClient::new(conn.clone()),
        changes: ChangeServiceClient::new(conn),
    }
}

impl Sess {
    async fn space(&self, name: &str) -> String {
        self.spaces
            .create_space(authed(
                &self.endpoint,
                CreateSpaceRequest { name: name.into() },
            ))
            .await
            .expect("space")
            .msg
            .space
            .expect("space")
            .id
    }

    async fn script(
        &self,
        space: &str,
        name: &str,
        source: &str,
        manifest: Manifest,
    ) -> Result<elri_proto::elri::v1::script::Script, ws_proto::Status> {
        Ok(self
            .scripts
            .create_script(authed(
                &self.endpoint,
                CreateScriptRequest {
                    space_id: space.to_owned(),
                    name: name.to_owned(),
                    source: source.to_owned(),
                    manifest: Some(manifest),
                },
            ))
            .await?
            .msg
            .script
            .expect("script"))
    }

    async fn run(
        &self,
        space: &str,
        id: &str,
        args: &str,
    ) -> Result<elri_proto::elri::v1::script::RunResult, ws_proto::Status> {
        Ok(self
            .scripts
            .run_script(authed(
                &self.endpoint,
                RunScriptRequest {
                    space_id: space.to_owned(),
                    id: id.to_owned(),
                    args_json: args.to_owned(),
                },
            ))
            .await?
            .msg
            .result
            .expect("result"))
    }
}

#[tokio::test]
async fn scripts_run_within_their_manifest_and_grants() {
    let dir = tmp_dir("run");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let c = open(&dir).await;
    let home = c.space("home").await;
    let other = c.space("other").await;

    // A benign script: reads and writes its own Space, returns a value.
    let src = r#"
        -- The word comes from the arguments so the script's own source,
        -- which is searchable text too, does not match.
        local made = elri.create({ kind = "entity", types = {"note"}, fields = { title = elri.args.word .. " note", n = 3 } })
        elri.log("created", made.id)
        local all = elri.list({ kind = "entity" })
        local found = elri.search(elri.args.word)
        elri.update(made.id, { fields = { n = elri.args.bump + made.fields.n } })
        local again = elri.get(made.id)
        return { count = #all, hits = #found, n = again.fields.n, title = again.fields.title }
    "#;
    let script = c
        .script(&home, "maker", src, Manifest::default())
        .await
        .expect("create");
    assert!(script.benign);
    assert_eq!(script.version.len(), 64);
    let result = c
        .run(&home, &script.id, r#"{"bump": 4, "word": "zebra"}"#)
        .await
        .expect("run");
    assert_eq!(result.error, "", "logs: {:?}", result.logs);
    let out: serde_json::Value = serde_json::from_str(&result.output_json).expect("json");
    assert_eq!(out["count"], 1);
    assert_eq!(out["hits"], 1);
    assert_eq!(out["n"], 7);
    assert_eq!(out["title"], "zebra note");
    assert_eq!(result.labels, vec![home.clone()]);
    assert!(result.logs[0].starts_with("created "));
    assert!(!result.grant_id.is_empty(), "auto-granted as benign");
    let listed = c
        .grants
        .list_grants(authed(&c.endpoint, ListGrantsRequest::default()))
        .await
        .expect("grants")
        .msg
        .grants;
    assert_eq!(listed.len(), 1);
    assert!(listed[0].benign);

    // The Change the script made is attributed to it.
    let changes = c
        .changes
        .list_changes(authed(
            &c.endpoint,
            ListChangesRequest {
                space_id: home.clone(),
                limit: 10,
                object_id: String::new(),
            },
        ))
        .await
        .expect("changes")
        .msg
        .changes;
    assert!(changes
        .iter()
        .any(|ch| ch.actor == format!("script:{}", script.id)));

    // Reading another Space without declaring it is denied at run time.
    let peek = format!(r#"return #elri.list(nil, "{other}")"#);
    let peeker = c
        .script(&home, "peeker", &peek, Manifest::default())
        .await
        .expect("create");
    let result = c.run(&home, &peeker.id, "").await.expect("run");
    assert!(
        result.error.contains("capability denied"),
        "{}",
        result.error
    );

    // Declaring it makes the script non-benign: a grant is required, then
    // it works and the run carries both labels.
    let declared = c
        .script(
            &home,
            "reader",
            &peek,
            Manifest {
                reads: vec![other.clone()],
                ..Default::default()
            },
        )
        .await
        .expect("create");
    assert!(!declared.benign);
    let denied = c.run(&home, &declared.id, "").await.expect_err("no grant");
    assert_eq!(denied.code, Code::PermissionDenied);
    let grant = c
        .grants
        .grant_script(authed(
            &c.endpoint,
            GrantScriptRequest {
                expected_version: String::new(),
                space_id: home.clone(),
                script_id: declared.id.clone(),
            },
        ))
        .await
        .expect("grant")
        .msg
        .grant
        .expect("grant");
    assert!(!grant.benign);
    let result = c.run(&home, &declared.id, "").await.expect("run");
    assert_eq!(result.error, "");
    assert_eq!(result.output_json, "0");
    assert_eq!(result.labels, vec![other.clone()]);
    c.grants
        .revoke_grant(authed(
            &c.endpoint,
            RevokeGrantRequest {
                id: grant.id.clone(),
            },
        ))
        .await
        .expect("revoke");
    let denied = c.run(&home, &declared.id, "").await.expect_err("revoked");
    assert_eq!(denied.code, Code::PermissionDenied);

    // With auto-grant off, even benign scripts need consent.
    c.grants
        .set_policy(authed(
            &c.endpoint,
            SetPolicyRequest {
                max_concurrent_scripts: 0,
                auto_grant_benign: false,
            },
        ))
        .await
        .expect("policy");
    let strict = c
        .script(&home, "strict", "return 1", Manifest::default())
        .await
        .expect("create");
    let denied = c.run(&home, &strict.id, "").await.expect_err("ask first");
    assert_eq!(denied.code, Code::PermissionDenied);

    // Bad source is refused at creation; a runaway loop hits the limit.
    let bad = c
        .script(&home, "bad", "return )", Manifest::default())
        .await
        .expect_err("syntax");
    assert_eq!(bad.code, Code::InvalidArgument);
    c.grants
        .set_policy(authed(
            &c.endpoint,
            SetPolicyRequest {
                max_concurrent_scripts: 0,
                auto_grant_benign: true,
            },
        ))
        .await
        .expect("policy");
    let spin = c
        .script(&home, "spin", "while true do end", Manifest::default())
        .await
        .expect("create");
    let result = c.run(&home, &spin.id, "").await.expect("run");
    assert!(result.error.contains("time limit"), "{}", result.error);
}

#[tokio::test]
async fn rules_fire_on_user_changes_but_not_on_their_own() {
    let dir = tmp_dir("rules");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let c = open(&dir).await;
    let home = c.space("home").await;

    // The rule tags every new task with a "seen" field; if it fired on its
    // own write it would loop, which the depth-one rule prevents.
    let src = r#"
        local ev = elri.args.event
        local o = elri.get(ev.object_id)
        if o.fields.seen then return "already" end
        elri.update(ev.object_id, { fields = { seen = true, seen_by = elri.args.rule } })
        elri.notify("tagged " .. ev.object_id)
        return "tagged"
    "#;
    let script = c
        .script(&home, "tagger", src, Manifest::default())
        .await
        .expect("create");
    let rule = c
        .rules
        .create_rule(authed(
            &c.endpoint,
            CreateRuleRequest {
                notify: 0,
                space_id: home.clone(),
                name: "tag tasks".into(),
                script_id: script.id.clone(),
                trigger: Some(Trigger {
                    kinds: vec![ObjectKind::Entity.into()],
                    type_id: "task".into(),
                    every_seconds: 0,
                    on_events: true,
                    missed: 0,
                }),
                executor_device_id: String::new(),
            },
        ))
        .await
        .expect("rule")
        .msg
        .rule
        .expect("rule");
    assert!(rule.enabled);
    assert!(!rule.executor_device_id.is_empty());

    let mut fields = HashMap::new();
    fields.insert("title".to_owned(), s("buy milk"));
    let task = c
        .objects
        .create_object(authed(
            &c.endpoint,
            CreateObjectRequest {
                space_id: home.clone(),
                kind: ObjectKind::Entity.into(),
                types: vec!["task".into()],
                fields,
                ..Default::default()
            },
        ))
        .await
        .expect("task")
        .msg
        .object
        .expect("object");
    let note = c
        .objects
        .create_object(authed(
            &c.endpoint,
            CreateObjectRequest {
                space_id: home.clone(),
                kind: ObjectKind::Entity.into(),
                types: vec!["note".into()],
                ..Default::default()
            },
        ))
        .await
        .expect("note")
        .msg
        .object
        .expect("object");

    // The task gets tagged by the rule; the note is not a task.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let objs = c
            .objects
            .list_objects(authed(
                &c.endpoint,
                ListObjectsRequest {
                    space_id: home.clone(),
                    ..Default::default()
                },
            ))
            .await
            .expect("list")
            .msg
            .objects;
        let tagged = objs
            .iter()
            .find(|o| o.id == task.id)
            .is_some_and(|o| o.fields.contains_key("seen"));
        if tagged {
            let t = objs.iter().find(|o| o.id == task.id).expect("task");
            assert_eq!(t.fields["seen_by"], s(&rule.id));
            assert!(!objs
                .iter()
                .find(|o| o.id == note.id)
                .expect("note")
                .fields
                .contains_key("seen"));
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "rule never fired");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Give a would-be loop time to show; the rule's own write must not
    // fire it again, so the task has exactly two Changes: create + tag.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let changes = c
        .changes
        .list_changes(authed(
            &c.endpoint,
            ListChangesRequest {
                space_id: home.clone(),
                limit: 50,
                object_id: String::new(),
            },
        ))
        .await
        .expect("changes")
        .msg
        .changes;
    let on_task: Vec<_> = changes
        .iter()
        .filter(|ch| ch.parts.iter().any(|p| p.object_id == task.id))
        .collect();
    assert_eq!(on_task.len(), 2, "{on_task:?}");
    assert!(on_task
        .iter()
        .any(|ch| ch.actor == format!("rule:{}/script:{}", rule.id, script.id)));
}

#[tokio::test]
async fn scheduled_rules_run_and_record_their_runs() {
    let dir = tmp_dir("schedule");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let c = open(&dir).await;
    let home = c.space("home").await;
    let script = c
        .script(
            &home,
            "ticker",
            r#"elri.create({ fields = { tick = elri.args.schedule.at } }); return "ticked""#,
            Manifest::default(),
        )
        .await
        .expect("create");
    let rule = c
        .rules
        .create_rule(authed(
            &c.endpoint,
            CreateRuleRequest {
                notify: 0,
                space_id: home.clone(),
                name: "every second".into(),
                script_id: script.id.clone(),
                trigger: Some(Trigger {
                    every_seconds: 1,
                    on_events: false,
                    ..Default::default()
                }),
                executor_device_id: String::new(),
            },
        ))
        .await
        .expect("rule")
        .msg
        .rule
        .expect("rule");
    assert_eq!(rule.trigger.as_ref().expect("trigger").every_seconds, 1);

    // The first scheduled run happens right after creation; it is recorded.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let runs = loop {
        let runs = c
            .rules
            .list_rule_runs(authed(
                &c.endpoint,
                ListRuleRunsRequest {
                    space_id: home.clone(),
                    id: rule.id.clone(),
                    limit: 0,
                },
            ))
            .await
            .expect("runs")
            .msg
            .runs;
        if !runs.is_empty() {
            break runs;
        }
        assert!(tokio::time::Instant::now() < deadline, "never ran");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(runs[0].cause, "schedule");
    assert_eq!(runs[0].error, "");
    let objs = c
        .objects
        .list_objects(authed(
            &c.endpoint,
            ListObjectsRequest {
                space_id: home.clone(),
                kind: ObjectKind::Entity.into(),
                ..Default::default()
            },
        ))
        .await
        .expect("list")
        .msg
        .objects;
    assert_eq!(objs.len(), 1, "the scheduled run created one object");
    assert!(objs[0].fields.contains_key("tick"));

    // Schedule-only: a user write does not fire it.
    c.objects
        .create_object(authed(
            &c.endpoint,
            CreateObjectRequest {
                space_id: home.clone(),
                kind: ObjectKind::Entity.into(),
                ..Default::default()
            },
        ))
        .await
        .expect("object");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let runs = c
        .rules
        .list_rule_runs(authed(
            &c.endpoint,
            ListRuleRunsRequest {
                space_id: home.clone(),
                id: rule.id.clone(),
                limit: 0,
            },
        ))
        .await
        .expect("runs")
        .msg
        .runs;
    assert_eq!(runs.len(), 1);

    // A manual run happens regardless of the trigger and is recorded.
    let manual = c
        .rules
        .run_rule(authed(
            &c.endpoint,
            RunRuleRequest {
                space_id: home.clone(),
                id: rule.id.clone(),
            },
        ))
        .await
        .expect("run")
        .msg
        .run
        .expect("run");
    assert_eq!(manual.cause, "manual");
    assert_eq!(manual.error, "");
    let runs = c
        .rules
        .list_rule_runs(authed(
            &c.endpoint,
            ListRuleRunsRequest {
                space_id: home.clone(),
                id: rule.id.clone(),
                limit: 0,
            },
        ))
        .await
        .expect("runs")
        .msg
        .runs;
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].cause, "manual");

    // An Object's history names the Changes that touched it, newest first.
    let history = c
        .changes
        .list_changes(authed(
            &c.endpoint,
            ListChangesRequest {
                space_id: home.clone(),
                limit: 10,
                object_id: objs[0].id.clone(),
            },
        ))
        .await
        .expect("history")
        .msg
        .changes;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].actor,
        format!("rule:{}/script:{}", rule.id, script.id)
    );
}
