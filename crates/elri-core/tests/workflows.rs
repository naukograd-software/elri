//! Durable workflows: steps survive a failure and are not repeated on
//! resume; an unknown outcome pauses the run until the user resolves it.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, ListObjectsRequest, ObjectKind, UpdateObjectRequest, Value,
};
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{CreateScriptRequest, Manifest};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use elri_proto::elri::v1::workflow::workflow_service_ws::WorkflowServiceClient;
use elri_proto::elri::v1::workflow::{
    resolve_workflow_step_request, GetWorkflowRunRequest, ListWorkflowRunsRequest,
    ResolveWorkflowStepRequest, ResumePolicy, ResumeWorkflowRunRequest, RunStatus,
    StartWorkflowRequest, StepStatus, WorkflowRun,
};
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-workflows-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn b(v: bool) -> Value {
    Value {
        value: Some(value::Value::Bool(v)),
    }
}

struct Sess {
    endpoint: Endpoint,
    spaces: SpaceServiceClient,
    objects: ObjectServiceClient,
    scripts: ScriptServiceClient,
    workflows: WorkflowServiceClient,
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
        workflows: WorkflowServiceClient::new(conn),
    }
}

impl Sess {
    async fn run(&self, id: &str) -> WorkflowRun {
        self.workflows
            .get_workflow_run(authed(
                &self.endpoint,
                GetWorkflowRunRequest { id: id.into() },
            ))
            .await
            .expect("get")
            .msg
            .run
            .expect("run")
    }

    async fn wait_until(&self, id: &str, status: RunStatus) -> WorkflowRun {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let run = self.run(id).await;
            if run.status() == status {
                return run;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "run never became {status:?}: {run:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn made(&self, space: &str) -> usize {
        self.objects
            .list_objects(authed(
                &self.endpoint,
                ListObjectsRequest {
                    space_id: space.to_owned(),
                    kind: ObjectKind::Entity.into(),
                    ..Default::default()
                },
            ))
            .await
            .expect("list")
            .msg
            .objects
            .iter()
            .filter(|o| o.fields.contains_key("made"))
            .count()
    }
}

fn step<'a>(run: &'a WorkflowRun, name: &str) -> &'a elri_proto::elri::v1::workflow::WorkflowStep {
    run.steps
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no step {name} in {run:?}"))
}

#[tokio::test]
async fn a_failed_run_resumes_without_repeating_its_finished_steps() {
    let dir = tmp_dir("resume");
    let _core = Core::start(Config::new(
        &dir,
        BuildInfo {
            version: "test",
            commit: "",
        },
    ))
    .await
    .expect("start");
    let c = open(&dir).await;
    let space = c
        .spaces
        .create_space(authed(
            &c.endpoint,
            CreateSpaceRequest {
                name: "home".into(),
            },
        ))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    let mut fields = HashMap::new();
    fields.insert("go".to_owned(), b(false));
    let flag = c
        .objects
        .create_object(authed(
            &c.endpoint,
            CreateObjectRequest {
                space_id: space.clone(),
                kind: ObjectKind::Entity.into(),
                fields,
                ..Default::default()
            },
        ))
        .await
        .expect("flag")
        .msg
        .object
        .expect("object");
    let script = c
        .scripts
        .create_script(authed(
            &c.endpoint,
            CreateScriptRequest {
                space_id: space.clone(),
                name: "steps".into(),
                source: r#"
                    local n = elri.step("count", function() return 41 + 1 end)
                    local made = elri.step("make", function()
                        local o = elri.create({ fields = { made = true } })
                        return o.id
                    end)
                    elri.step("check", function()
                        local flag = elri.get(elri.args.flag)
                        if not flag.fields.go then error("not yet") end
                        return "ok"
                    end)
                    return { n = n, made = made }
                "#
                .into(),
                manifest: Some(Manifest::default()),
            },
        ))
        .await
        .expect("script")
        .msg
        .script
        .expect("script");

    let run = c
        .workflows
        .start_workflow(authed(
            &c.endpoint,
            StartWorkflowRequest {
                space_id: space.clone(),
                script_id: script.id.clone(),
                args_json: format!(r#"{{"flag":"{}"}}"#, flag.id),
                resume: ResumePolicy::Ask.into(),
            },
        ))
        .await
        .expect("start")
        .msg
        .run
        .expect("run");
    assert_eq!(run.version, script.version);

    // The third step raises: the run fails with the first two recorded.
    let failed = c.wait_until(&run.id, RunStatus::Failed).await;
    assert!(failed.reason.contains("not yet"), "{}", failed.reason);
    assert_eq!(step(&failed, "count").status(), StepStatus::Done);
    assert_eq!(step(&failed, "count").result_json, "42");
    assert_eq!(step(&failed, "make").status(), StepStatus::Done);
    assert_eq!(step(&failed, "check").status(), StepStatus::Failed);
    assert_eq!(c.made(&space).await, 1);

    // Fix the cause and resume: finished steps are reused, nothing is made
    // twice, the run completes.
    let mut set = HashMap::new();
    set.insert("go".to_owned(), b(true));
    c.objects
        .update_object(authed(
            &c.endpoint,
            UpdateObjectRequest {
                space_id: space.clone(),
                id: flag.id.clone(),
                set_fields: set,
                ..Default::default()
            },
        ))
        .await
        .expect("update");
    c.workflows
        .resume_workflow_run(authed(
            &c.endpoint,
            ResumeWorkflowRunRequest { id: run.id.clone() },
        ))
        .await
        .expect("resume");
    let done = c.wait_until(&run.id, RunStatus::Done).await;
    assert_eq!(step(&done, "check").status(), StepStatus::Done);
    assert_eq!(c.made(&space).await, 1, "the make step did not run again");
    let out: serde_json::Value = serde_json::from_str(&done.output_json).expect("output");
    assert_eq!(out["n"], 42);
    assert_eq!(
        out["made"].as_str().expect("id"),
        step(&done, "make").result_json.trim_matches('"')
    );

    // A finished run neither resumes nor cancels; resolving a done step is
    // refused; the listing shows it.
    let err = c
        .workflows
        .resume_workflow_run(authed(
            &c.endpoint,
            ResumeWorkflowRunRequest { id: run.id.clone() },
        ))
        .await
        .expect_err("done runs do not resume");
    assert_eq!(err.code, Code::FailedPrecondition);
    let err = c
        .workflows
        .resolve_workflow_step(authed(
            &c.endpoint,
            ResolveWorkflowStepRequest {
                id: run.id.clone(),
                step: "count".into(),
                outcome: Some(resolve_workflow_step_request::Outcome::Retry(true)),
            },
        ))
        .await
        .expect_err("done steps are not resolved");
    assert_eq!(err.code, Code::FailedPrecondition);
    let runs = c
        .workflows
        .list_workflow_runs(authed(
            &c.endpoint,
            ListWorkflowRunsRequest {
                space_id: space.clone(),
                script_id: script.id.clone(),
                limit: 0,
            },
        ))
        .await
        .expect("list")
        .msg
        .runs;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status(), RunStatus::Done);
}
