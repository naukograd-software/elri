//! Durable workflows: a script run whose steps are recorded on the device
//! so it can continue after an interruption. The script marks steps with
//! `elri.step`; the host writes the intent before a step and the result
//! after. Resuming reuses recorded results and goes on from the first
//! step without one. A step the device stopped in the middle of has an
//! unknown outcome: the run pauses until the user says what happened, so
//! an action with effects outside the system is never repeated blindly.

use std::sync::Arc;

use elri_proto::elri::v1::events::{event, NoticeEvent};
use elri_proto::elri::v1::object::ObjectKind;
use elri_proto::elri::v1::workflow::{ResumePolicy, WorkflowRun};
use uuid::Uuid;

use crate::model::{ObjectId, SpaceId};
use crate::rules::grant_or_auto;
use crate::script::{self, Host, RunIdentity, ScriptError};
use crate::store::workflows::{status, step};
use crate::store::Store;

/// Starts `script_id` as a workflow run and returns at once; the run
/// proceeds in the background.
pub async fn start(
    store: &Arc<Store>,
    host: &Host,
    space_id: SpaceId,
    script_id: ObjectId,
    args: serde_json::Value,
    resume: ResumePolicy,
) -> Result<WorkflowRun, ScriptError> {
    let space = store.space(space_id).await?;
    let o = space.get(script_id).await?;
    if o.kind() != ObjectKind::Script {
        return Err(crate::store::StoreError::ObjectNotFound(script_id).into());
    }
    let s = script::script_of(&o)?;
    if grant_or_auto(store, space_id, &s).await?.is_none() {
        return Err(ScriptError::Denied(
            "this version of the script has no grant on this device".to_owned(),
        ));
    }
    let args_json = if args.is_null() {
        String::new()
    } else {
        args.to_string()
    };
    let resume = match resume {
        ResumePolicy::Auto => ResumePolicy::Auto,
        ResumePolicy::Unspecified | ResumePolicy::Ask => ResumePolicy::Ask,
    };
    let run = store
        .workflow_create(space_id, script_id, &s.version, &args_json, resume)
        .await?;
    spawn(Arc::clone(store), host.clone(), run.id.clone());
    Ok(run)
}

fn spawn(store: Arc<Store>, host: Host, id: String) {
    tokio::spawn(async move {
        if let Err(e) = execute(&store, &host, &id).await {
            tracing::warn!(run = %id, error = %e, "workflow run failed to execute");
        }
    });
}

/// Runs (or continues) a run that is marked running.
async fn execute(store: &Arc<Store>, host: &Host, id: &str) -> Result<(), ScriptError> {
    let Some(run) = store.workflow_run(id).await? else {
        return Ok(());
    };
    let space_id = ObjectId::parse(&run.space_id).map_err(crate::store::StoreError::from)?;
    let script_id = ObjectId::parse(&run.script_id).map_err(crate::store::StoreError::from)?;
    let run_id = Uuid::parse_str(&run.id).map_err(|_| ScriptError::Workflow("run id".into()))?;
    let pause = |reason: String| async move {
        store
            .workflow_set_status(id, status::PAUSED, &reason, None)
            .await?;
        notify(store, space_id, id, &reason);
        Ok::<(), ScriptError>(())
    };
    let Ok(space) = store.space(space_id).await else {
        return pause("the space is gone".to_owned()).await;
    };
    let s = match space.get(script_id).await {
        Ok(o) if o.kind() == ObjectKind::Script => script::script_of(&o)?,
        _ => return pause("the script is gone".to_owned()).await,
    };
    // The run is pinned to the script version it started with: a changed
    // script may mean different steps, so the user decides.
    if s.version != run.version {
        return pause(
            "the script changed since the run started; resume to continue with the new version"
                .to_owned(),
        )
        .await;
    }
    let Some(grant) = grant_or_auto(store, space_id, &s).await? else {
        return pause("this version of the script has no grant on this device".to_owned()).await;
    };
    let args: serde_json::Value = if run.args_json.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&run.args_json).unwrap_or(serde_json::Value::Null)
    };
    let result = host
        .run_workflow(
            space_id,
            &s,
            args,
            RunIdentity {
                script_id: s.id.clone(),
                rule_id: None,
                grant_id: grant.id,
            },
            run_id,
        )
        .await;
    if result.error.is_empty() {
        store
            .workflow_set_status(id, status::DONE, "", Some(&result.output_json))
            .await?;
    } else if !store
        .workflow_steps_in(run_id, step::UNKNOWN)
        .await?
        .is_empty()
    {
        pause(result.error).await?;
    } else {
        store
            .workflow_set_status(id, status::FAILED, &result.error, None)
            .await?;
        notify(store, space_id, id, &result.error);
    }
    Ok(())
}

fn notify(store: &Store, space_id: SpaceId, id: &str, text: &str) {
    store.emit(event::Event::Notice(NoticeEvent {
        space_id: space_id.to_string(),
        source: format!("workflow:{id}"),
        text: text.to_owned(),
    }));
}

/// At start: every run left running was interrupted. A step caught
/// between intent and result becomes unknown and pauses the run; the rest
/// continue by themselves or wait, as their policy says.
pub async fn recover(store: &Arc<Store>, host: &Host) -> Result<(), ScriptError> {
    for id in store.workflow_runs_in(status::RUNNING).await? {
        let Some(run) = store.workflow_run(&id).await? else {
            continue;
        };
        let run_id =
            Uuid::parse_str(&run.id).map_err(|_| ScriptError::Workflow("run id".into()))?;
        let open = store.workflow_steps_in(run_id, step::INTENT).await?;
        let space_id = ObjectId::parse(&run.space_id).map_err(crate::store::StoreError::from)?;
        if let Some(name) = open.first() {
            store
                .workflow_step_finish(run_id, name, step::UNKNOWN, "", "interrupted")
                .await?;
            let reason = format!(
                "step {name}: the device stopped before its result was recorded; say what happened before resuming"
            );
            store
                .workflow_set_status(&id, status::PAUSED, &reason, None)
                .await?;
            notify(store, space_id, &id, &reason);
        } else if run.resume() == ResumePolicy::Auto {
            spawn(Arc::clone(store), host.clone(), id);
        } else {
            let reason = "interrupted; resume to continue".to_owned();
            store
                .workflow_set_status(&id, status::PAUSED, &reason, None)
                .await?;
            notify(store, space_id, &id, &reason);
        }
    }
    Ok(())
}

/// Continues a paused or failed run.
pub async fn resume(store: &Arc<Store>, host: &Host, id: &str) -> Result<WorkflowRun, ScriptError> {
    let run = load(store, id).await?;
    if run.status() != elri_proto::elri::v1::workflow::RunStatus::Paused
        && run.status() != elri_proto::elri::v1::workflow::RunStatus::Failed
    {
        return Err(ScriptError::Workflow(format!(
            "run is {}",
            run.status().as_str_name().to_lowercase()
        )));
    }
    let run_id = Uuid::parse_str(&run.id).map_err(|_| ScriptError::Workflow("run id".into()))?;
    if let Some(name) = store
        .workflow_steps_in(run_id, step::UNKNOWN)
        .await?
        .first()
    {
        return Err(ScriptError::Workflow(format!(
            "step {name} has an unknown outcome; resolve it first"
        )));
    }
    // A run paused because the script changed continues with the current
    // version, by the user's explicit choice here.
    let space_id = ObjectId::parse(&run.space_id).map_err(crate::store::StoreError::from)?;
    let script_id = ObjectId::parse(&run.script_id).map_err(crate::store::StoreError::from)?;
    if let Ok(o) = async { store.space(space_id).await?.get(script_id).await }.await {
        if o.kind() == ObjectKind::Script {
            let s = script::script_of(&o)?;
            if s.version != run.version {
                store.workflow_repin(id, &s.version).await?;
            }
        }
    }
    store
        .workflow_set_status(id, status::RUNNING, "", None)
        .await?;
    spawn(Arc::clone(store), host.clone(), id.to_owned());
    load(store, id).await
}

/// What the user found out about a step: its result, or that it may run
/// again.
#[derive(Debug, Clone)]
pub enum Resolution {
    /// The step happened; this is its value.
    Done(String),
    /// The step did not happen, or may safely happen again.
    Retry,
}

/// Records a resolution for a step of a paused or failed run.
pub async fn resolve(
    store: &Arc<Store>,
    id: &str,
    name: &str,
    resolution: Resolution,
) -> Result<WorkflowRun, ScriptError> {
    let run = load(store, id).await?;
    if run.status() == elri_proto::elri::v1::workflow::RunStatus::Running {
        return Err(ScriptError::Workflow("run is running".to_owned()));
    }
    let run_id = Uuid::parse_str(&run.id).map_err(|_| ScriptError::Workflow("run id".into()))?;
    let Some(rec) = store.workflow_step(run_id, name).await? else {
        return Err(ScriptError::Workflow(format!("no step {name}")));
    };
    if rec.status == step::DONE {
        return Err(ScriptError::Workflow(format!("step {name} is done")));
    }
    match resolution {
        Resolution::Done(result_json) => {
            if result_json.len() > script::OUTPUT_LIMIT {
                return Err(ScriptError::Limit("step result size"));
            }
            if !result_json.is_empty() {
                serde_json::from_str::<serde_json::Value>(&result_json)
                    .map_err(|e| ScriptError::Workflow(format!("result_json: {e}")))?;
            }
            store
                .workflow_step_finish(run_id, name, step::DONE, &result_json, "")
                .await?;
        }
        Resolution::Retry => store.workflow_step_forget(run_id, name).await?,
    }
    load(store, id).await
}

/// Abandons a paused or failed run.
pub async fn cancel(store: &Arc<Store>, id: &str) -> Result<WorkflowRun, ScriptError> {
    let run = load(store, id).await?;
    match run.status() {
        elri_proto::elri::v1::workflow::RunStatus::Paused
        | elri_proto::elri::v1::workflow::RunStatus::Failed => {}
        other => {
            return Err(ScriptError::Workflow(format!(
                "run is {}",
                other.as_str_name().to_lowercase()
            )))
        }
    }
    store
        .workflow_set_status(id, status::CANCELLED, "cancelled by the user", None)
        .await?;
    load(store, id).await
}

async fn load(store: &Store, id: &str) -> Result<WorkflowRun, ScriptError> {
    store
        .workflow_run(id)
        .await?
        .ok_or_else(|| ScriptError::Workflow(format!("no run {id}")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use crate::device::DeviceId;
    use crate::extension::Modules;
    use elri_proto::elri::v1::workflow::{RunStatus, StepStatus};

    #[tokio::test]
    async fn an_interrupted_step_pauses_the_run_until_resolved() {
        let dir = std::env::temp_dir().join(format!("elri-wf-recover-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let device = DeviceId::load_or_create(&dir).expect("device");
        let store = Arc::new(Store::open(&dir, device).await.expect("store"));
        let host = Host::new(Arc::clone(&store), device.as_string(), Modules::new(&dir));
        let space = SpaceId::new();
        let script = ObjectId::new();

        // A run stopped between a step's intent and its result.
        let run = store
            .workflow_create(space, script, "v1", "", ResumePolicy::Auto)
            .await
            .expect("create");
        let run_id = Uuid::parse_str(&run.id).expect("id");
        store
            .workflow_step_finish(run_id, "first", step::DONE, "1", "")
            .await
            .expect("first");
        store
            .workflow_step_intent(run_id, "send")
            .await
            .expect("intent");

        recover(&store, &host).await.expect("recover");
        let paused = store
            .workflow_run(&run.id)
            .await
            .expect("get")
            .expect("run");
        assert_eq!(paused.status(), RunStatus::Paused);
        assert!(paused.reason.contains("send"), "{}", paused.reason);
        let send = paused
            .steps
            .iter()
            .find(|s| s.name == "send")
            .expect("send");
        assert_eq!(send.status(), StepStatus::Unknown);

        // Resuming is refused until the user says what happened.
        let err = resume(&store, &host, &run.id)
            .await
            .expect_err("unknown step");
        assert!(err.to_string().contains("unknown outcome"), "{err}");

        // The user found the effect did happen: the result is recorded and
        // the step will not run again.
        let resolved = resolve(&store, &run.id, "send", Resolution::Done("\"sent\"".into()))
            .await
            .expect("resolve");
        let send = resolved
            .steps
            .iter()
            .find(|s| s.name == "send")
            .expect("send");
        assert_eq!(send.status(), StepStatus::Done);
        assert_eq!(send.result_json, "\"sent\"");
        assert_eq!(resolved.status(), RunStatus::Paused);

        // Resume: the script is gone, so the run pauses again, by itself.
        resume(&store, &host, &run.id).await.expect("resume");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let r = store
                .workflow_run(&run.id)
                .await
                .expect("get")
                .expect("run");
            if r.status() == RunStatus::Paused {
                assert!(r.reason.contains("gone"), "{}", r.reason);
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "{r:?}");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let cancelled = cancel(&store, &run.id).await.expect("cancel");
        assert_eq!(cancelled.status(), RunStatus::Cancelled);
    }
}
