//! Workflow runs and their steps in the device database.

use elri_proto::elri::v1::workflow::{
    ResumePolicy, RunStatus, StepStatus, WorkflowRun, WorkflowStep,
};
use uuid::Uuid;

use super::models::{WorkflowRunRow, WorkflowStepRow};
use super::{uuid_arg, Store, StoreError};
use crate::model::{now_ms, ObjectId, SpaceId};

/// Run status names as stored.
pub mod status {
    /// The script is executing.
    pub const RUNNING: &str = "running";
    /// Waiting for the user.
    pub const PAUSED: &str = "paused";
    /// Finished.
    pub const DONE: &str = "done";
    /// The script raised.
    pub const FAILED: &str = "failed";
    /// Abandoned by the user.
    pub const CANCELLED: &str = "cancelled";
}

/// Step status names as stored.
pub mod step {
    /// Recorded before the step ran.
    pub const INTENT: &str = "intent";
    /// Finished with a result.
    pub const DONE: &str = "done";
    /// Raised.
    pub const FAILED: &str = "failed";
    /// Interrupted between intent and result.
    pub const UNKNOWN: &str = "unknown";
}

fn run_status(s: &str) -> RunStatus {
    match s {
        status::RUNNING => RunStatus::Running,
        status::PAUSED => RunStatus::Paused,
        status::DONE => RunStatus::Done,
        status::FAILED => RunStatus::Failed,
        status::CANCELLED => RunStatus::Cancelled,
        _ => RunStatus::Unspecified,
    }
}

fn step_status(s: &str) -> StepStatus {
    match s {
        step::INTENT => StepStatus::Intent,
        step::DONE => StepStatus::Done,
        step::FAILED => StepStatus::Failed,
        step::UNKNOWN => StepStatus::Unknown,
        _ => StepStatus::Unspecified,
    }
}

fn run_of(r: &WorkflowRunRow, steps: Vec<WorkflowStep>) -> WorkflowRun {
    WorkflowRun {
        id: r.id.to_string(),
        space_id: r.space_id.to_string(),
        script_id: r.script_id.to_string(),
        version: r.version.clone(),
        args_json: r.args_json.clone(),
        status: run_status(&r.status).into(),
        reason: r.reason.clone(),
        output_json: r.output_json.clone(),
        resume: ResumePolicy::try_from(i32::try_from(r.resume).unwrap_or(0))
            .unwrap_or(ResumePolicy::Ask)
            .into(),
        created_at_ms: r.created_at,
        updated_at_ms: r.updated_at,
        steps,
    }
}

fn step_of(s: &WorkflowStepRow) -> WorkflowStep {
    WorkflowStep {
        name: s.name.clone(),
        status: step_status(&s.status).into(),
        result_json: s.result_json.clone(),
        error: s.error.clone(),
        at_ms: s.at,
    }
}

/// A recorded step, as the host sees it.
#[derive(Debug, Clone)]
pub struct StepRecord {
    /// `intent`, `done`, `failed`, or `unknown`.
    pub status: String,
    /// The step's value as JSON, when done.
    pub result_json: String,
}

impl Store {
    /// Records a new run as running.
    pub async fn workflow_create(
        &self,
        space_id: SpaceId,
        script_id: ObjectId,
        version: &str,
        args_json: &str,
        resume: ResumePolicy,
    ) -> Result<WorkflowRun, StoreError> {
        let now = now_ms();
        let row = WorkflowRunRow {
            id: Uuid::now_v7(),
            space_id: space_id.uuid(),
            script_id: script_id.uuid(),
            version: version.to_owned(),
            args_json: args_json.to_owned(),
            status: status::RUNNING.to_owned(),
            reason: String::new(),
            output_json: String::new(),
            resume: i64::from(resume as i32),
            created_at: now,
            updated_at: now,
        };
        let run = run_of(&row, Vec::new());
        let mut db = self.device.lock().await;
        toasty::create!(WorkflowRunRow {
            id: row.id,
            space_id: row.space_id,
            script_id: row.script_id,
            version: row.version,
            args_json: row.args_json,
            status: row.status,
            reason: row.reason,
            output_json: row.output_json,
            resume: row.resume,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .exec(&mut *db)
        .await?;
        Ok(run)
    }

    /// A run with its steps, or `None`.
    pub async fn workflow_run(&self, id: &str) -> Result<Option<WorkflowRun>, StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        let Some(row) = WorkflowRunRow::filter_by_id(id)
            .first()
            .exec(&mut *db)
            .await?
        else {
            return Ok(None);
        };
        let mut steps = WorkflowStepRow::filter(WorkflowStepRow::fields().run_id().eq(id))
            .exec(&mut *db)
            .await?;
        steps.sort_by_key(|s| (s.at, s.id));
        Ok(Some(run_of(&row, steps.iter().map(step_of).collect())))
    }

    /// Runs of a Space, newest first, optionally of one script.
    pub async fn workflow_runs(
        &self,
        space_id: SpaceId,
        script_id: Option<ObjectId>,
        limit: usize,
    ) -> Result<Vec<WorkflowRun>, StoreError> {
        let limit = if limit == 0 { 50 } else { limit.min(500) };
        let mut db = self.device.lock().await;
        let mut rows =
            WorkflowRunRow::filter(WorkflowRunRow::fields().space_id().eq(space_id.uuid()))
                .exec(&mut *db)
                .await?;
        if let Some(s) = script_id {
            rows.retain(|r| r.script_id == s.uuid());
        }
        rows.sort_by_key(|r| std::cmp::Reverse((r.created_at, r.id)));
        rows.truncate(limit);
        Ok(rows.iter().map(|r| run_of(r, Vec::new())).collect())
    }

    /// Ids of every run in `status`.
    pub async fn workflow_runs_in(&self, status: &str) -> Result<Vec<String>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = WorkflowRunRow::filter(WorkflowRunRow::fields().status().eq(status.to_owned()))
            .exec(&mut *db)
            .await?;
        Ok(rows.iter().map(|r| r.id.to_string()).collect())
    }

    /// Sets a run's status and reason; `output` replaces the output when
    /// given.
    pub async fn workflow_set_status(
        &self,
        id: &str,
        status: &str,
        reason: &str,
        output: Option<&str>,
    ) -> Result<(), StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        let output = match output {
            Some(o) => o.to_owned(),
            None => WorkflowRunRow::filter_by_id(id)
                .first()
                .exec(&mut *db)
                .await?
                .map(|r| r.output_json)
                .unwrap_or_default(),
        };
        toasty::update!(WorkflowRunRow::filter_by_id(id) {
            status: status.to_owned(),
            reason: reason.to_owned(),
            output_json: output,
            updated_at: now_ms(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Pins a run to another script version.
    pub async fn workflow_repin(&self, id: &str, version: &str) -> Result<(), StoreError> {
        let id = uuid_arg(id)?;
        let mut db = self.device.lock().await;
        toasty::update!(WorkflowRunRow::filter_by_id(id) {
            version: version.to_owned(),
            updated_at: now_ms(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// The record of a step, if the run reached it.
    pub async fn workflow_step(
        &self,
        run: Uuid,
        name: &str,
    ) -> Result<Option<StepRecord>, StoreError> {
        let mut db = self.device.lock().await;
        Ok(step_row(&mut db, run, name).await?.map(|s| StepRecord {
            status: s.status,
            result_json: s.result_json,
        }))
    }

    /// Records that a step is about to run, replacing a failed record.
    pub async fn workflow_step_intent(&self, run: Uuid, name: &str) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        delete_step(&mut db, run, name).await?;
        toasty::create!(WorkflowStepRow {
            id: Uuid::now_v7(),
            run_id: run,
            name: name.to_owned(),
            status: step::INTENT.to_owned(),
            result_json: String::new(),
            error: String::new(),
            at: now_ms(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Records a step's outcome: `done` with a result, or `failed` with an
    /// error, or `unknown`.
    pub async fn workflow_step_finish(
        &self,
        run: Uuid,
        name: &str,
        status: &str,
        result_json: &str,
        error: &str,
    ) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        delete_step(&mut db, run, name).await?;
        toasty::create!(WorkflowStepRow {
            id: Uuid::now_v7(),
            run_id: run,
            name: name.to_owned(),
            status: status.to_owned(),
            result_json: result_json.to_owned(),
            error: error.to_owned(),
            at: now_ms(),
        })
        .exec(&mut *db)
        .await?;
        Ok(())
    }

    /// Forgets a step so it runs again.
    pub async fn workflow_step_forget(&self, run: Uuid, name: &str) -> Result<(), StoreError> {
        let mut db = self.device.lock().await;
        delete_step(&mut db, run, name).await
    }

    /// Steps of a run in `status`, by name.
    pub async fn workflow_steps_in(
        &self,
        run: Uuid,
        status: &str,
    ) -> Result<Vec<String>, StoreError> {
        let mut db = self.device.lock().await;
        let rows = WorkflowStepRow::filter(WorkflowStepRow::fields().run_id().eq(run))
            .exec(&mut *db)
            .await?;
        Ok(rows
            .into_iter()
            .filter(|s| s.status == status)
            .map(|s| s.name)
            .collect())
    }
}

async fn step_row(
    db: &mut toasty::Db,
    run: Uuid,
    name: &str,
) -> Result<Option<WorkflowStepRow>, StoreError> {
    let rows = WorkflowStepRow::filter(WorkflowStepRow::fields().run_id().eq(run))
        .exec(&mut *db)
        .await?;
    Ok(rows.into_iter().find(|s| s.name == name))
}

async fn delete_step(db: &mut toasty::Db, run: Uuid, name: &str) -> Result<(), StoreError> {
    if let Some(existing) = step_row(db, run, name).await? {
        WorkflowStepRow::filter_by_id(existing.id)
            .delete()
            .exec(&mut *db)
            .await?;
    }
    Ok(())
}
