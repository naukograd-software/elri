//! `WorkflowService`: durable script runs on this device.

use std::sync::Arc;

use elri_proto::elri::v1::workflow::workflow_service_ws::{
    register_workflow_service_arc, WorkflowServiceHandler,
};
use elri_proto::elri::v1::workflow::{
    resolve_workflow_step_request, CancelWorkflowRunRequest, CancelWorkflowRunResponse,
    GetWorkflowRunRequest, GetWorkflowRunResponse, ListWorkflowRunsRequest,
    ListWorkflowRunsResponse, ResolveWorkflowStepRequest, ResolveWorkflowStepResponse,
    ResumeWorkflowRunRequest, ResumeWorkflowRunResponse, StartWorkflowRequest,
    StartWorkflowResponse,
};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::workflows::{self, Resolution};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_workflow_service_arc(server, services);
}

impl WorkflowServiceHandler for Services {
    async fn start_workflow(
        &self,
        req: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let script_id = parse_id(&m.script_id, "script_id")?;
        let args: serde_json::Value = if m.args_json.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&m.args_json).map_err(|e| invalid(format!("args_json: {e}")))?
        };
        let run = workflows::start(
            &self.store,
            &self.scripts,
            space_id,
            script_id,
            args,
            m.resume(),
        )
        .await?;
        Ok(Response::new(StartWorkflowResponse { run: Some(run) }))
    }

    async fn list_workflow_runs(
        &self,
        req: Request<ListWorkflowRunsRequest>,
    ) -> Result<Response<ListWorkflowRunsResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let script_id = if m.script_id.is_empty() {
            None
        } else {
            Some(parse_id(&m.script_id, "script_id")?)
        };
        let runs = self
            .store
            .workflow_runs(space_id, script_id, usize::try_from(m.limit).unwrap_or(0))
            .await?;
        Ok(Response::new(ListWorkflowRunsResponse { runs }))
    }

    async fn get_workflow_run(
        &self,
        req: Request<GetWorkflowRunRequest>,
    ) -> Result<Response<GetWorkflowRunResponse>, Status> {
        parse_id(&req.msg.id, "id")?;
        let run = self
            .store
            .workflow_run(&req.msg.id)
            .await?
            .ok_or_else(|| Status::new(Code::NotFound, "no such run"))?;
        Ok(Response::new(GetWorkflowRunResponse { run: Some(run) }))
    }

    async fn resume_workflow_run(
        &self,
        req: Request<ResumeWorkflowRunRequest>,
    ) -> Result<Response<ResumeWorkflowRunResponse>, Status> {
        parse_id(&req.msg.id, "id")?;
        let run = workflows::resume(&self.store, &self.scripts, &req.msg.id).await?;
        Ok(Response::new(ResumeWorkflowRunResponse { run: Some(run) }))
    }

    async fn resolve_workflow_step(
        &self,
        req: Request<ResolveWorkflowStepRequest>,
    ) -> Result<Response<ResolveWorkflowStepResponse>, Status> {
        let m = &req.msg;
        parse_id(&m.id, "id")?;
        if m.step.trim().is_empty() {
            return Err(invalid("step is required"));
        }
        let resolution = match &m.outcome {
            Some(resolve_workflow_step_request::Outcome::ResultJson(r)) => {
                Resolution::Done(r.clone())
            }
            Some(resolve_workflow_step_request::Outcome::Retry(true)) => Resolution::Retry,
            _ => return Err(invalid("outcome: result_json or retry is required")),
        };
        let run = workflows::resolve(&self.store, &m.id, m.step.trim(), resolution).await?;
        Ok(Response::new(ResolveWorkflowStepResponse {
            run: Some(run),
        }))
    }

    async fn cancel_workflow_run(
        &self,
        req: Request<CancelWorkflowRunRequest>,
    ) -> Result<Response<CancelWorkflowRunResponse>, Status> {
        parse_id(&req.msg.id, "id")?;
        let run = workflows::cancel(&self.store, &req.msg.id).await?;
        Ok(Response::new(CancelWorkflowRunResponse { run: Some(run) }))
    }
}
