//! `ScriptService`, `GrantService`, and `RuleService`.

use std::sync::Arc;

use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::script::grant_service_ws::{
    register_grant_service_arc, GrantServiceHandler,
};
use elri_proto::elri::v1::script::rule_service_ws::{
    register_rule_service_arc, RuleServiceHandler,
};
use elri_proto::elri::v1::script::script_service_ws::{
    register_script_service_arc, ScriptServiceHandler,
};
use elri_proto::elri::v1::script::{
    CheckScriptRequest, CheckScriptResponse, CreateRuleRequest, CreateRuleResponse,
    CreateScriptRequest, CreateScriptResponse, GetPolicyRequest, GetPolicyResponse,
    GetScriptRequest, GetScriptResponse, GetSdkRequest, GetSdkResponse, GrantScriptRequest,
    GrantScriptResponse, ListGrantsRequest, ListGrantsResponse, ListRuleRunsRequest,
    ListRuleRunsResponse, ListRulesRequest, ListRulesResponse, ListScriptsRequest,
    ListScriptsResponse, RevokeGrantRequest, RevokeGrantResponse, RunRuleRequest, RunRuleResponse,
    RunScriptRequest, RunScriptResponse, SetPolicyRequest, SetPolicyResponse, UpdateRuleRequest,
    UpdateRuleResponse, UpdateScriptRequest, UpdateScriptResponse,
};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::model::{bool_value, now_ms, reserved, string_value, ObjectDoc, ObjectId};
use crate::rules::{self, rule_of, AUTO_GRANT_BENIGN};
use crate::script::{self, script_of, RunIdentity, ScriptError};
use crate::store::{ObjectFilter, StoreError};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_script_service_arc(server, Arc::clone(&services));
    register_grant_service_arc(server, Arc::clone(&services));
    register_rule_service_arc(server, services);
}

impl From<ScriptError> for Status {
    fn from(e: ScriptError) -> Self {
        match e {
            ScriptError::Store(s) => s.into(),
            ScriptError::Lua(_) | ScriptError::Manifest(_) => {
                Status::new(Code::InvalidArgument, e.to_string())
            }
            ScriptError::Denied(_) | ScriptError::Policy(_) => {
                Status::new(Code::PermissionDenied, e.to_string())
            }
            ScriptError::Limit(_) => Status::new(Code::ResourceExhausted, e.to_string()),
            ScriptError::Workflow(_) => Status::new(Code::FailedPrecondition, e.to_string()),
        }
    }
}

fn require_name(name: &str) -> Result<&str, Status> {
    let n = name.trim();
    if n.is_empty() {
        return Err(invalid("name is required"));
    }
    Ok(n)
}

impl Services {
    async fn script_object(&self, space_id: &str, id: &str) -> Result<Object, Status> {
        let space_id = parse_id(space_id, "space_id")?;
        let id = parse_id(id, "id")?;
        let o = self.store.space(space_id).await?.get(id).await?;
        if o.kind() != ObjectKind::Script {
            return Err(Status::new(Code::NotFound, "not a script"));
        }
        Ok(o)
    }
}

impl ScriptServiceHandler for Services {
    async fn create_script(
        &self,
        req: Request<CreateScriptRequest>,
    ) -> Result<Response<CreateScriptResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let name = require_name(&m.name)?;
        let diagnostics = script::check(&m.source);
        if !diagnostics.is_empty() {
            return Err(invalid(format!("source: {}", diagnostics.join("; "))));
        }
        let manifest = m.manifest.clone().unwrap_or_default();
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Script, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        script::write_script(&doc, Some(&m.source), Some(&manifest)).map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space_id).await?.insert(&doc).await?;
        Ok(Response::new(CreateScriptResponse {
            script: Some(script_of(&rec)?),
        }))
    }

    async fn update_script(
        &self,
        req: Request<UpdateScriptRequest>,
    ) -> Result<Response<UpdateScriptResponse>, Status> {
        let m = &req.msg;
        self.script_object(&m.space_id, &m.id).await?;
        if let Some(src) = &m.source {
            let diagnostics = script::check(src);
            if !diagnostics.is_empty() {
                return Err(invalid(format!("source: {}", diagnostics.join("; "))));
            }
        }
        let name = m
            .name
            .as_deref()
            .map(require_name)
            .transpose()?
            .map(str::to_owned);
        let source = m.source.clone();
        let manifest = m.manifest.clone();
        let rec = self
            .store
            .space(parse_id(&m.space_id, "space_id")?)
            .await?
            .update(parse_id(&m.id, "id")?, move |doc| {
                if let Some(n) = name {
                    doc.set_field(reserved::NAME, &string_value(n))?;
                }
                script::write_script(doc, source.as_deref(), manifest.as_ref())
            })
            .await?;
        Ok(Response::new(UpdateScriptResponse {
            script: Some(script_of(&rec)?),
        }))
    }

    async fn get_script(
        &self,
        req: Request<GetScriptRequest>,
    ) -> Result<Response<GetScriptResponse>, Status> {
        let o = self.script_object(&req.msg.space_id, &req.msg.id).await?;
        Ok(Response::new(GetScriptResponse {
            script: Some(script_of(&o)?),
        }))
    }

    async fn list_scripts(
        &self,
        req: Request<ListScriptsRequest>,
    ) -> Result<Response<ListScriptsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let objects = self
            .store
            .space(space_id)
            .await?
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Script),
                include_deleted: req.msg.include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        let mut scripts = Vec::with_capacity(objects.len());
        for o in &objects {
            scripts.push(script_of(o)?);
        }
        Ok(Response::new(ListScriptsResponse { scripts }))
    }

    async fn check_script(
        &self,
        req: Request<CheckScriptRequest>,
    ) -> Result<Response<CheckScriptResponse>, Status> {
        Ok(Response::new(CheckScriptResponse {
            diagnostics: script::check(&req.msg.source),
        }))
    }

    async fn get_sdk(
        &self,
        _req: Request<GetSdkRequest>,
    ) -> Result<Response<GetSdkResponse>, Status> {
        Ok(Response::new(GetSdkResponse {
            source: script::SDK.to_owned(),
        }))
    }

    async fn run_script(
        &self,
        req: Request<RunScriptRequest>,
    ) -> Result<Response<RunScriptResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let o = self.script_object(&m.space_id, &m.id).await?;
        let script = script_of(&o)?;
        let args: serde_json::Value = if m.args_json.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&m.args_json).map_err(|e| invalid(format!("args_json: {e}")))?
        };
        let Some(grant) = rules::grant_or_auto(&self.store, space_id, &script).await? else {
            return Err(Status::new(
                Code::PermissionDenied,
                "this version of the script has no grant on this device",
            ));
        };
        let result = self
            .scripts
            .run(
                space_id,
                &script,
                args,
                RunIdentity {
                    script_id: script.id.clone(),
                    rule_id: None,
                    grant_id: grant.id,
                },
            )
            .await;
        Ok(Response::new(RunScriptResponse {
            result: Some(result),
        }))
    }
}

impl GrantServiceHandler for Services {
    async fn list_grants(
        &self,
        req: Request<ListGrantsRequest>,
    ) -> Result<Response<ListGrantsResponse>, Status> {
        let space = if req.msg.space_id.is_empty() {
            None
        } else {
            Some(parse_id(&req.msg.space_id, "space_id")?)
        };
        Ok(Response::new(ListGrantsResponse {
            grants: self.store.grants(space, req.msg.include_revoked).await?,
        }))
    }

    async fn grant_script(
        &self,
        req: Request<GrantScriptRequest>,
    ) -> Result<Response<GrantScriptResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let o = self
            .script_object(&req.msg.space_id, &req.msg.script_id)
            .await?;
        let script = script_of(&o)?;
        if !req.msg.expected_version.is_empty() && req.msg.expected_version != script.version {
            return Err(Status::new(
                Code::FailedPrecondition,
                "the script changed since it was shown; look at it again before granting",
            ));
        }
        let grant = self
            .store
            .grant(
                space_id,
                parse_id(&script.id, "script_id")?,
                &script.version,
                script.benign,
            )
            .await?;
        Ok(Response::new(GrantScriptResponse { grant: Some(grant) }))
    }

    async fn revoke_grant(
        &self,
        req: Request<RevokeGrantRequest>,
    ) -> Result<Response<RevokeGrantResponse>, Status> {
        parse_id(&req.msg.id, "id")?;
        self.store.revoke_grant(&req.msg.id).await?;
        Ok(Response::new(RevokeGrantResponse {}))
    }

    async fn get_policy(
        &self,
        _req: Request<GetPolicyRequest>,
    ) -> Result<Response<GetPolicyResponse>, Status> {
        Ok(Response::new(GetPolicyResponse {
            auto_grant_benign: self.store.setting(AUTO_GRANT_BENIGN, 1).await? != 0,
            max_concurrent_scripts: i32::try_from(self.scripts.concurrency()).unwrap_or(i32::MAX),
        }))
    }

    async fn set_policy(
        &self,
        req: Request<SetPolicyRequest>,
    ) -> Result<Response<SetPolicyResponse>, Status> {
        self.store
            .set_setting(AUTO_GRANT_BENIGN, i64::from(req.msg.auto_grant_benign))
            .await?;
        if req.msg.max_concurrent_scripts > 0 {
            let n = usize::try_from(req.msg.max_concurrent_scripts)
                .unwrap_or(1)
                .min(script::MAX_CONCURRENCY);
            self.store
                .set_setting(rules::MAX_CONCURRENT_SCRIPTS, i64::try_from(n).unwrap_or(1))
                .await?;
            self.scripts.set_concurrency(n);
        }
        Ok(Response::new(SetPolicyResponse {
            auto_grant_benign: req.msg.auto_grant_benign,
        }))
    }
}

impl RuleServiceHandler for Services {
    async fn create_rule(
        &self,
        req: Request<CreateRuleRequest>,
    ) -> Result<Response<CreateRuleResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let name = require_name(&m.name)?;
        self.script_object(&m.space_id, &m.script_id).await?;
        let executor = if m.executor_device_id.trim().is_empty() {
            self.device_id.as_string()
        } else {
            m.executor_device_id.trim().to_owned()
        };
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Rule, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        doc.set_field(rules::SCRIPT, &string_value(&m.script_id))
            .map_err(StoreError::from)?;
        doc.set_field(rules::EXECUTOR, &string_value(executor))
            .map_err(StoreError::from)?;
        doc.set_field(rules::ENABLED, &bool_value(true))
            .map_err(StoreError::from)?;
        rules::write_trigger(&doc, &m.trigger.clone().unwrap_or_default())
            .map_err(StoreError::from)?;
        rules::write_notify(&doc, m.notify()).map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space_id).await?.insert(&doc).await?;
        Ok(Response::new(CreateRuleResponse {
            rule: Some(rule_of(&rec)),
        }))
    }

    async fn update_rule(
        &self,
        req: Request<UpdateRuleRequest>,
    ) -> Result<Response<UpdateRuleResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        let space = self.store.space(space_id).await?;
        if space.get(id).await?.kind() != ObjectKind::Rule {
            return Err(Status::new(Code::NotFound, "not a rule"));
        }
        let name = m
            .name
            .as_deref()
            .map(require_name)
            .transpose()?
            .map(str::to_owned);
        let enabled = m.enabled;
        let trigger = m.trigger.clone();
        let executor = m.executor_device_id.clone();
        let notify = (m.notify != 0).then_some(m.notify());
        let rec = space
            .update(id, move |doc| {
                if let Some(n) = notify {
                    rules::write_notify(doc, n)?;
                }
                if let Some(n) = name {
                    doc.set_field(reserved::NAME, &string_value(n))?;
                }
                if let Some(e) = enabled {
                    doc.set_field(rules::ENABLED, &bool_value(e))?;
                }
                if let Some(t) = trigger {
                    rules::write_trigger(doc, &t)?;
                }
                if let Some(x) = executor {
                    doc.set_field(rules::EXECUTOR, &string_value(x))?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdateRuleResponse {
            rule: Some(rule_of(&rec)),
        }))
    }

    async fn list_rule_runs(
        &self,
        req: Request<ListRuleRunsRequest>,
    ) -> Result<Response<ListRuleRunsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        if self.store.space(space_id).await?.get(id).await?.kind() != ObjectKind::Rule {
            return Err(Status::new(Code::NotFound, "not a rule"));
        }
        let runs = self
            .store
            .rule_runs(id, usize::try_from(req.msg.limit).unwrap_or(0))
            .await?;
        Ok(Response::new(ListRuleRunsResponse { runs }))
    }

    async fn run_rule(
        &self,
        req: Request<RunRuleRequest>,
    ) -> Result<Response<RunRuleResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let run = rules::run_now(&self.store, &self.scripts, space_id, id).await?;
        Ok(Response::new(RunRuleResponse { run: Some(run) }))
    }

    async fn list_rules(
        &self,
        req: Request<ListRulesRequest>,
    ) -> Result<Response<ListRulesResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let objects = self
            .store
            .space(space_id)
            .await?
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Rule),
                include_deleted: req.msg.include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        Ok(Response::new(ListRulesResponse {
            rules: objects.iter().map(rule_of).collect(),
        }))
    }
}
