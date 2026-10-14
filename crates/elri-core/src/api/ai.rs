//! `AiService` and `AgentService`: provider accounts and models on this
//! device; agents, skills, prompts and conversations in a Space.

use std::sync::Arc;

use elri_proto::elri::v1::ai::agent_service_ws::{
    register_agent_service_arc, AgentServiceHandler, SendServerStream,
};
use elri_proto::elri::v1::ai::ai_service_ws::{register_ai_service_arc, AiServiceHandler};
use elri_proto::elri::v1::ai::{
    AddApiKeyRequest, AddApiKeyResponse, CreateAgentRequest, CreateAgentResponse,
    CreatePromptRequest, CreatePromptResponse, CreateSkillRequest, CreateSkillResponse,
    GetConversationRequest, GetConversationResponse, GetSignInRequest, GetSignInResponse,
    GrantAgentRequest, GrantAgentResponse, ListAccountsRequest, ListAccountsResponse,
    ListAgentsRequest, ListAgentsResponse, ListConversationsRequest, ListConversationsResponse,
    ListModelsRequest, ListModelsResponse, ListPromptsRequest, ListPromptsResponse,
    ListSkillsRequest, ListSkillsResponse, SendRequest, SignOutRequest, SignOutResponse,
    StartConversationRequest, StartConversationResponse, StartSignInRequest, StartSignInResponse,
    UpdateAgentRequest, UpdateAgentResponse, UpdatePromptRequest, UpdatePromptResponse,
    UpdateSkillRequest, UpdateSkillResponse,
};
use elri_proto::elri::v1::object::ObjectKind;
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::ai::harness::{
    agent_of, as_script, check_agent, conversation_of, field, prompt_of, skill_of, write_agent,
};
use crate::ai::AiError;
use crate::model::{now_ms, reserved, string_value, text_value, ObjectDoc, ObjectId, SpaceId};
use crate::store::{ObjectFilter, StoreError};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_ai_service_arc(server, Arc::clone(&services));
    register_agent_service_arc(server, services);
}

impl From<AiError> for Status {
    fn from(e: AiError) -> Self {
        match e {
            AiError::Store(s) => s.into(),
            AiError::Secrets(s) => {
                tracing::error!(error = %s, "credential store failed");
                Status::new(Code::Internal, "credential store failed")
            }
            AiError::NoAccount(m) => Status::new(Code::NotFound, m),
            AiError::SignIn(m) => Status::new(Code::Unauthenticated, m),
            AiError::Invalid(m) => Status::new(Code::InvalidArgument, m),
            AiError::Transport(m) => Status::new(Code::Unavailable, m),
            e @ AiError::Provider { .. } => Status::new(Code::FailedPrecondition, e.to_string()),
        }
    }
}

impl Services {
    async fn of_kind(
        &self,
        space: SpaceId,
        kind: ObjectKind,
        include_deleted: bool,
    ) -> Result<Vec<elri_proto::elri::v1::object::Object>, Status> {
        Ok(self
            .store
            .space(space)
            .await?
            .list(&ObjectFilter {
                kind: Some(kind),
                include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?)
    }

    async fn ensure_kind(
        &self,
        space: SpaceId,
        id: ObjectId,
        kind: ObjectKind,
    ) -> Result<(), Status> {
        if self.store.space(space).await?.get(id).await?.kind() == kind {
            Ok(())
        } else {
            Err(Status::new(
                Code::NotFound,
                format!("not a {}", kind.as_str_name().to_lowercase()),
            ))
        }
    }
}

fn create_doc(space: SpaceId, kind: ObjectKind) -> Result<ObjectDoc, Status> {
    Ok(ObjectDoc::create(ObjectId::new(), space, kind, now_ms()).map_err(StoreError::from)?)
}

impl AiServiceHandler for Services {
    async fn start_sign_in(
        &self,
        req: Request<StartSignInRequest>,
    ) -> Result<Response<StartSignInResponse>, Status> {
        let m = &req.msg;
        let existing = (!m.account_id.is_empty()).then_some(m.account_id.as_str());
        let (attempt_id, authorize_url) = self.ai.start_sign_in(existing, &m.label).await?;
        Ok(Response::new(StartSignInResponse {
            attempt_id,
            authorize_url,
        }))
    }

    async fn get_sign_in(
        &self,
        req: Request<GetSignInRequest>,
    ) -> Result<Response<GetSignInResponse>, Status> {
        let a = self
            .ai
            .sign_in_state(&req.msg.attempt_id)
            .ok_or_else(|| Status::new(Code::NotFound, "no such sign-in"))?;
        Ok(Response::new(GetSignInResponse {
            state: a.state.into(),
            account: a.account,
            error: a.error,
        }))
    }

    async fn add_api_key(
        &self,
        req: Request<AddApiKeyRequest>,
    ) -> Result<Response<AddApiKeyResponse>, Status> {
        let account = self
            .ai
            .add_api_key(&req.msg.label, &req.msg.api_key)
            .await?;
        Ok(Response::new(AddApiKeyResponse {
            account: Some(account),
        }))
    }

    async fn list_accounts(
        &self,
        _req: Request<ListAccountsRequest>,
    ) -> Result<Response<ListAccountsResponse>, Status> {
        Ok(Response::new(ListAccountsResponse {
            accounts: self.store.ai_accounts().await?,
        }))
    }

    async fn sign_out(
        &self,
        req: Request<SignOutRequest>,
    ) -> Result<Response<SignOutResponse>, Status> {
        parse_id(&req.msg.account_id, "account_id")?;
        let revoked = self.ai.sign_out(&req.msg.account_id).await?;
        Ok(Response::new(SignOutResponse { revoked }))
    }

    async fn list_models(
        &self,
        req: Request<ListModelsRequest>,
    ) -> Result<Response<ListModelsResponse>, Status> {
        let account = if req.msg.account_id.is_empty() {
            let s = self.store.settings().await?.ai.unwrap_or_default();
            if s.default_account_id.is_empty() {
                self.store
                    .ai_accounts()
                    .await?
                    .into_iter()
                    .find(|a| !a.needs_sign_in)
                    .map(|a| a.id)
                    .ok_or_else(|| {
                        Status::new(Code::FailedPrecondition, "no AI account; sign in first")
                    })?
            } else {
                s.default_account_id
            }
        } else {
            req.msg.account_id.clone()
        };
        Ok(Response::new(ListModelsResponse {
            models: self.ai.list_models(&account).await?,
        }))
    }
}

impl AgentServiceHandler for Services {
    async fn create_agent(
        &self,
        req: Request<CreateAgentRequest>,
    ) -> Result<Response<CreateAgentResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let a = req.msg.agent.clone().unwrap_or_default();
        check_agent(&a).map_err(invalid)?;
        let doc = create_doc(space, ObjectKind::Agent)?;
        write_agent(&doc, &a).map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space).await?.insert(&doc).await?;
        Ok(Response::new(CreateAgentResponse {
            agent: Some(agent_of(&rec)),
        }))
    }

    async fn update_agent(
        &self,
        req: Request<UpdateAgentRequest>,
    ) -> Result<Response<UpdateAgentResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let a = req.msg.agent.clone().unwrap_or_default();
        let id = parse_id(&a.id, "agent.id")?;
        check_agent(&a).map_err(invalid)?;
        self.ensure_kind(space, id, ObjectKind::Agent).await?;
        let rec = self
            .store
            .space(space)
            .await?
            .update(id, move |doc| write_agent(doc, &a))
            .await?;
        Ok(Response::new(UpdateAgentResponse {
            agent: Some(agent_of(&rec)),
        }))
    }

    async fn list_agents(
        &self,
        req: Request<ListAgentsRequest>,
    ) -> Result<Response<ListAgentsResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let agents = self
            .of_kind(space, ObjectKind::Agent, req.msg.include_deleted)
            .await?
            .iter()
            .map(agent_of)
            .collect();
        Ok(Response::new(ListAgentsResponse { agents }))
    }

    async fn grant_agent(
        &self,
        req: Request<GrantAgentRequest>,
    ) -> Result<Response<GrantAgentResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.agent_id, "agent_id")?;
        let o = self.store.space(space).await?.get(id).await?;
        if o.kind() != ObjectKind::Agent {
            return Err(Status::new(Code::NotFound, "not an agent"));
        }
        let a = agent_of(&o);
        if !req.msg.expected_version.is_empty() && req.msg.expected_version != a.version {
            return Err(Status::new(
                Code::FailedPrecondition,
                "the agent changed since it was shown; look at it again before granting",
            ));
        }
        let s = as_script(&a);
        let grant = self.store.grant(space, id, &s.version, s.benign).await?;
        Ok(Response::new(GrantAgentResponse { grant_id: grant.id }))
    }

    async fn create_skill(
        &self,
        req: Request<CreateSkillRequest>,
    ) -> Result<Response<CreateSkillResponse>, Status> {
        let m = &req.msg;
        let space = parse_id(&m.space_id, "space_id")?;
        if m.name.trim().is_empty() || m.body.trim().is_empty() {
            return Err(invalid("a skill needs a name and a body"));
        }
        let doc = create_doc(space, ObjectKind::Skill)?;
        doc.set_field(reserved::NAME, &string_value(m.name.trim()))
            .map_err(StoreError::from)?;
        doc.set_field(field::DESCRIPTION, &string_value(m.description.trim()))
            .map_err(StoreError::from)?;
        doc.set_field(field::BODY, &text_value(&m.body))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space).await?.insert(&doc).await?;
        Ok(Response::new(CreateSkillResponse {
            skill: Some(skill_of(&rec)),
        }))
    }

    async fn update_skill(
        &self,
        req: Request<UpdateSkillRequest>,
    ) -> Result<Response<UpdateSkillResponse>, Status> {
        let m = req.msg;
        let space = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        self.ensure_kind(space, id, ObjectKind::Skill).await?;
        let rec = self
            .store
            .space(space)
            .await?
            .update(id, move |doc| {
                if let Some(n) = &m.name {
                    doc.set_field(reserved::NAME, &string_value(n.trim()))?;
                }
                if let Some(d) = &m.description {
                    doc.set_field(field::DESCRIPTION, &string_value(d.trim()))?;
                }
                if let Some(b) = &m.body {
                    doc.set_field(field::BODY, &text_value(b))?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdateSkillResponse {
            skill: Some(skill_of(&rec)),
        }))
    }

    async fn list_skills(
        &self,
        req: Request<ListSkillsRequest>,
    ) -> Result<Response<ListSkillsResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let mut skills = crate::ai::skills::builtin();
        skills.extend(
            self.of_kind(space, ObjectKind::Skill, false)
                .await?
                .iter()
                .map(skill_of),
        );
        Ok(Response::new(ListSkillsResponse { skills }))
    }

    async fn create_prompt(
        &self,
        req: Request<CreatePromptRequest>,
    ) -> Result<Response<CreatePromptResponse>, Status> {
        let m = &req.msg;
        let space = parse_id(&m.space_id, "space_id")?;
        if m.name.trim().is_empty() {
            return Err(invalid("a prompt needs a name"));
        }
        let doc = create_doc(space, ObjectKind::Prompt)?;
        doc.set_field(reserved::NAME, &string_value(m.name.trim()))
            .map_err(StoreError::from)?;
        doc.set_field(field::BODY, &text_value(&m.body))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space).await?.insert(&doc).await?;
        Ok(Response::new(CreatePromptResponse {
            prompt: Some(prompt_of(&rec)),
        }))
    }

    async fn update_prompt(
        &self,
        req: Request<UpdatePromptRequest>,
    ) -> Result<Response<UpdatePromptResponse>, Status> {
        let m = req.msg;
        let space = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        self.ensure_kind(space, id, ObjectKind::Prompt).await?;
        let rec = self
            .store
            .space(space)
            .await?
            .update(id, move |doc| {
                if let Some(n) = &m.name {
                    doc.set_field(reserved::NAME, &string_value(n.trim()))?;
                }
                if let Some(b) = &m.body {
                    doc.set_field(field::BODY, &text_value(b))?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdatePromptResponse {
            prompt: Some(prompt_of(&rec)),
        }))
    }

    async fn list_prompts(
        &self,
        req: Request<ListPromptsRequest>,
    ) -> Result<Response<ListPromptsResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let prompts = self
            .of_kind(space, ObjectKind::Prompt, false)
            .await?
            .iter()
            .map(prompt_of)
            .collect();
        Ok(Response::new(ListPromptsResponse { prompts }))
    }

    async fn start_conversation(
        &self,
        req: Request<StartConversationRequest>,
    ) -> Result<Response<StartConversationResponse>, Status> {
        let m = &req.msg;
        let space = parse_id(&m.space_id, "space_id")?;
        let agent = parse_id(&m.agent_id, "agent_id")?;
        self.ensure_kind(space, agent, ObjectKind::Agent).await?;
        let doc = create_doc(space, ObjectKind::Conversation)?;
        let title = if m.title.trim().is_empty() {
            "New conversation"
        } else {
            m.title.trim()
        };
        doc.set_field(reserved::NAME, &string_value(title))
            .map_err(StoreError::from)?;
        doc.set_field(field::AGENT, &string_value(agent.to_string()))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = self.store.space(space).await?.insert(&doc).await?;
        Ok(Response::new(StartConversationResponse {
            conversation: Some(conversation_of(&rec, true)),
        }))
    }

    async fn list_conversations(
        &self,
        req: Request<ListConversationsRequest>,
    ) -> Result<Response<ListConversationsResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let mut conversations: Vec<_> = self
            .of_kind(space, ObjectKind::Conversation, false)
            .await?
            .iter()
            .map(|o| conversation_of(o, false))
            .filter(|c| req.msg.agent_id.is_empty() || c.agent_id == req.msg.agent_id)
            .collect();
        conversations.sort_by_key(|c| std::cmp::Reverse(c.updated_at_ms));
        Ok(Response::new(ListConversationsResponse { conversations }))
    }

    async fn get_conversation(
        &self,
        req: Request<GetConversationRequest>,
    ) -> Result<Response<GetConversationResponse>, Status> {
        let space = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let o = self.store.space(space).await?.get(id).await?;
        if o.kind() != ObjectKind::Conversation {
            return Err(Status::new(Code::NotFound, "not a conversation"));
        }
        Ok(Response::new(GetConversationResponse {
            conversation: Some(conversation_of(&o, true)),
        }))
    }

    async fn send(
        &self,
        req: Request<SendRequest>,
        stream: SendServerStream,
    ) -> Result<(), Status> {
        let m = req.msg;
        let space = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.conversation_id, "conversation_id")?;
        if m.text.trim().is_empty() {
            return Err(invalid("text is required"));
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let ai = Arc::clone(&self.ai);
        // The turn runs to its end even if the caller goes away: what it
        // does is saved to the conversation either way.
        tokio::spawn(async move { ai.run_turn(space, id, m.text, tx).await });
        let mut open = true;
        while let Some(ev) = rx.recv().await {
            if open && stream.send(&ev).await.is_err() {
                open = false;
            }
        }
        Ok(())
    }
}
