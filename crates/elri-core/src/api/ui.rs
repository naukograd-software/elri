//! `InterfaceService`: interfaces as Objects, evaluated and driven by the
//! core so the renderer never runs user code.

use std::sync::Arc;

use elri_proto::elri::v1::object::ObjectKind;
use elri_proto::elri::v1::ui::interface_service_ws::{
    register_interface_service_arc, InterfaceServiceHandler,
};
use elri_proto::elri::v1::ui::{
    CreateInterfaceRequest, CreateInterfaceResponse, DispatchRequest, DispatchResponse,
    EvaluateRequest, EvaluateResponse, GetInterfaceRequest, GetInterfaceResponse, Interface,
    ListInterfacesRequest, ListInterfacesResponse, PublishInterfaceRequest,
    PublishInterfaceResponse, SelectVersionRequest, SelectVersionResponse, UpdateInterfaceRequest,
    UpdateInterfaceResponse,
};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::model::{int_value, now_ms, reserved, string_value, ObjectDoc, ObjectId, SpaceId};
use crate::rules::grant_or_auto;
use crate::store::{ObjectFilter, SpaceStore, StoreError};
use crate::ui::{self, interface_of};

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_interface_service_arc(server, services);
}

fn json_or_null(s: &str, what: &str) -> Result<serde_json::Value, Status> {
    if s.trim().is_empty() {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }
    serde_json::from_str(s).map_err(|e| invalid(format!("{what}: {e}")))
}

fn check_state(s: &str) -> Result<String, Status> {
    let v = json_or_null(s, "state_json")?;
    if !v.is_object() {
        return Err(invalid("state_json must be a JSON object"));
    }
    Ok(v.to_string())
}

impl Services {
    async fn interface(
        &self,
        space_id: SpaceId,
        id: ObjectId,
    ) -> Result<(Arc<SpaceStore>, Interface), Status> {
        let space = self.store.space(space_id).await?;
        let doc = space.load(id).await?;
        let o = doc.read().map_err(StoreError::from)?;
        if !ui::is_interface(&o) {
            return Err(Status::new(Code::NotFound, "not an interface"));
        }
        Ok((space, interface_of(&o, &doc).map_err(StoreError::from)?))
    }

    /// The draft itself, or the version this device selected for it.
    async fn resolved(
        &self,
        space_id: SpaceId,
        id: ObjectId,
    ) -> Result<(Arc<SpaceStore>, Interface, i32), Status> {
        let (space, iface) = self.interface(space_id, id).await?;
        if iface.version != 0 {
            let v = iface.version;
            return Ok((space, iface, v));
        }
        let selected =
            i32::try_from(self.store.setting(&ui::selected_key(id), 0).await?).unwrap_or(0);
        if selected == 0 {
            return Ok((space, iface, 0));
        }
        let versions = self.versions_of(&space, &iface.id).await?;
        match versions.into_iter().find(|v| v.version == selected) {
            Some(v) => Ok((space, v, selected)),
            None => Ok((space, iface, 0)),
        }
    }

    async fn versions_of(&self, space: &SpaceStore, draft: &str) -> Result<Vec<Interface>, Status> {
        let objects = space
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Interface),
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        let mut out = Vec::new();
        for o in objects {
            if crate::model::string_field(&o, ui::DRAFT) != Some(draft) {
                continue;
            }
            let doc = space
                .load(ObjectId::parse(&o.id).map_err(StoreError::from)?)
                .await?;
            out.push(interface_of(&o, &doc).map_err(StoreError::from)?);
        }
        out.sort_by_key(|v| std::cmp::Reverse(v.version));
        Ok(out)
    }
}

impl InterfaceServiceHandler for Services {
    async fn create_interface(
        &self,
        req: Request<CreateInterfaceRequest>,
    ) -> Result<Response<CreateInterfaceResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let name = m.name.trim();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        let state = check_state(&m.state_json)?;
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Interface, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        doc.set_field(ui::STATE, &string_value(state))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let space = self.store.space(space_id).await?;
        let rec = space.insert(&doc).await?;
        Ok(Response::new(CreateInterfaceResponse {
            interface: Some(interface_of(&rec, &doc).map_err(StoreError::from)?),
        }))
    }

    async fn get_interface(
        &self,
        req: Request<GetInterfaceRequest>,
    ) -> Result<Response<GetInterfaceResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let (_, iface, selected) = if req.msg.selected {
            self.resolved(space_id, id).await?
        } else {
            let (space, iface) = self.interface(space_id, id).await?;
            let selected =
                i32::try_from(self.store.setting(&ui::selected_key(id), 0).await?).unwrap_or(0);
            (space, iface, selected)
        };
        Ok(Response::new(GetInterfaceResponse {
            interface: Some(iface),
            selected_version: selected,
        }))
    }

    async fn list_interfaces(
        &self,
        req: Request<ListInterfacesRequest>,
    ) -> Result<Response<ListInterfacesResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let space = self.store.space(space_id).await?;
        if !req.msg.versions_of.is_empty() {
            parse_id(&req.msg.versions_of, "versions_of")?;
            return Ok(Response::new(ListInterfacesResponse {
                interfaces: self.versions_of(&space, &req.msg.versions_of).await?,
            }));
        }
        let objects = space
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Interface),
                include_deleted: req.msg.include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        let mut interfaces = Vec::new();
        for o in objects {
            if crate::model::string_field(&o, ui::DRAFT).is_some() {
                continue;
            }
            let doc = space
                .load(ObjectId::parse(&o.id).map_err(StoreError::from)?)
                .await?;
            interfaces.push(interface_of(&o, &doc).map_err(StoreError::from)?);
        }
        Ok(Response::new(ListInterfacesResponse { interfaces }))
    }

    async fn update_interface(
        &self,
        req: Request<UpdateInterfaceRequest>,
    ) -> Result<Response<UpdateInterfaceResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        let (space, iface) = self.interface(space_id, id).await?;
        if iface.version != 0 {
            return Err(Status::new(
                Code::FailedPrecondition,
                "a published version is fixed; edit the draft",
            ));
        }
        for n in &m.set_nodes {
            ui::check_node(n).map_err(invalid)?;
        }
        let name = m.name.as_deref().map(str::trim).map(str::to_owned);
        if name.as_deref() == Some("") {
            return Err(invalid("name must not be empty"));
        }
        let state = m.state_json.as_deref().map(check_state).transpose()?;
        let set = m.set_nodes.clone();
        let remove = m.remove_nodes.clone();
        let rec = space
            .update(id, move |doc| {
                if let Some(n) = name {
                    doc.set_field(reserved::NAME, &string_value(n))?;
                }
                if let Some(s) = state {
                    doc.set_field(ui::STATE, &string_value(s))?;
                }
                for n in &set {
                    let index = usize::try_from(n.index).unwrap_or(0);
                    if n.id.is_empty() {
                        doc.create_ui_node(&n.parent_id, index, n)?;
                    } else {
                        doc.set_ui_node(&n.id, &n.parent_id, index, n)?;
                    }
                }
                for n in &remove {
                    doc.remove_ui_node(n)?;
                }
                Ok(())
            })
            .await?;
        let doc = space.load(id).await?;
        Ok(Response::new(UpdateInterfaceResponse {
            interface: Some(interface_of(&rec, &doc).map_err(StoreError::from)?),
        }))
    }

    async fn publish_interface(
        &self,
        req: Request<PublishInterfaceRequest>,
    ) -> Result<Response<PublishInterfaceResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let (space, draft) = self.interface(space_id, id).await?;
        if draft.version != 0 {
            return Err(Status::new(
                Code::FailedPrecondition,
                "only a draft can be published",
            ));
        }
        let next = self
            .versions_of(&space, &draft.id)
            .await?
            .first()
            .map_or(1, |v| v.version + 1);
        // A copy with the same tree: node ids are fresh, parents are mapped.
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Interface, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(&draft.name))
            .map_err(StoreError::from)?;
        doc.set_field(ui::STATE, &string_value(&draft.state_json))
            .map_err(StoreError::from)?;
        doc.set_field(ui::VERSION, &int_value(i64::from(next)))
            .map_err(StoreError::from)?;
        doc.set_field(ui::DRAFT, &string_value(&draft.id))
            .map_err(StoreError::from)?;
        let mut mapped: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for n in &draft.nodes {
            let parent = if n.parent_id.is_empty() {
                String::new()
            } else {
                mapped.get(&n.parent_id).cloned().unwrap_or_default()
            };
            let new_id = doc
                .create_ui_node(&parent, usize::try_from(n.index).unwrap_or(0), n)
                .map_err(StoreError::from)?;
            mapped.insert(n.id.clone(), new_id);
        }
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = space.insert(&doc).await?;
        Ok(Response::new(PublishInterfaceResponse {
            version: Some(interface_of(&rec, &doc).map_err(StoreError::from)?),
        }))
    }

    async fn select_version(
        &self,
        req: Request<SelectVersionRequest>,
    ) -> Result<Response<SelectVersionResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let draft = parse_id(&req.msg.draft_id, "draft_id")?;
        let (space, iface) = self.interface(space_id, draft).await?;
        if iface.version != 0 {
            return Err(invalid("draft_id must name a draft"));
        }
        if req.msg.version != 0
            && !self
                .versions_of(&space, &iface.id)
                .await?
                .iter()
                .any(|v| v.version == req.msg.version)
        {
            return Err(Status::new(Code::NotFound, "no such version"));
        }
        self.store
            .set_setting(&ui::selected_key(draft), i64::from(req.msg.version))
            .await?;
        Ok(Response::new(SelectVersionResponse {
            version: req.msg.version,
        }))
    }

    async fn evaluate(
        &self,
        req: Request<EvaluateRequest>,
    ) -> Result<Response<EvaluateResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let (_, iface, _) = self.resolved(space_id, id).await?;
        let state = json_or_null(&req.msg.state_json, "state_json")?;
        let nodes = ui::evaluate(&self.scripts, space_id, &iface, &state).await;
        Ok(Response::new(EvaluateResponse { nodes }))
    }

    async fn dispatch(
        &self,
        req: Request<DispatchRequest>,
    ) -> Result<Response<DispatchResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        let (_, iface, _) = self.resolved(space_id, id).await?;
        let state = json_or_null(&m.state_json, "state_json")?;
        let payload = json_or_null(&m.payload_json, "payload_json")?;
        // Handlers are executable code: a grant for this version of the
        // interface, benign by construction, so auto-granted under policy.
        let Some(grant) = grant_or_auto(&self.store, space_id, &ui::as_script(&iface)).await?
        else {
            return Err(Status::new(
                Code::PermissionDenied,
                "this version of the interface has no grant on this device",
            ));
        };
        let (state_after, logs, error) = ui::dispatch(
            &self.scripts,
            space_id,
            &iface,
            grant.id,
            ui::UiEvent {
                node_id: &m.node_id,
                event: &m.event,
                state,
                payload,
            },
        )
        .await;
        Ok(Response::new(DispatchResponse {
            state_json: state_after.to_string(),
            logs,
            error,
        }))
    }
}
