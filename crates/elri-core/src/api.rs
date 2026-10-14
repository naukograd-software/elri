//! ws-proto surface of the core. Every client goes through the same server:
//! a token middleware first, then the typed service handlers.

use std::collections::HashMap;
use std::sync::Arc;

use elri_proto::elri::v1::change::change_service_ws::{
    register_change_service_arc, ChangeServiceHandler,
};
use elri_proto::elri::v1::change::{self as cpb, ListChangesRequest, ListChangesResponse};
use elri_proto::elri::v1::core::core_service_ws::{register_core_service_arc, CoreServiceHandler};
use elri_proto::elri::v1::core::{EndpointKind, GetInfoRequest, GetInfoResponse};
use elri_proto::elri::v1::object::object_service_ws::{
    register_object_service_arc, ObjectServiceHandler,
};
use elri_proto::elri::v1::object::{
    self as pb, CreateObjectRequest, CreateObjectResponse, DeleteObjectRequest,
    DeleteObjectResponse, GetObjectRequest, GetObjectResponse, ListObjectsRequest,
    ListObjectsResponse, UpdateObjectRequest, UpdateObjectResponse,
};
use elri_proto::elri::v1::space::space_service_ws::{
    register_space_service_arc, SpaceServiceHandler,
};
use elri_proto::elri::v1::space::{
    self as spb, CreateSpaceRequest, CreateSpaceResponse, ListSpacesRequest, ListSpacesResponse,
};
use ws_proto::server::{Handler, OriginPolicy};
use ws_proto::{Code, Request, Response, Server, ServerConfig, Status};

use crate::change::{Change, ChangeStatus};
use crate::device::{DeviceId, LaunchToken};
use crate::model::{now_ms, FieldValue, ModelError, ObjectDoc, ObjectId, ObjectKind, ObjectRecord};
use crate::store::{Authority, ObjectFilter, SpaceInfo, Store, StoreError};
use crate::BuildInfo;

/// Everything the handlers need.
pub struct Services {
    /// Version of the embedding binary.
    pub build: BuildInfo,
    /// This device.
    pub device_id: DeviceId,
    /// Token every request must carry.
    pub token: LaunchToken,
    /// Storage.
    pub store: Arc<Store>,
}

/// Builds the loopback server: same-origin policy, bearer-token middleware,
/// all services registered.
pub fn build_server(services: Services) -> Server {
    let server = Server::new(ServerConfig {
        origin: OriginPolicy::same_origin_only(),
        ..ServerConfig::default()
    });
    let token = services.token.clone();
    server.add_middleware(move |next: Handler| require_token(token.clone(), next));

    let services = Arc::new(services);
    register_core_service_arc(&server, Arc::clone(&services));
    register_space_service_arc(&server, Arc::clone(&services));
    register_object_service_arc(&server, Arc::clone(&services));
    register_change_service_arc(&server, services);
    server
}

fn require_token(token: LaunchToken, next: Handler) -> Handler {
    Arc::new(move |stream| {
        let next = Arc::clone(&next);
        let presented = stream.header().get("authorization").cloned();
        let ok = presented
            .as_deref()
            .is_some_and(|h| token.matches_header(h));
        Box::pin(async move {
            if ok {
                next(stream).await
            } else {
                Err(Status::new(
                    Code::Unauthenticated,
                    "missing or invalid token",
                ))
            }
        })
    })
}

// --- error mapping -----------------------------------------------------------

impl From<StoreError> for Status {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::SpaceNotFound(_) | StoreError::ObjectNotFound(_) => {
                Status::new(Code::NotFound, e.to_string())
            }
            StoreError::Model(ModelError::InvalidId(_)) => {
                Status::new(Code::InvalidArgument, e.to_string())
            }
            other => {
                tracing::error!(error = %other, "storage failure");
                Status::new(Code::Internal, "storage failure")
            }
        }
    }
}

fn invalid(msg: impl Into<String>) -> Status {
    Status::new(Code::InvalidArgument, msg)
}

fn parse_id(s: &str, what: &str) -> Result<ObjectId, Status> {
    ObjectId::parse(s).map_err(|_| invalid(format!("{what}: not a valid id")))
}

// --- proto conversions -------------------------------------------------------

fn space_to_pb(s: &SpaceInfo) -> spb::Space {
    spb::Space {
        id: s.id.to_string(),
        name: s.name.clone(),
        role: spb::Role::Owner.into(),
        authority: match s.authority {
            Authority::Local => spb::Authority::Local,
            Authority::Server => spb::Authority::Server,
        }
        .into(),
    }
}

fn kind_from_pb(kind: i32) -> Result<ObjectKind, Status> {
    match pb::ObjectKind::try_from(kind) {
        Ok(pb::ObjectKind::Entity) => Ok(ObjectKind::Entity),
        Ok(pb::ObjectKind::Relation) => Ok(ObjectKind::Relation),
        Ok(pb::ObjectKind::Layer) => Ok(ObjectKind::Layer),
        _ => Err(invalid("kind is required")),
    }
}

fn kind_to_pb(kind: ObjectKind) -> pb::ObjectKind {
    match kind {
        ObjectKind::Entity => pb::ObjectKind::Entity,
        ObjectKind::Relation => pb::ObjectKind::Relation,
        ObjectKind::Layer => pb::ObjectKind::Layer,
    }
}

fn value_from_pb(field: &str, v: &pb::Value) -> Result<FieldValue, Status> {
    use pb::value::Value as V;
    match &v.value {
        Some(V::Bool(b)) => Ok(FieldValue::Bool(*b)),
        Some(V::Int(i)) => Ok(FieldValue::Int(*i)),
        Some(V::Double(d)) => Ok(FieldValue::Double(*d)),
        Some(V::String(s)) => Ok(FieldValue::String(s.clone())),
        Some(V::TimeMs(t)) => Ok(FieldValue::TimeMs(*t)),
        Some(V::Ref(r)) => Ok(FieldValue::Ref(parse_id(r, &format!("field {field}"))?)),
        None => Err(invalid(format!("field {field}: value is empty"))),
    }
}

fn value_to_pb(v: &FieldValue) -> pb::Value {
    use pb::value::Value as V;
    pb::Value {
        value: Some(match v {
            FieldValue::Bool(b) => V::Bool(*b),
            FieldValue::Int(i) => V::Int(*i),
            FieldValue::Double(d) => V::Double(*d),
            FieldValue::String(s) => V::String(s.clone()),
            FieldValue::TimeMs(t) => V::TimeMs(*t),
            FieldValue::Ref(r) => V::Ref(r.to_string()),
        }),
    }
}

fn object_to_pb(o: &ObjectRecord) -> pb::Object {
    pb::Object {
        id: o.id.to_string(),
        space_id: o.space.to_string(),
        kind: kind_to_pb(o.kind).into(),
        types: o.types.clone(),
        fields: o
            .fields
            .iter()
            .map(|(k, v)| (k.clone(), value_to_pb(v)))
            .collect(),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

fn fields_from_pb(
    fields: &HashMap<String, pb::Value>,
) -> Result<Vec<(String, FieldValue)>, Status> {
    let mut out = Vec::with_capacity(fields.len());
    for (k, v) in fields {
        if k.is_empty() {
            return Err(invalid("field id must not be empty"));
        }
        out.push((k.clone(), value_from_pb(k, v)?));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn change_to_pb(space: &str, c: &Change) -> cpb::Change {
    cpb::Change {
        id: c.id.to_string(),
        space_id: space.to_owned(),
        device_id: c.device_id.clone(),
        created_at_ms: c.created_at_ms,
        status: match c.status {
            ChangeStatus::Local => cpb::ChangeStatus::Local,
            ChangeStatus::Queued => cpb::ChangeStatus::Queued,
            ChangeStatus::Waiting => cpb::ChangeStatus::Waiting,
            ChangeStatus::Accepted => cpb::ChangeStatus::Accepted,
            ChangeStatus::Rejected => cpb::ChangeStatus::Rejected,
            ChangeStatus::Paused => cpb::ChangeStatus::Paused,
        }
        .into(),
        parts: c
            .parts
            .iter()
            .map(|p| cpb::ChangePart {
                object_id: p.object_id.to_string(),
                bytes_len: u32::try_from(p.bytes.len()).unwrap_or(u32::MAX),
            })
            .collect(),
    }
}

// --- handlers ----------------------------------------------------------------

impl CoreServiceHandler for Services {
    async fn get_info(
        &self,
        _req: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            version: self.build.version.to_owned(),
            commit: self.build.commit.to_owned(),
            kind: EndpointKind::LocalCore.into(),
            device_id: self.device_id.as_string(),
        }))
    }
}

impl SpaceServiceHandler for Services {
    async fn list_spaces(
        &self,
        _req: Request<ListSpacesRequest>,
    ) -> Result<Response<ListSpacesResponse>, Status> {
        let spaces = self.store.list_spaces().await?;
        Ok(Response::new(ListSpacesResponse {
            spaces: spaces.iter().map(space_to_pb).collect(),
        }))
    }

    async fn create_space(
        &self,
        req: Request<CreateSpaceRequest>,
    ) -> Result<Response<CreateSpaceResponse>, Status> {
        let name = req.msg.name.trim();
        if name.is_empty() {
            return Err(invalid("name is required"));
        }
        let space = self.store.create_space(name).await?;
        Ok(Response::new(CreateSpaceResponse {
            space: Some(space_to_pb(&space)),
        }))
    }
}

impl ObjectServiceHandler for Services {
    async fn create_object(
        &self,
        req: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let kind = kind_from_pb(m.kind)?;
        let fields = fields_from_pb(&m.fields)?;
        let space = self.store.space(space_id).await?;

        let doc = ObjectDoc::create(ObjectId::new(), space_id, kind, now_ms())
            .map_err(StoreError::from)?;
        for t in &m.types {
            doc.add_type(t).map_err(StoreError::from)?;
        }
        for (k, v) in &fields {
            doc.set_field(k, v).map_err(StoreError::from)?;
        }
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = space.insert(&doc).await?;
        Ok(Response::new(CreateObjectResponse {
            object: Some(object_to_pb(&rec)),
        }))
    }

    async fn get_object(
        &self,
        req: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let rec = self.store.space(space_id).await?.get(id).await?;
        Ok(Response::new(GetObjectResponse {
            object: Some(object_to_pb(&rec)),
        }))
    }

    async fn update_object(
        &self,
        req: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        let set = fields_from_pb(&m.set_fields)?;
        let space = self.store.space(space_id).await?;
        let rec = space
            .update(id, |doc| {
                for (k, v) in &set {
                    doc.set_field(k, v)?;
                }
                for k in &m.remove_fields {
                    doc.remove_field(k)?;
                }
                for t in &m.add_types {
                    doc.add_type(t)?;
                }
                for t in &m.remove_types {
                    doc.remove_type(t)?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdateObjectResponse {
            object: Some(object_to_pb(&rec)),
        }))
    }

    async fn delete_object(
        &self,
        req: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let space = self.store.space(space_id).await?;
        space.update(id, |doc| doc.mark_deleted(now_ms())).await?;
        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn list_objects(
        &self,
        req: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let kind = match pb::ObjectKind::try_from(m.kind) {
            Ok(pb::ObjectKind::Unspecified) | Err(_) => None,
            Ok(_) => Some(kind_from_pb(m.kind)?),
        };
        let filter = ObjectFilter {
            kind,
            type_id: (!m.type_id.is_empty()).then(|| m.type_id.clone()),
            include_deleted: m.include_deleted,
            limit: usize::try_from(m.limit).unwrap_or(0),
        };
        let objects = self.store.space(space_id).await?.list(&filter).await?;
        Ok(Response::new(ListObjectsResponse {
            objects: objects.iter().map(object_to_pb).collect(),
        }))
    }
}

impl ChangeServiceHandler for Services {
    async fn list_changes(
        &self,
        req: Request<ListChangesRequest>,
    ) -> Result<Response<ListChangesResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let limit = usize::try_from(req.msg.limit).unwrap_or(0);
        let changes = self
            .store
            .space(space_id)
            .await?
            .list_changes(limit)
            .await?;
        Ok(Response::new(ListChangesResponse {
            changes: changes
                .iter()
                .map(|c| change_to_pb(&req.msg.space_id, c))
                .collect(),
        }))
    }
}
