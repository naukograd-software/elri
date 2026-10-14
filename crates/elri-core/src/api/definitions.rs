//! `DefinitionService`: typed access to field, type, and role definitions,
//! which are Objects underneath.

use std::sync::Arc;

use elri_proto::elri::v1::definition::definition_service_ws::{
    register_definition_service_arc, DefinitionServiceHandler,
};
use elri_proto::elri::v1::definition::{
    self as dpb, Constraint, CreateConstraintRequest, CreateConstraintResponse,
    CreateFieldDefinitionRequest, CreateFieldDefinitionResponse, CreateRoleDefinitionRequest,
    CreateRoleDefinitionResponse, CreateTypeDefinitionRequest, CreateTypeDefinitionResponse,
    GetSchemaRequest, GetSchemaResponse, ListConstraintsRequest, ListConstraintsResponse,
    ListFieldDefinitionsRequest, ListFieldDefinitionsResponse, ListPublicationsRequest,
    ListPublicationsResponse, ListRoleDefinitionsRequest, ListRoleDefinitionsResponse,
    ListTypeDefinitionsRequest, ListTypeDefinitionsResponse, ListViolationsRequest,
    ListViolationsResponse, PinDefinitionRequest, PinDefinitionResponse, Publication,
    PublishDefinitionRequest, PublishDefinitionResponse, UpdateConstraintRequest,
    UpdateConstraintResponse, UpdateFieldDefinitionRequest, UpdateFieldDefinitionResponse,
    UpdateTypeDefinitionRequest, UpdateTypeDefinitionResponse,
};
use ws_proto::{Request, Response, Server, Status};

use elri_proto::elri::v1::definition::FieldKind;
use elri_proto::elri::v1::object::{Object, ObjectKind};

use super::{invalid, parse_id, Services};
use crate::model::{
    bool_value, constraint_of, field_def_kind, int_value, is_definition, now_ms, publication_of,
    reserved, string_field, string_value, ModelError, ObjectDoc, ObjectId, SpaceId,
};
use crate::store::SpaceStore;
use crate::store::{ObjectFilter, StoreError};
use elri_proto::elri::v1::space::Authority;

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_definition_service_arc(server, services);
}

fn field_kind_arg(kind: i32) -> Result<FieldKind, Status> {
    FieldKind::try_from(kind)
        .ok()
        .filter(|k| *k != FieldKind::Unspecified)
        .ok_or_else(|| invalid("kind is required"))
}

fn name_of(o: &Object) -> String {
    string_field(o, reserved::NAME)
        .unwrap_or_default()
        .to_owned()
}

fn description_of(o: &Object) -> String {
    string_field(o, reserved::DESCRIPTION)
        .unwrap_or_default()
        .to_owned()
}

fn field_def_to_pb(o: &Object) -> dpb::FieldDefinition {
    dpb::FieldDefinition {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: name_of(o),
        description: description_of(o),
        kind: field_def_kind(o).unwrap_or(FieldKind::Unspecified).into(),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

fn type_def_to_pb(o: &Object) -> dpb::TypeDefinition {
    dpb::TypeDefinition {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: name_of(o),
        description: description_of(o),
        fields: o.type_fields.clone(),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

fn role_def_to_pb(o: &Object) -> dpb::RoleDefinition {
    dpb::RoleDefinition {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: name_of(o),
        description: description_of(o),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

fn require_name(name: &str) -> Result<&str, Status> {
    let n = name.trim();
    if n.is_empty() {
        return Err(invalid("name is required"));
    }
    Ok(n)
}

fn check_type_fields(fields: &[dpb::TypeField]) -> Result<(), Status> {
    if fields.iter().any(|f| f.field_id.is_empty()) {
        return Err(invalid("field id must not be empty"));
    }
    Ok(())
}

impl Services {
    async fn create_definition(
        &self,
        space_id: &str,
        kind: ObjectKind,
        name: &str,
        description: &str,
        fill: impl FnOnce(&ObjectDoc) -> Result<(), ModelError>,
    ) -> Result<Object, Status> {
        let space_id = parse_id(space_id, "space_id")?;
        let name = require_name(name)?;
        let space = self.store.space(space_id).await?;
        let doc = ObjectDoc::create(ObjectId::new(), space_id, kind, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        if !description.trim().is_empty() {
            doc.set_field(reserved::DESCRIPTION, &string_value(description.trim()))
                .map_err(StoreError::from)?;
        }
        fill(&doc).map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        Ok(space.insert(&doc).await?)
    }

    async fn list_definitions(
        &self,
        space_id: &str,
        kind: ObjectKind,
        include_deleted: bool,
    ) -> Result<Vec<Object>, Status> {
        let space_id = parse_id(space_id, "space_id")?;
        let filter = ObjectFilter {
            kind: Some(kind),
            include_deleted,
            limit: 1000,
            ..ObjectFilter::default()
        };
        Ok(self.store.space(space_id).await?.list(&filter).await?)
    }

    async fn update_definition(
        &self,
        space_id: &str,
        id: &str,
        kind: ObjectKind,
        name: Option<&str>,
        description: Option<&str>,
        edit: impl FnOnce(&ObjectDoc) -> Result<(), ModelError>,
    ) -> Result<Object, Status> {
        let space_id = parse_id(space_id, "space_id")?;
        let id = parse_id(id, "id")?;
        let name = name.map(require_name).transpose()?.map(str::to_owned);
        let space = self.store.space(space_id).await?;
        if space.get(id).await?.kind() != kind {
            return Err(invalid("id does not name a definition of this kind"));
        }
        let description = description.map(|d| d.trim().to_owned());
        Ok(space
            .update(id, |doc| {
                if let Some(n) = name {
                    doc.set_field(reserved::NAME, &string_value(n))?;
                }
                if let Some(d) = description {
                    if d.is_empty() {
                        doc.remove_field(reserved::DESCRIPTION)?;
                    } else {
                        doc.set_field(reserved::DESCRIPTION, &string_value(d))?;
                    }
                }
                edit(doc)
            })
            .await?)
    }
}

fn canonical(v: &serde_json::Value) -> String {
    // serde_json keeps object keys sorted when built from a BTreeMap; the
    // snapshots below are built that way, so this is canonical.
    v.to_string()
}

fn digest(snapshot: &str, deps: &std::collections::BTreeMap<String, i32>) -> String {
    use sha2::Digest as _;
    let mut h = sha2::Sha256::new();
    h.update(snapshot.as_bytes());
    h.update(b"\0");
    h.update(serde_json::to_string(deps).unwrap_or_default().as_bytes());
    h.finalize()
        .iter()
        .fold(String::with_capacity(64), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

impl Services {
    async fn publications(
        &self,
        space: &SpaceStore,
        of: Option<&str>,
    ) -> Result<Vec<Publication>, Status> {
        let objects = space
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Publication),
                limit: 10_000,
                ..ObjectFilter::default()
            })
            .await?;
        let mut out: Vec<Publication> = objects
            .iter()
            .filter_map(publication_of)
            .filter(|p| of.is_none_or(|id| p.definition_id == id))
            .collect();
        out.sort_by(|a, b| {
            a.definition_id
                .cmp(&b.definition_id)
                .then(b.version.cmp(&a.version))
        });
        Ok(out)
    }

    /// Publishes a definition as its next version. A type definition's
    /// fields are published first when they have no publication yet, so
    /// the dependencies are exact.
    async fn publish(
        &self,
        space: &SpaceStore,
        space_id: SpaceId,
        id: ObjectId,
    ) -> Result<Publication, Status> {
        let def = space.get(id).await?;
        if !is_definition(def.kind()) {
            return Err(invalid("id does not name a definition"));
        }
        let mut deps: std::collections::BTreeMap<String, i32> = std::collections::BTreeMap::new();
        let mut snapshot = serde_json::Map::new();
        snapshot.insert("name".into(), serde_json::Value::String(name_of(&def)));
        snapshot.insert(
            "description".into(),
            serde_json::Value::String(description_of(&def)),
        );
        snapshot.insert(
            "kind".into(),
            serde_json::Value::String(def.kind().as_str_name().to_lowercase()),
        );
        match def.kind() {
            ObjectKind::FieldDef => {
                let kind = field_def_kind(&def).unwrap_or(FieldKind::Unspecified);
                snapshot.insert("kind".into(), serde_json::Value::from(kind as i32));
                snapshot.insert(
                    "kind_name".into(),
                    serde_json::Value::String(kind.as_str_name().to_lowercase()),
                );
            }
            ObjectKind::TypeDef => {
                let mut fields = Vec::new();
                for f in &def.type_fields {
                    // Only a field definition is a dependency; anything
                    // else the id names (a type, say) is free-form here, so
                    // publication cannot chase a cycle of types.
                    let is_field = match ObjectId::parse(&f.field_id) {
                        Ok(fid) => space
                            .get(fid)
                            .await
                            .is_ok_and(|o| o.kind() == ObjectKind::FieldDef),
                        Err(_) => false,
                    };
                    let version = match ObjectId::parse(&f.field_id) {
                        Ok(fid) if is_field => {
                            let latest = self
                                .publications(space, Some(&f.field_id))
                                .await?
                                .into_iter()
                                .next();
                            let p = match latest {
                                Some(p) => p,
                                None => Box::pin(self.publish(space, space_id, fid)).await?,
                            };
                            deps.insert(f.field_id.clone(), p.version);
                            p.version
                        }
                        // A field id without a definition in this Space is
                        // free-form: nothing to pin.
                        _ => 0,
                    };
                    let mut entry = serde_json::Map::new();
                    entry.insert(
                        "field_id".into(),
                        serde_json::Value::String(f.field_id.clone()),
                    );
                    entry.insert("required".into(), serde_json::Value::Bool(f.required));
                    entry.insert("version".into(), serde_json::Value::from(version));
                    fields.push(serde_json::Value::Object(entry));
                }
                snapshot.insert("fields".into(), serde_json::Value::Array(fields));
            }
            _ => {}
        }
        let snapshot: serde_json::Value = serde_json::Value::Object(
            snapshot
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        );
        let snapshot_json = canonical(&snapshot);
        let next = self
            .publications(space, Some(&def.id))
            .await?
            .first()
            .map_or(1, |p| p.version + 1);
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Publication, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::OF, &string_value(&def.id))
            .map_err(StoreError::from)?;
        doc.set_field(reserved::VERSION, &int_value(i64::from(next)))
            .map_err(StoreError::from)?;
        doc.set_field(reserved::SNAPSHOT, &string_value(&snapshot_json))
            .map_err(StoreError::from)?;
        doc.set_field(
            reserved::DEPS,
            &string_value(serde_json::to_string(&deps).unwrap_or_default()),
        )
        .map_err(StoreError::from)?;
        doc.set_field(
            reserved::DIGEST,
            &string_value(digest(&snapshot_json, &deps)),
        )
        .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = space.insert(&doc).await?;
        publication_of(&rec).ok_or_else(|| Status::new(ws_proto::Code::Internal, "publication"))
    }
}

impl Services {
    async fn constraint_object(
        &self,
        space_id: SpaceId,
        id: ObjectId,
    ) -> Result<Constraint, Status> {
        let o = self.store.space(space_id).await?.get(id).await?;
        constraint_of(&o).ok_or_else(|| Status::new(ws_proto::Code::NotFound, "not a constraint"))
    }
}

impl DefinitionServiceHandler for Services {
    async fn create_constraint(
        &self,
        req: Request<CreateConstraintRequest>,
    ) -> Result<Response<CreateConstraintResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let name = require_name(&m.name)?;
        let field = parse_id(&m.field_id, "field_id")?;
        if !m.unique {
            return Err(invalid("a constraint must constrain something: unique"));
        }
        let space = self.store.space(space_id).await?;
        if space.field_kind(&m.field_id).await?.is_none() {
            return Err(invalid("field_id must name a live field definition"));
        }
        let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Constraint, now_ms())
            .map_err(StoreError::from)?;
        doc.set_field(reserved::NAME, &string_value(name))
            .map_err(StoreError::from)?;
        doc.set_field(reserved::FIELD, &string_value(field.to_string()))
            .map_err(StoreError::from)?;
        doc.set_field(reserved::UNIQUE, &bool_value(true))
            .map_err(StoreError::from)?;
        doc.set_field(reserved::STRICT, &bool_value(m.strict))
            .map_err(StoreError::from)?;
        doc.touch(now_ms()).map_err(StoreError::from)?;
        let rec = space.insert(&doc).await?;
        Ok(Response::new(CreateConstraintResponse {
            constraint: constraint_of(&rec),
        }))
    }

    async fn update_constraint(
        &self,
        req: Request<UpdateConstraintRequest>,
    ) -> Result<Response<UpdateConstraintResponse>, Status> {
        let m = &req.msg;
        let space_id = parse_id(&m.space_id, "space_id")?;
        let id = parse_id(&m.id, "id")?;
        self.constraint_object(space_id, id).await?;
        let name = m
            .name
            .as_deref()
            .map(require_name)
            .transpose()?
            .map(str::to_owned);
        let strict = m.strict;
        let rec = self
            .store
            .space(space_id)
            .await?
            .update(id, move |doc| {
                if let Some(n) = name {
                    doc.set_field(reserved::NAME, &string_value(n))?;
                }
                if let Some(s) = strict {
                    doc.set_field(reserved::STRICT, &bool_value(s))?;
                }
                Ok(())
            })
            .await?;
        Ok(Response::new(UpdateConstraintResponse {
            constraint: constraint_of(&rec),
        }))
    }

    async fn list_constraints(
        &self,
        req: Request<ListConstraintsRequest>,
    ) -> Result<Response<ListConstraintsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let objects = self
            .store
            .space(space_id)
            .await?
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Constraint),
                include_deleted: req.msg.include_deleted,
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        Ok(Response::new(ListConstraintsResponse {
            constraints: objects.iter().filter_map(constraint_of).collect(),
        }))
    }

    async fn list_violations(
        &self,
        req: Request<ListViolationsRequest>,
    ) -> Result<Response<ListViolationsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        Ok(Response::new(ListViolationsResponse {
            violations: self.store.space(space_id).await?.violations().await?,
        }))
    }

    async fn publish_definition(
        &self,
        req: Request<PublishDefinitionRequest>,
    ) -> Result<Response<PublishDefinitionResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let id = parse_id(&req.msg.id, "id")?;
        let space = self.store.space(space_id).await?;
        let publication = self.publish(&space, space_id, id).await?;
        Ok(Response::new(PublishDefinitionResponse {
            publication: Some(publication),
        }))
    }

    async fn list_publications(
        &self,
        req: Request<ListPublicationsRequest>,
    ) -> Result<Response<ListPublicationsResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let space = self.store.space(space_id).await?;
        let of = if req.msg.definition_id.is_empty() {
            None
        } else {
            Some(req.msg.definition_id.as_str())
        };
        Ok(Response::new(ListPublicationsResponse {
            publications: self.publications(&space, of).await?,
        }))
    }

    async fn pin_definition(
        &self,
        req: Request<PinDefinitionRequest>,
    ) -> Result<Response<PinDefinitionResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        let publication = parse_id(&req.msg.publication_id, "publication_id")?;
        let authority = self
            .store
            .list_spaces()
            .await?
            .into_iter()
            .find(|s| s.id == space_id.to_string())
            .map_or(Authority::Local, |s| s.authority());
        let pin = if authority == Authority::Server {
            self.remote
                .pin_definition(space_id, publication, req.msg.expected_epoch)
                .await?
        } else {
            self.store
                .space(space_id)
                .await?
                .pin_definition(publication, req.msg.expected_epoch)
                .await?
        };
        Ok(Response::new(PinDefinitionResponse { pin: Some(pin) }))
    }

    async fn get_schema(
        &self,
        req: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let space_id = parse_id(&req.msg.space_id, "space_id")?;
        Ok(Response::new(GetSchemaResponse {
            schema: Some(self.store.space(space_id).await?.schema().await?),
        }))
    }

    async fn create_field_definition(
        &self,
        req: Request<CreateFieldDefinitionRequest>,
    ) -> Result<Response<CreateFieldDefinitionResponse>, Status> {
        let m = &req.msg;
        let kind = field_kind_arg(m.kind)?;
        let rec = self
            .create_definition(
                &m.space_id,
                ObjectKind::FieldDef,
                &m.name,
                &m.description,
                |doc| doc.set_field(reserved::FIELD_KIND, &int_value(i64::from(kind as i32))),
            )
            .await?;
        Ok(Response::new(CreateFieldDefinitionResponse {
            definition: Some(field_def_to_pb(&rec)),
        }))
    }

    async fn update_field_definition(
        &self,
        req: Request<UpdateFieldDefinitionRequest>,
    ) -> Result<Response<UpdateFieldDefinitionResponse>, Status> {
        let m = &req.msg;
        let rec = self
            .update_definition(
                &m.space_id,
                &m.id,
                ObjectKind::FieldDef,
                m.name.as_deref(),
                m.description.as_deref(),
                |_| Ok(()),
            )
            .await?;
        Ok(Response::new(UpdateFieldDefinitionResponse {
            definition: Some(field_def_to_pb(&rec)),
        }))
    }

    async fn list_field_definitions(
        &self,
        req: Request<ListFieldDefinitionsRequest>,
    ) -> Result<Response<ListFieldDefinitionsResponse>, Status> {
        let defs = self
            .list_definitions(
                &req.msg.space_id,
                ObjectKind::FieldDef,
                req.msg.include_deleted,
            )
            .await?;
        Ok(Response::new(ListFieldDefinitionsResponse {
            definitions: defs.iter().map(field_def_to_pb).collect(),
        }))
    }

    async fn create_type_definition(
        &self,
        req: Request<CreateTypeDefinitionRequest>,
    ) -> Result<Response<CreateTypeDefinitionResponse>, Status> {
        let m = &req.msg;
        check_type_fields(&m.fields)?;
        let fields = m.fields.clone();
        let rec = self
            .create_definition(
                &m.space_id,
                ObjectKind::TypeDef,
                &m.name,
                &m.description,
                |doc| {
                    for f in &fields {
                        doc.set_type_field(&f.field_id, f.required)?;
                    }
                    Ok(())
                },
            )
            .await?;
        Ok(Response::new(CreateTypeDefinitionResponse {
            definition: Some(type_def_to_pb(&rec)),
        }))
    }

    async fn update_type_definition(
        &self,
        req: Request<UpdateTypeDefinitionRequest>,
    ) -> Result<Response<UpdateTypeDefinitionResponse>, Status> {
        let m = &req.msg;
        check_type_fields(&m.set_fields)?;
        let set = m.set_fields.clone();
        let remove = m.remove_fields.clone();
        let rec = self
            .update_definition(
                &m.space_id,
                &m.id,
                ObjectKind::TypeDef,
                m.name.as_deref(),
                m.description.as_deref(),
                |doc| {
                    for f in &set {
                        doc.set_type_field(&f.field_id, f.required)?;
                    }
                    for f in &remove {
                        doc.remove_type_field(f)?;
                    }
                    Ok(())
                },
            )
            .await?;
        Ok(Response::new(UpdateTypeDefinitionResponse {
            definition: Some(type_def_to_pb(&rec)),
        }))
    }

    async fn list_type_definitions(
        &self,
        req: Request<ListTypeDefinitionsRequest>,
    ) -> Result<Response<ListTypeDefinitionsResponse>, Status> {
        let defs = self
            .list_definitions(
                &req.msg.space_id,
                ObjectKind::TypeDef,
                req.msg.include_deleted,
            )
            .await?;
        Ok(Response::new(ListTypeDefinitionsResponse {
            definitions: defs.iter().map(type_def_to_pb).collect(),
        }))
    }

    async fn create_role_definition(
        &self,
        req: Request<CreateRoleDefinitionRequest>,
    ) -> Result<Response<CreateRoleDefinitionResponse>, Status> {
        let m = &req.msg;
        let rec = self
            .create_definition(
                &m.space_id,
                ObjectKind::RoleDef,
                &m.name,
                &m.description,
                |_| Ok(()),
            )
            .await?;
        Ok(Response::new(CreateRoleDefinitionResponse {
            definition: Some(role_def_to_pb(&rec)),
        }))
    }

    async fn list_role_definitions(
        &self,
        req: Request<ListRoleDefinitionsRequest>,
    ) -> Result<Response<ListRoleDefinitionsResponse>, Status> {
        let defs = self
            .list_definitions(
                &req.msg.space_id,
                ObjectKind::RoleDef,
                req.msg.include_deleted,
            )
            .await?;
        Ok(Response::new(ListRoleDefinitionsResponse {
            definitions: defs.iter().map(role_def_to_pb).collect(),
        }))
    }
}
