//! `DefinitionService`: typed access to field, type, and role definitions,
//! which are Objects underneath.

use std::sync::Arc;

use elri_proto::elri::v1::definition::definition_service_ws::{
    register_definition_service_arc, DefinitionServiceHandler,
};
use elri_proto::elri::v1::definition::{
    self as dpb, CreateFieldDefinitionRequest, CreateFieldDefinitionResponse,
    CreateRoleDefinitionRequest, CreateRoleDefinitionResponse, CreateTypeDefinitionRequest,
    CreateTypeDefinitionResponse, ListFieldDefinitionsRequest, ListFieldDefinitionsResponse,
    ListRoleDefinitionsRequest, ListRoleDefinitionsResponse, ListTypeDefinitionsRequest,
    ListTypeDefinitionsResponse, UpdateFieldDefinitionRequest, UpdateFieldDefinitionResponse,
    UpdateTypeDefinitionRequest, UpdateTypeDefinitionResponse,
};
use ws_proto::{Request, Response, Server, Status};

use elri_proto::elri::v1::definition::FieldKind;
use elri_proto::elri::v1::object::{Object, ObjectKind};

use super::{invalid, parse_id, Services};
use crate::model::{
    field_def_kind, int_value, now_ms, reserved, string_field, string_value, ModelError, ObjectDoc,
    ObjectId,
};
use crate::store::{ObjectFilter, StoreError};

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

impl DefinitionServiceHandler for Services {
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
