//! `ExtensionService`: install, grant, and call WASM and native extensions.

use std::sync::Arc;

use elri_proto::elri::v1::extension::extension_service_ws::{
    register_extension_service_arc, ExtensionServiceHandler, InstallExtensionServerStream,
};
use elri_proto::elri::v1::extension::{
    install_extension_request, CallExtensionRequest, CallExtensionResponse, ExtensionKind,
    GrantExtensionRequest, GrantExtensionResponse, InstallExtensionResponse, ListExtensionsRequest,
    ListExtensionsResponse, RemoveExtensionRequest, RemoveExtensionResponse,
};
use ws_proto::{Code, Request, Response, Server, Status};

use super::{invalid, parse_id, Services};
use crate::extension::{self, ExtensionError, MAX_MODULE_BYTES};
use crate::model::{now_ms, ObjectId};
use crate::rules::grant_or_auto;
use crate::script;
use crate::store::ExtensionRecord;

pub(super) fn register(server: &Server, services: Arc<Services>) {
    register_extension_service_arc(server, services);
}

impl From<ExtensionError> for Status {
    fn from(e: ExtensionError) -> Self {
        match e {
            ExtensionError::Store(s) => s.into(),
            ExtensionError::Module(_) => Status::new(Code::InvalidArgument, e.to_string()),
            ExtensionError::NotFound(_) => Status::new(Code::NotFound, e.to_string()),
            ExtensionError::NativeUnsupported => {
                Status::new(Code::FailedPrecondition, e.to_string())
            }
            ExtensionError::Run(_) | ExtensionError::Io { .. } => {
                tracing::error!(error = %e, "extension failure");
                Status::new(Code::Internal, e.to_string())
            }
        }
    }
}

impl Services {
    async fn installed(&self, name: &str) -> Result<ExtensionRecord, Status> {
        self.store.extension(name).await?.ok_or_else(|| {
            Status::new(Code::NotFound, format!("extension {name} is not installed"))
        })
    }

    async fn is_granted(&self, rec: &ExtensionRecord) -> Result<bool, Status> {
        Ok(self
            .store
            .grant_for(extension::grant_identity(&rec.name), &rec.version)
            .await?
            .is_some())
    }
}

impl ExtensionServiceHandler for Services {
    async fn install_extension(
        &self,
        stream: InstallExtensionServerStream,
    ) -> Result<Response<InstallExtensionResponse>, Status> {
        let header = match stream.recv().await? {
            Some(m) => match m.request {
                Some(install_extension_request::Request::Header(h)) => h,
                _ => return Err(invalid("the first message must be the header")),
            },
            None => return Err(invalid("empty install")),
        };
        extension::check_name(&header.name)?;
        let kind = ExtensionKind::try_from(header.kind)
            .ok()
            .filter(|k| *k != ExtensionKind::Unspecified)
            .ok_or_else(|| invalid("kind is required"))?;
        if kind == ExtensionKind::Native && !extension::native_supported() {
            return Err(ExtensionError::NativeUnsupported.into());
        }
        if header.functions.is_empty() {
            return Err(invalid("at least one function is required"));
        }
        let mut bytes = Vec::new();
        while let Some(m) = stream.recv().await? {
            match m.request {
                Some(install_extension_request::Request::Chunk(c)) => {
                    bytes.extend_from_slice(&c);
                    if bytes.len() > MAX_MODULE_BYTES {
                        return Err(invalid("module too large"));
                    }
                }
                Some(install_extension_request::Request::Header(_)) => {
                    return Err(invalid("a second header"))
                }
                None => {}
            }
        }
        if bytes.is_empty() {
            return Err(invalid("module bytes are required"));
        }
        if kind == ExtensionKind::Wasm {
            extension::check_wasm(&bytes, &header.functions)?;
        }
        let version = self.modules.store(&header.name, kind, &bytes)?;
        let manifest = header.manifest.unwrap_or_default();
        let rec = ExtensionRecord {
            name: header.name.clone(),
            kind: i64::from(kind as i32),
            version,
            functions: serde_json::to_string(&header.functions).unwrap_or_default(),
            manifest: script::manifest_json(&manifest),
            installed_at: now_ms(),
        };
        self.store.put_extension(rec.clone()).await?;
        let granted = self.is_granted(&rec).await?;
        Ok(Response::new(InstallExtensionResponse {
            extension: Some(extension::extension_of(&rec, granted)),
        }))
    }

    async fn list_extensions(
        &self,
        _req: Request<ListExtensionsRequest>,
    ) -> Result<Response<ListExtensionsResponse>, Status> {
        let mut out = Vec::new();
        for rec in self.store.extensions().await? {
            let granted = self.is_granted(&rec).await?;
            out.push(extension::extension_of(&rec, granted));
        }
        Ok(Response::new(ListExtensionsResponse { extensions: out }))
    }

    async fn remove_extension(
        &self,
        req: Request<RemoveExtensionRequest>,
    ) -> Result<Response<RemoveExtensionResponse>, Status> {
        extension::check_name(&req.msg.name)?;
        if !self.store.remove_extension(&req.msg.name).await? {
            return Err(Status::new(Code::NotFound, "no such extension"));
        }
        self.modules.remove(&req.msg.name)?;
        Ok(Response::new(RemoveExtensionResponse {}))
    }

    async fn grant_extension(
        &self,
        req: Request<GrantExtensionRequest>,
    ) -> Result<Response<GrantExtensionResponse>, Status> {
        let rec = self.installed(&req.msg.name).await?;
        if rec.kind == ExtensionKind::Native as i64 && !req.msg.accept_native_risk {
            return Err(Status::new(
                Code::FailedPrecondition,
                "a native extension runs outside the sandbox; accept_native_risk must be set",
            ));
        }
        let s = extension::as_script(&rec);
        let grant = self
            .store
            .grant(
                parse_id(&s.space_id, "space")?,
                extension::grant_identity(&rec.name),
                &rec.version,
                s.benign,
            )
            .await?;
        Ok(Response::new(GrantExtensionResponse { grant: Some(grant) }))
    }

    async fn call_extension(
        &self,
        req: Request<CallExtensionRequest>,
    ) -> Result<Response<CallExtensionResponse>, Status> {
        let m = &req.msg;
        let rec = self.installed(&m.name).await?;
        let space = parse_id(&m.space_id, "space_id")?;
        self.store.space(space).await?;
        let args: serde_json::Value = if m.args_json.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&m.args_json).map_err(|e| invalid(format!("args_json: {e}")))?
        };
        let s = extension::as_script(&rec);
        let grant = if rec.kind == ExtensionKind::Native as i64 {
            // Never automatic.
            self.store
                .grant_for(extension::grant_identity(&rec.name), &rec.version)
                .await?
        } else {
            grant_or_auto(&self.store, ObjectId::from_uuid(uuid::Uuid::nil()), &s).await?
        };
        let Some(grant) = grant else {
            return Err(Status::new(
                Code::PermissionDenied,
                "this version of the extension has no grant on this device",
            ));
        };
        let result = extension::call(
            Arc::clone(&self.store),
            &self.modules,
            extension::Invocation {
                rec: &rec,
                function: &m.function,
                space,
                args,
                grant_id: grant.id,
                parent: None,
            },
        )
        .await;
        Ok(Response::new(CallExtensionResponse {
            result: Some(result),
        }))
    }
}
